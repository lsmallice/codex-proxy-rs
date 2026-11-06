//! 验证原生兼容在 HTTP/WS 路由前生效，格式可修复且不会静默删工具或历史
use axum::{
    body::{Body, to_bytes},
    http::{Request, StatusCode},
};
use futures::{SinkExt, StreamExt, future::BoxFuture};
use gateway_api::openai::responses::ResponsesIngressCompatibility;
use gateway_core::{
    engine::execution::{
        AuthenticatedClient, ClientAuthenticationError, ExecutionService, StartExecution,
        StartProviderExecution, StartedExecution,
    },
    error::{GatewayError, GatewayErrorKind},
    operation::Operation,
    routing::PublicModelId,
};
use serde_json::{Value, json};
use std::sync::{Arc, Mutex};
use tokio_tungstenite::{
    connect_async,
    tungstenite::{Message, client::IntoClientRequest},
};
use tower::ServiceExt;

struct Capture {
    body: Arc<Mutex<Option<Value>>>,
}
impl ExecutionService for Capture {
    fn authenticate(
        &self,
        plaintext: &str,
    ) -> Result<AuthenticatedClient, ClientAuthenticationError> {
        if plaintext == "sk_compat_fixture" {
            Ok(crate::openai::authenticated_client(plaintext))
        } else {
            Err(ClientAuthenticationError::InvalidKey)
        }
    }
    fn public_models(&self, _: &AuthenticatedClient) -> Vec<PublicModelId> {
        vec![PublicModelId::new("gpt-test").unwrap()]
    }
    fn contains_public_model(&self, _: &AuthenticatedClient, _: &PublicModelId) -> bool {
        true
    }
    fn start(
        &self,
        request: StartExecution,
    ) -> BoxFuture<'_, Result<StartedExecution, GatewayError>> {
        Box::pin(async move {
            let Operation::Generate(generate) = request.operation else {
                panic!("Responses generate")
            };
            *self.body.lock().unwrap() = Some(json!(generate.protocol_payload().body()));
            Err(GatewayError::new(
                GatewayErrorKind::Internal,
                "Fixture captured normalized request.",
            ))
        })
    }
    fn start_provider_endpoint(
        &self,
        _: StartProviderExecution,
    ) -> BoxFuture<'_, Result<StartedExecution, GatewayError>> {
        Box::pin(async { panic!("unexpected provider endpoint") })
    }
}
async fn router(
    policy: ResponsesIngressCompatibility,
) -> (axum::Router, Arc<Mutex<Option<Value>>>) {
    router_with_source(policy, None).await
}

async fn router_with_source(
    policy: ResponsesIngressCompatibility,
    source: Option<gateway_api::openai::responses::ResponsesPolicySource>,
) -> (axum::Router, Arc<Mutex<Option<Value>>>) {
    let body = Arc::new(Mutex::new(None));
    let execution = Arc::new(Capture { body: body.clone() });
    let admin = crate::admin::AdminTestFixture::new().await;
    let app = crate::openai::api_router_with_config_and_execution(
        admin.services,
        gateway_api::ApiConfig {
            responses_policy_source: source,
            responses_compatibility: policy,
            asset_directory: std::env::temp_dir(),
            cors_allowed_origins: Vec::new(),
            request_timeout_seconds: None,
            request_id_header: "x-request-id".into(),
        },
        execution,
    );
    (app, body)
}
fn policy() -> ResponsesIngressCompatibility {
    ResponsesIngressCompatibility {
        min_output_tokens_16: true,
        tool_structure: true,
    }
}

#[tokio::test]
async fn hot_settings_same_websocket_connection_reads_new_policy_each_turn() {
    use std::sync::atomic::{AtomicBool, Ordering};
    let enabled = Arc::new(AtomicBool::new(false));
    let source_flag = enabled.clone();
    let source = gateway_api::openai::responses::ResponsesPolicySource::new(move |operation| {
        Ok((
            operation,
            ResponsesIngressCompatibility {
                min_output_tokens_16: source_flag.load(Ordering::SeqCst),
                tool_structure: false,
            },
        ))
    });
    let (app, captured) = router_with_source(Default::default(), Some(source)).await;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    let mut request = format!("ws://{address}/v1/responses")
        .into_client_request()
        .unwrap();
    request
        .headers_mut()
        .insert("authorization", "Bearer sk_compat_fixture".parse().unwrap());
    let (mut ws, _) = connect_async(request).await.unwrap();
    for (active, expected) in [(false, 1), (true, 16), (false, 1)] {
        enabled.store(active, Ordering::SeqCst);
        ws.send(Message::Text(json!({"type":"response.create","model":"gpt-test","input":"hello","max_output_tokens":1}).to_string().into())).await.unwrap();
        tokio::time::timeout(std::time::Duration::from_secs(3), ws.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert_eq!(
            captured.lock().unwrap().as_ref().unwrap()["max_output_tokens"],
            expected
        );
    }
    ws.close(None).await.ok();
    server.abort();
    let _ = server.await;
}
fn original() -> Value {
    json!({"model":"gpt-test","stream":false,"input":[{"type":"function_call","name":"lookup","call_id":"keep","arguments":"{}"},{"type":"function_call_output","call_id":"keep","output":"keep result"}],"max_output_tokens":1,"reasoning":{"effort":"none"},"tools":[{"type":"function","function":{"name":"lookup","description":"fixture","parameters":{"type":"object"},"strict":true}},{"type":"web_search"}],"tool_choice":{"type":"function","function":{"name":"lookup"}},"previous_response_id":"resp_keep"})
}
async fn http(app: axum::Router, body: Value, encoding: Option<&str>) -> (StatusCode, Value) {
    let bytes = serde_json::to_vec(&body).unwrap();
    let bytes = if encoding == Some("zstd") {
        zstd::stream::encode_all(bytes.as_slice(), 3).unwrap()
    } else {
        bytes
    };
    let mut request = Request::post("/v1/responses")
        .header("authorization", "Bearer sk_compat_fixture")
        .header("content-type", "application/json");
    if let Some(encoding) = encoding {
        request = request.header("content-encoding", encoding);
    }
    let response = app
        .oneshot(request.body(Body::from(bytes)).unwrap())
        .await
        .unwrap();
    let status = response.status();
    let body = to_bytes(response.into_body(), 1024 * 1024).await.unwrap();
    (status, serde_json::from_slice(&body).unwrap())
}
fn assert_normalized(actual: &Value, original: &Value) {
    assert_eq!(actual["max_output_tokens"], 16);
    assert_eq!(actual["tools"][0]["name"], "lookup");
    assert_eq!(
        actual["tools"][0]["parameters"],
        original["tools"][0]["function"]["parameters"]
    );
    assert!(actual["tools"][0].get("function").is_none());
    assert_eq!(actual["tools"].as_array().unwrap().len(), 2);
    assert_eq!(
        actual["tool_choice"],
        json!({"type":"function","name":"lookup"})
    );
    assert_eq!(actual["input"], original["input"]);
    assert_eq!(
        actual["previous_response_id"],
        original["previous_response_id"]
    );
    assert_eq!(actual["reasoning"], original["reasoning"]);
}
#[tokio::test]
async fn native_compatibility_http_preserves_history_and_lifts_legacy_tools_before_execution() {
    for encoding in [None, Some("zstd")] {
        let (app, body) = router(policy()).await;
        let original = original();
        let (status, _) = http(app, original.clone(), encoding).await;
        assert_eq!(status, StatusCode::INTERNAL_SERVER_ERROR);
        assert_normalized(body.lock().unwrap().as_ref().unwrap(), &original);
    }
}
#[tokio::test]
async fn native_compatibility_websocket_uses_same_preselection_rules() {
    let (app, body) = router(policy()).await;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });
    let mut request = format!("ws://{address}/v1/responses")
        .into_client_request()
        .unwrap();
    request
        .headers_mut()
        .insert("authorization", "Bearer sk_compat_fixture".parse().unwrap());
    let (mut ws, _) = connect_async(request).await.unwrap();
    let mut original = original();
    original["type"] = json!("response.create");
    original["stream"] = json!(true);
    // 未建立归属的续接在 WS 层应被拒绝，本场景验证新链入站修正
    original
        .as_object_mut()
        .unwrap()
        .remove("previous_response_id");
    ws.send(Message::Text(original.to_string().into()))
        .await
        .unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(3), ws.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_normalized(body.lock().unwrap().as_ref().unwrap(), &original);
    ws.close(None).await.ok();
    server.abort();
    let _ = server.await;
}
#[tokio::test]
async fn native_compatibility_invalid_names_and_conflicting_definitions_fail_without_execution() {
    for bad in [
        json!({"type":"function","name":"bad.tool"}),
        json!({"type":"custom","name":"bad tool"}),
        json!({"type":"function","name":"one","function":{"name":"two"}}),
        json!({"type":"function","function":{"name":"lookup","unexpected":true}}),
    ] {
        let (app, body) = router(policy()).await;
        let mut value = original();
        value["tools"] = json!([bad]);
        let (status, error) = http(app, value, None).await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{error}");
        assert!(
            matches!(
                error["error"]["code"].as_str(),
                Some("invalid_tool_name" | "invalid_tool_structure")
            ),
            "{error}"
        );
        assert!(body.lock().unwrap().is_none());
    }
}
#[tokio::test]
async fn native_compatibility_disabled_and_non_integer_budgets_remain_unchanged() {
    let (app, body) = router(Default::default()).await;
    let original = original();
    http(app, original.clone(), None).await;
    assert_eq!(*body.lock().unwrap(), Some(original.clone()));
    for value in [
        json!(16),
        json!(100),
        json!("1"),
        json!(1.0),
        Value::Null,
        json!(true),
    ] {
        let (app, body) = router(ResponsesIngressCompatibility {
            min_output_tokens_16: true,
            tool_structure: false,
        })
        .await;
        let mut original = original.clone();
        original["max_output_tokens"] = value;
        http(app, original.clone(), None).await;
        assert_eq!(*body.lock().unwrap(), Some(original));
    }
}
