//! 网关状态 delta 的协议范围、一次发送、计量隔离及取消边界
use super::*;
use provider_openai::config::CodexHappyFirstToken;

fn policy() -> CodexHappyFirstToken {
    CodexHappyFirstToken {
        enabled: true,
        client_key_ids: vec!["key_openai_contract".into()],
        min_delay_ms: 50,
        max_delay_ms: 50,
        ..Default::default()
    }
}

fn request_context(transport: ClientTransport, endpoint: &str) -> RequestAttemptContext {
    RequestAttemptContext::new(
        ModelRequestId::new("req_happy_first_token").unwrap(),
        ClientApiKeyId::new("key_openai_contract").unwrap(),
    )
    .with_middleware(None, Arc::from([]), endpoint.into(), transport)
}

fn operation(websocket: bool) -> Operation {
    Operation::Generate(GenerateRequest::from_protocol_payload(
        ProtocolPayload::json_object(
            "openai",
            json!({"model":"gpt-5.4","stream":true,"input":"hello"})
                .as_object()
                .unwrap()
                .clone(),
        )
        .unwrap()
        .with_context(Map::from_iter([("use_websocket".into(), json!(websocket))])),
    ))
}

fn prelude() -> Value {
    json!({"type":"response.created","response":{"id":"resp_happy","model":"gpt-5.4","status":"in_progress","output":[]}})
}
fn sse(value: &Value) -> String {
    gateway_protocol::openai::sse::encode_sse_event(
        value["type"].as_str().unwrap(),
        &value.to_string(),
    )
}

async fn configured(
    store: &Arc<MemoryAccountStore>,
    base: String,
    policy: CodexHappyFirstToken,
) -> Arc<CodexProvider> {
    Arc::new(
        Arc::try_unwrap(provider_with_base_url(store, base))
            .ok()
            .unwrap()
            .with_happy_first_token(policy),
    )
}

#[tokio::test]
async fn happy_delta_follows_real_prelude_once_without_model_facts_for_http_and_ws_upstreams() {
    for websocket in [false, true] {
        let store = Arc::new(MemoryAccountStore::default());
        create_account(&store, "acct_provider_contract").await;
        let complete = response_recovery::completed("resp_happy");
        let (base, release, server) = if websocket {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let base = format!("http://{}", listener.local_addr().unwrap());
            let (tx, rx) = oneshot::channel();
            let server = tokio::spawn(async move {
                let (socket, _) = listener.accept().await.unwrap();
                let mut ws = accept_codex_test_websocket(socket).await;
                ws.next().await.unwrap().unwrap();
                ws.send(Message::Text(prelude().to_string().into()))
                    .await
                    .unwrap();
                rx.await.unwrap();
                ws.send(Message::Text(complete.to_string().into()))
                    .await
                    .unwrap();
            });
            (base, tx, server)
        } else {
            let (base, tx, _, server) =
                paused_chunked_sse_server(sse(&prelude()), sse(&complete)).await;
            (base, tx, server)
        };
        let ctx = request_context(ClientTransport::HttpSse, "/v1/responses");
        let mut stream = configured(&store, base, policy())
            .await
            .execute(
                planned_request("openai", operation(websocket)),
                context_for_request(ctx, CancellationToken::new()),
            )
            .await
            .unwrap();
        let first = timeout(
            Duration::from_secs(2),
            precommit::next_client_event(&mut stream),
        )
        .await
        .unwrap();
        assert_eq!(
            first.wire_event().unwrap().event_type(),
            Some("response.created")
        );
        let hint = timeout(
            Duration::from_secs(2),
            precommit::next_client_event(&mut stream),
        )
        .await
        .unwrap();
        assert!(hint.is_local_control());
        assert!(hint.is_replayable_prelude());
        assert!(!hint.has_canonical_facts());
        assert!(hint.response_observation().is_none());
        assert!(hint.session_update().is_none());
        assert_eq!(
            hint.wire_event().unwrap().data(),
            &json!({"type":"gateway.status.delta","delta":"upstream_connected","metadata":{"gateway_status":"upstream_connected"}})
        );
        release.send(()).unwrap();
        let mut completed = false;
        let mut real_text_time = None;
        while let Some(event) = stream.next().await {
            let event = event.unwrap();
            assert!(!event.is_local_control());
            if let Some(o) = event.response_observation() {
                real_text_time = o.timings().first_text_ms.or(real_text_time);
            }
            completed |= event
                .canonical_facts()
                .iter()
                .any(|f| matches!(f, GatewayEvent::Completed(_)));
        }
        assert!(completed);
        assert!(real_text_time.is_some_and(|t| t >= 50));
        server.await.unwrap();
    }
}

#[tokio::test]
async fn happy_delta_does_not_appear_for_disabled_empty_scope_other_protocols_or_early_output() {
    for case in [
        "disabled",
        "empty",
        "other_key",
        "ws_client",
        "json_client",
        "chat",
        "early_text",
        "unknown_event",
        "terminal",
        "comments_only",
    ] {
        let store = Arc::new(MemoryAccountStore::default());
        create_account(&store, "acct_provider_contract").await;
        let mut p = policy();
        let mut initial = sse(&prelude());
        let transport = match case {
            "ws_client" => ClientTransport::WebSocket,
            "json_client" => ClientTransport::HttpJson,
            _ => ClientTransport::HttpSse,
        };
        match case {
            "disabled" => p.enabled = false,
            "empty" => p.client_key_ids.clear(),
            "other_key" => p.client_key_ids = vec!["other".into()],
            "early_text" => {
                initial += &sse(
                    &json!({"type":"response.output_text.delta","delta":"hello","output_index":0,"content_index":0}),
                )
            }
            "unknown_event" => {
                initial += &sse(&json!({"type":"gateway.status.delta","delta":"from_upstream"}))
            }
            "terminal" => initial += &sse(&response_recovery::completed("resp_happy")),
            "comments_only" => initial = ": upstream-heartbeat\n\n".into(),
            _ => {}
        }
        let terminal = if case == "terminal" {
            String::new()
        } else {
            sse(&response_recovery::completed("resp_happy"))
        };
        let (base, release, _, server) = paused_chunked_sse_server(initial, terminal).await;
        let sender = tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(130)).await;
            let _ = release.send(());
        });
        let ctx = request_context(
            transport,
            if case == "chat" {
                "/v1/chat/completions"
            } else {
                "/v1/responses"
            },
        );
        let mut stream = configured(&store, base, p)
            .await
            .execute(
                planned_request("openai", operation(false)),
                context_for_request(ctx, CancellationToken::new()),
            )
            .await
            .unwrap();
        while let Some(event) = stream.next().await {
            assert!(!event.unwrap().is_local_control(), "{case}");
        }
        sender.await.unwrap();
        server.await.unwrap();
    }
}

#[tokio::test]
async fn happy_delta_request_state_survives_new_attempt_and_cancellation_stops_emission() {
    let request = request_context(ClientTransport::HttpSse, "/v1/responses");
    for attempt in 0..2 {
        let store = Arc::new(MemoryAccountStore::default());
        create_account(&store, "acct_provider_contract").await;
        let (base, release, _, server) = paused_chunked_sse_server(
            sse(&prelude()),
            sse(&response_recovery::completed("resp_happy")),
        )
        .await;
        let ctx = context_for_request(request.clone(), CancellationToken::new());
        let mut stream = configured(&store, base, policy())
            .await
            .execute(planned_request("openai", operation(false)), ctx)
            .await
            .unwrap();
        precommit::next_client_event(&mut stream).await;
        if attempt == 0 {
            assert!(
                precommit::next_client_event(&mut stream)
                    .await
                    .is_local_control()
            );
        }
        let send = tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(100)).await;
            let _ = release.send(());
        });
        while let Some(event) = stream.next().await {
            assert!(!event.unwrap().is_local_control());
        }
        send.await.unwrap();
        server.await.unwrap();
    }
    let store = Arc::new(MemoryAccountStore::default());
    create_account(&store, "acct_provider_contract").await;
    let (base, release, _, server) =
        paused_chunked_sse_server(sse(&prelude()), String::new()).await;
    let cancel = CancellationToken::new();
    let mut p = policy();
    p.min_delay_ms = 1000;
    p.max_delay_ms = 1000;
    let mut stream = configured(&store, base, p)
        .await
        .execute(
            planned_request("openai", operation(false)),
            context_for_request(
                request_context(ClientTransport::HttpSse, "/v1/responses"),
                cancel.clone(),
            ),
        )
        .await
        .unwrap();
    precommit::next_client_event(&mut stream).await;
    cancel.cancel();
    let (events, error) = precommit::stream_failure(&mut stream).await;
    assert_eq!(error.kind(), ProviderErrorKind::Cancelled);
    assert!(events.iter().all(|e| !e.is_local_control()));
    let _ = release.send(());
    server.await.unwrap();
}
