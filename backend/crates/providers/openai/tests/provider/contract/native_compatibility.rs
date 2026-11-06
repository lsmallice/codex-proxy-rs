//! 验证原生参数补正覆盖 HTTP/WS 与 OAuth/API Key，保留合法值和推理档位
use super::*;
use provider_openai::config::CodexRequestCompatibility;

fn policy() -> CodexRequestCompatibility {
    CodexRequestCompatibility {
        lite_context: true,
        store_false: true,
        json_object: true,
        ..Default::default()
    }
}

#[tokio::test]
async fn native_compatibility_applies_minimum_after_official_oauth_token_alias_normalization() {
    for alias in ["max_tokens", "max_completion_tokens"] {
        for enabled in [false, true] {
            let mut body = json!({"model":"gpt-5.4","input":"hello","store":false});
            body[alias] = json!(1);
            let policy = CodexRequestCompatibility {
                min_output_tokens_16: enabled,
                ..Default::default()
            };
            let sent =
                capture_downstream_request_with_policy(body, false, false, false, policy).await;
            assert!(sent.get(alias).is_none());
            assert_eq!(sent["max_output_tokens"], if enabled { 16 } else { 1 });
        }
    }
}

#[tokio::test]
async fn native_compatibility_oauth_store_correction_keeps_native_continuation_scope_consistent() {
    let store = Arc::new(MemoryAccountStore::default());
    create_account(&store, "acct_provider_contract").await;
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/codex/responses"))
        .respond_with(
            ResponseTemplate::new(200).set_body_raw(CAPTURE_COMPLETED_SSE, "text/event-stream"),
        )
        .expect(1)
        .mount(&server)
        .await;
    let provider = Arc::new(
        Arc::try_unwrap(provider_with_base_url(&store, server.uri()))
            .ok()
            .unwrap()
            .with_request_compatibility(policy()),
    );
    let seed = generate_with_persisted_session_context(
        "acct_provider_contract",
        "compat-conversation",
        "compat-session",
        "compat-thread",
    );
    let mut context = seed.protocol_payload().context().clone();
    context.insert("use_websocket".into(), json!(false));
    let body = json!({"model":"gpt-5.4","input":"hello","store":"true"});
    let operation = Operation::Generate(seed)
        .replace_protocol_wire(Some(body.to_string().into()), context)
        .unwrap();
    let mut stream = provider
        .execute(
            planned_request("openai", operation),
            super::context("req_store_scope", CancellationToken::new()),
        )
        .await
        .unwrap();
    let mut scope = None;
    while let Some(event) = stream.next().await {
        if let Some(state) = event.unwrap().session_update() {
            scope = state.payload().get("continuation_scope").cloned();
        }
    }
    assert_eq!(scope, Some(json!("replay_required")));
    assert_eq!(
        captured_request_body(&server.received_requests().await.unwrap()[0])["store"],
        false
    );
}
#[tokio::test]
async fn native_compatibility_ws_marker_overrides_header_but_http_uses_actual_header() {
    for websocket in [false, true] {
        for marker in [json!("false"), json!(false), Value::Null, json!("true")] {
            let body = json!({"model":"gpt-5.4","input":"hello","store":false,"client_metadata":{"ws_request_header_x_openai_internal_codex_responses_lite":marker},"reasoning":{"effort":"none"}});
            let sent =
                capture_downstream_request_with_policy(body, true, websocket, true, policy()).await;
            assert_eq!(
                sent["reasoning"].get("context").and_then(Value::as_str),
                if !websocket || marker == "true" {
                    Some("all_turns")
                } else {
                    None
                }
            );
            assert_eq!(sent["reasoning"]["effort"], "none");
        }
    }
}
#[tokio::test]
async fn native_compatibility_applies_before_send_without_changing_reasoning_effort() {
    for api_key in [false, true] {
        for websocket in [false, true] {
            let body = json!({"model":"gpt-5.4","input":[{"role":"user","content":"hello"}],"store":"true","reasoning":{"effort":"none","summary":"auto"},"text":{"format":{"type":"json_object"}},"tools":[{"type":"function","name":"fixture","parameters":{"type":"object"}}]});
            let sent = capture_downstream_request_with_policy(
                body.clone(),
                api_key,
                websocket,
                true,
                policy(),
            )
            .await;
            assert_eq!(sent["reasoning"]["context"], "all_turns");
            assert_eq!(sent["reasoning"]["effort"], "none");
            assert_eq!(sent["reasoning"]["summary"], "auto");
            assert_eq!(
                sent["store"],
                if api_key { json!("true") } else { json!(false) }
            );
            assert_eq!(sent["input"].as_array().unwrap().len(), 2);
            assert_eq!(sent["input"][0]["role"], "developer");
            assert_eq!(sent["input"][1], body["input"][0]);
            assert_eq!(sent["tools"], body["tools"]);
        }
    }
}
#[tokio::test]
async fn native_compatibility_does_not_invent_json_instructions_for_unrelated_payloads() {
    for (input, format, added) in [
        (json!("Return JSON"), "json_object", false),
        (
            json!([{"role":"user","content":[{"type":"input_text","text":"JsOn please"}]}]),
            "json_object",
            false,
        ),
        (
            json!([{"type":"function_call_output","call_id":"keep","output":"JSON is tool text"}]),
            "json_object",
            true,
        ),
        (json!("hello"), "json_schema", false),
        (json!([]), "json_object", false),
    ] {
        let body = json!({"model":"gpt-5.4","input":input,"store":false,"text":{"format":{"type":format}},"reasoning":{"context":"last_turn","effort":"high"}});
        let baseline = capture_downstream_request(body.clone(), true, false, false).await;
        let sent =
            capture_downstream_request_with_policy(body.clone(), true, false, false, policy())
                .await;
        assert_eq!(sent["reasoning"], body["reasoning"]);
        if added {
            assert_eq!(sent["input"][1], body["input"][0]);
        } else {
            assert_eq!(sent["input"], baseline["input"]);
        }
    }
}
#[tokio::test]
async fn native_compatibility_flags_are_independent_and_invalid_reasoning_is_preserved() {
    for option in ["disabled", "lite", "store", "json"] {
        let p = CodexRequestCompatibility {
            lite_context: option == "lite",
            store_false: option == "store",
            json_object: option == "json",
            ..Default::default()
        };
        let body = json!({"model":"gpt-5.4","input":"hello","store":true,"reasoning":"unexpected","text":{"format":{"type":"json_object"}}});
        let sent =
            capture_downstream_request_with_policy(body.clone(), false, false, true, p).await;
        assert_eq!(sent["reasoning"], "unexpected");
        assert_eq!(sent["store"], json!(option != "store"));
        assert_eq!(
            sent["input"].as_array().unwrap().len(),
            if option == "json" { 2 } else { 1 }
        );
    }
}
