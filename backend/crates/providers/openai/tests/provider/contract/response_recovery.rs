//! 验证未交付前导的有界 WS 补发、预算退出及可信完整历史续接恢复
use super::*;
use gateway_core::event::ProviderEvent;
use provider_openai::config::CodexWsResponseRecovery;

pub(super) fn configured(
    store: &Arc<MemoryAccountStore>,
    base: String,
    policy: CodexWsResponseRecovery,
) -> Arc<CodexProvider> {
    Arc::new(
        Arc::try_unwrap(provider_with_base_url(store, base))
            .ok()
            .unwrap()
            .with_ws_response_recovery(policy),
    )
}
fn policy() -> CodexWsResponseRecovery {
    CodexWsResponseRecovery {
        enabled: true,
        full_history: true,
        window_ms: 500,
        max_prefix_bytes: 65536,
        ..Default::default()
    }
}
fn request(input: Value, previous: Option<&str>, state: Option<ProviderSessionState>) -> Operation {
    let mut body = json!({"model":"gpt-5.4","stream":true,"store":false,"input":input,"x-codex-turn-state":"fixture-turn"});
    if let Some(previous) = previous {
        body["previous_response_id"] = json!(previous);
    }
    let mut generate = GenerateRequest::from_protocol_payload(
        ProtocolPayload::json_object("openai", body.as_object().unwrap().clone())
            .unwrap()
            .with_context(Map::from_iter([(
                "downstream_websocket_connection_id".into(),
                json!("response-recovery-fixture"),
            )])),
    );
    let state=state.unwrap_or_else(||ProviderSessionState::new("openai",json!({"account_id":"acct_provider_contract","conversation_id":"response-recovery-fixture","continuation_scope":"persisted"}).as_object().unwrap().clone()).unwrap());
    generate = generate.with_provider_session_state(state);
    Operation::Generate(generate)
}
pub(super) fn completed(id: &str) -> Value {
    json!({"type":"response.completed","response":{"id":id,"status":"completed","model":"gpt-5.4","output":[{"type":"message","role":"assistant","content":[{"type":"output_text","text":"OK"}]}],"usage":{"input_tokens":1,"output_tokens":1,"total_tokens":2}}})
}
pub(super) async fn collect(
    provider: Arc<CodexProvider>,
    operation: Operation,
    context: AttemptContext,
) -> (
    Vec<ProviderEvent>,
    Option<gateway_core::error::ProviderError>,
) {
    let stream = provider
        .execute(planned_request("openai", operation), context)
        .await;
    let mut stream = match stream {
        Ok(s) => s,
        Err(e) => return (vec![], Some(e)),
    };
    let mut events = vec![];
    while let Some(event) = timeout(Duration::from_secs(6), stream.next())
        .await
        .unwrap()
    {
        match event {
            Ok(e) => events.push(e),
            Err(e) => return (events, Some(e)),
        }
    }
    (events, None)
}

#[tokio::test]
async fn empty_prelude_then_close_replays_once_without_exposing_first_response() {
    for code in [1000u16, 1011, 1012, 0] {
        let store = Arc::new(MemoryAccountStore::default());
        create_account(&store, "acct_provider_contract").await;
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base = format!("http://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(async move {
            let mut payload = None;
            for round in 0..2 {
                let (socket, _) = listener.accept().await.unwrap();
                let mut ws = accept_codex_test_websocket(socket).await;
                let frame = ws.next().await.unwrap().unwrap();
                let mut body: Value = serde_json::from_str(frame.to_text().unwrap()).unwrap();
                body["client_metadata"]
                    .as_object_mut()
                    .unwrap()
                    .remove("x-codex-ws-stream-request-start-ms");
                if let Some(first) = payload.as_ref() {
                    assert_eq!(&body, first);
                } else {
                    payload = Some(body);
                }
                if round == 0 {
                    ws.send(Message::Text(json!({"type":"response.created","response":{"id":"resp_discard","output":[]}}).to_string().into())).await.unwrap();
                    ws.send(Message::Text(json!({"type":"response.in_progress","response":{"id":"resp_discard","output":[]}}).to_string().into())).await.unwrap();
                    if code != 0 {
                        ws.close(Some(CloseFrame {
                            code: code.into(),
                            reason: "".into(),
                        }))
                        .await
                        .unwrap();
                    }
                } else {
                    ws.send(Message::Text(completed("resp_replayed").to_string().into()))
                        .await
                        .unwrap();
                }
            }
        });
        let (events, error) = collect(
            configured(&store, base, policy()),
            request(json!([{"role":"user","content":"hello"}]), None, None),
            context("req_response_recovery", CancellationToken::new()),
        )
        .await;
        assert!(error.is_none(), "{error:?}");
        assert!(events.iter().any(|e| {
            e.wire_event()
                .is_some_and(|w| w.event_type() == Some("response.completed"))
        }));
        assert!(!events.iter().any(|e| {
            e.wire_event()
                .is_some_and(|w| w.data().to_string().contains("resp_discard"))
        }));
        timeout(Duration::from_secs(6), server)
            .await
            .unwrap()
            .unwrap();
    }
}

#[tokio::test]
async fn semantic_output_unknown_events_and_prefix_budget_prevent_replay() {
    for scenario in [
        "text",
        "tool",
        "reasoning",
        "unknown",
        "bytes",
        "time",
        "disabled",
        "tools",
    ] {
        let store = Arc::new(MemoryAccountStore::default());
        create_account(&store, "acct_provider_contract").await;
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base = format!("http://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(async move {
            let (socket, _) = listener.accept().await.unwrap();
            let mut ws = accept_codex_test_websocket(socket).await;
            ws.next().await.unwrap().unwrap();
            let event = match scenario {
                "text" => json!({"type":"response.output_text.delta","delta":"hello"}),
                "tool" => json!({"type":"response.function_call_arguments.delta","delta":"{"}),
                "reasoning" => json!({"type":"response.reasoning_text.delta","delta":"thinking"}),
                "unknown" => json!({"type":"future.event"}),
                "bytes" => {
                    json!({"type":"response.created","response":{"output":[],"padding":"x".repeat(2048)}})
                }
                _ => json!({"type":"response.created","response":{"output":[]}}),
            };
            ws.send(Message::Text(event.to_string().into()))
                .await
                .unwrap();
            if scenario == "time" {
                tokio::time::sleep(Duration::from_millis(60)).await;
            }
            ws.close(Some(CloseFrame {
                code: CloseCode::Normal,
                reason: "".into(),
            }))
            .await
            .unwrap();
            assert!(
                timeout(Duration::from_millis(150), listener.accept())
                    .await
                    .is_err(),
                "{scenario}"
            );
        });
        let p = CodexWsResponseRecovery {
            enabled: scenario != "disabled",
            max_retries: 3,
            window_ms: if scenario == "time" { 20 } else { 500 },
            max_prefix_bytes: 1024,
            ..policy()
        };
        let mut op = request(json!([{"role":"user","content":"hello"}]), None, None);
        if scenario == "tools" {
            let body = json!({"model":"gpt-5.4","input":"hello","tools":[{"type":"web_search"}]});
            op = Operation::Generate(GenerateRequest::from_protocol_payload(
                ProtocolPayload::json_object("openai", body.as_object().unwrap().clone()).unwrap(),
            ));
        }
        let (events, error) = collect(
            configured(&store, base, p),
            op,
            context("req_no_replay", CancellationToken::new()),
        )
        .await;
        assert!(error.is_some(), "{scenario}");
        assert!(
            !events.is_empty(),
            "buffered events must survive {scenario}"
        );
        timeout(Duration::from_secs(6), server)
            .await
            .unwrap()
            .unwrap();
    }
}

#[tokio::test]
async fn configured_response_retries_stop_at_the_budget_without_core_recovery() {
    for retries in [0, 1, 3, 5] {
        let store = Arc::new(MemoryAccountStore::default());
        create_account(&store, "acct_provider_contract").await;
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base = format!("http://{}", listener.local_addr().unwrap());
        let connections = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let observed = Arc::clone(&connections);
        let server = tokio::spawn(async move {
            for _ in 0..=retries {
                let (socket, _) = listener.accept().await.unwrap();
                let mut ws = accept_codex_test_websocket(socket).await;
                ws.next().await.unwrap().unwrap();
                observed.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                ws.send(Message::Text(
                    json!({"type":"response.in_progress","response":{"output":[]}})
                        .to_string()
                        .into(),
                ))
                .await
                .unwrap();
                ws.close(Some(CloseFrame {
                    code: CloseCode::Restart,
                    reason: "".into(),
                }))
                .await
                .unwrap();
            }
            assert!(
                timeout(Duration::from_millis(150), listener.accept())
                    .await
                    .is_err()
            );
        });
        let (_, error) = collect(
            configured(
                &store,
                base,
                CodexWsResponseRecovery {
                    max_retries: retries,
                    window_ms: 5000,
                    ..policy()
                },
            ),
            request(json!("hello"), None, None),
            context("req_once_only", CancellationToken::new()),
        )
        .await;
        let error = error.unwrap();
        assert_eq!(
            connections.load(std::sync::atomic::Ordering::SeqCst),
            retries as usize + 1,
            "retries={retries}, error={error:?}"
        );
        if retries > 0 {
            assert!(error.retry_is_prohibited());
        }
        timeout(Duration::from_secs(6), server)
            .await
            .unwrap()
            .unwrap();
    }
}

#[tokio::test]
async fn later_response_retry_can_succeed_without_exposing_discarded_preludes() {
    let store = Arc::new(MemoryAccountStore::default());
    create_account(&store, "acct_provider_contract").await;
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(async move {
        for round in 0..4 {
            let (socket, _) = listener.accept().await.unwrap();
            let mut ws = accept_codex_test_websocket(socket).await;
            ws.next().await.unwrap().unwrap();
            if round < 3 {
                ws.send(Message::Text(json!({"type":"response.in_progress","response":{"id":format!("resp_discard_{round}"),"output":[]}}).to_string().into())).await.unwrap();
                ws.close(Some(CloseFrame {
                    code: CloseCode::Restart,
                    reason: "".into(),
                }))
                .await
                .unwrap();
            } else {
                ws.send(Message::Text(completed("resp_final").to_string().into()))
                    .await
                    .unwrap();
            }
        }
    });
    let (events, error) = collect(
        configured(
            &store,
            base,
            CodexWsResponseRecovery {
                max_retries: 3,
                window_ms: 5000,
                ..policy()
            },
        ),
        request(json!("hello"), None, None),
        context("req_third_retry", CancellationToken::new()),
    )
    .await;
    assert!(error.is_none(), "{error:?}");
    assert!(events.iter().any(|e| {
        e.wire_event()
            .is_some_and(|w| w.event_type() == Some("response.completed"))
    }));
    assert!(!events.iter().any(|e| {
        e.wire_event()
            .is_some_and(|w| w.data().to_string().contains("resp_discard_"))
    }));
    timeout(Duration::from_secs(6), server)
        .await
        .unwrap()
        .unwrap();
}

#[tokio::test]
async fn repeated_disconnects_share_one_window_and_inspection_budget() {
    for budget in ["time", "bytes"] {
        let store = Arc::new(MemoryAccountStore::default());
        create_account(&store, "acct_provider_contract").await;
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base = format!("http://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(async move {
            for round in 0..2 {
                let (socket, _) = listener.accept().await.unwrap();
                let mut ws = accept_codex_test_websocket(socket).await;
                ws.next().await.unwrap().unwrap();
                ws.send(Message::Text(json!({"type":"response.in_progress","response":{"output":[],"padding":if budget=="bytes"{"x".repeat(600)}else{String::new()}}}).to_string().into())).await.unwrap();
                if budget == "time" {
                    tokio::time::sleep(Duration::from_millis(if round == 0 { 300 } else { 800 }))
                        .await;
                }
                ws.close(Some(CloseFrame {
                    code: CloseCode::Restart,
                    reason: "".into(),
                }))
                .await
                .unwrap();
            }
            assert!(
                timeout(Duration::from_millis(150), listener.accept())
                    .await
                    .is_err(),
                "{budget}"
            );
        });
        let p = CodexWsResponseRecovery {
            max_retries: 5,
            window_ms: if budget == "time" { 1000 } else { 5000 },
            max_prefix_bytes: 1024,
            ..policy()
        };
        let (_, error) = collect(
            configured(&store, base, p),
            request(json!("hello"), None, None),
            context("req_shared_budget", CancellationToken::new()),
        )
        .await;
        assert!(error.unwrap().retry_is_prohibited());
        timeout(Duration::from_secs(6), server)
            .await
            .unwrap()
            .unwrap();
    }
}

#[tokio::test]
async fn recovery_handshake_rejection_keeps_prior_send_and_original_status() {
    let store = Arc::new(MemoryAccountStore::default());
    create_account(&store, "acct_provider_contract").await;
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(async move {
        let (socket, _) = listener.accept().await.unwrap();
        let mut ws = accept_codex_test_websocket(socket).await;
        ws.next().await.unwrap().unwrap();
        ws.send(Message::Text(
            json!({"type":"response.in_progress","response":{"output":[]}})
                .to_string()
                .into(),
        ))
        .await
        .unwrap();
        ws.close(Some(CloseFrame {
            code: CloseCode::Restart,
            reason: "".into(),
        }))
        .await
        .unwrap();
        let (mut socket, _) = listener.accept().await.unwrap();
        capture_http_request(&mut socket).await;
        let body = r#"{"error":{"code":"permission_denied","message":"Denied"}}"#;
        socket.write_all(format!("HTTP/1.1 403 Forbidden\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",body.len()).as_bytes()).await.unwrap();
    });
    let (_, error) = collect(
        configured(&store, base, policy()),
        request(json!("hello"), None, None),
        context("req_retry_rejected", CancellationToken::new()),
    )
    .await;
    let error = error.unwrap();
    assert_eq!(error.upstream_status(), Some(403));
    assert_eq!(error.send_state(), UpstreamSendState::Sent);
    assert!(error.retry_is_prohibited());
    assert!(!error.replay_is_safe());
    timeout(Duration::from_secs(6), server)
        .await
        .unwrap()
        .unwrap();
}

#[tokio::test]
async fn cancellation_while_buffering_never_replays_and_preserves_sent_state() {
    let store = Arc::new(MemoryAccountStore::default());
    create_account(&store, "acct_provider_contract").await;
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("http://{}", listener.local_addr().unwrap());
    let token = CancellationToken::new();
    let cancel = token.clone();
    let server = tokio::spawn(async move {
        let (socket, _) = listener.accept().await.unwrap();
        let mut ws = accept_codex_test_websocket(socket).await;
        ws.next().await.unwrap().unwrap();
        ws.send(Message::Text(
            json!({"type":"response.in_progress","response":{"output":[]}})
                .to_string()
                .into(),
        ))
        .await
        .unwrap();
        cancel.cancel();
        assert!(
            timeout(Duration::from_millis(200), listener.accept())
                .await
                .is_err()
        );
    });
    let (_, error) = collect(
        configured(&store, base, policy()),
        request(json!("hello"), None, None),
        context("req_cancel_recovery", token),
    )
    .await;
    let error = error.unwrap();
    assert_eq!(error.kind(), ProviderErrorKind::Cancelled);
    assert_eq!(error.send_state(), UpstreamSendState::Sent);
    timeout(Duration::from_secs(6), server)
        .await
        .unwrap()
        .unwrap();
}

#[tokio::test]
async fn configured_close_codes_and_account_changes_block_replay() {
    for scenario in ["code", "credential", "quota", "model_filter"] {
        let store = Arc::new(MemoryAccountStore::default());
        create_account(&store, "acct_provider_contract").await;
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base = format!("http://{}", listener.local_addr().unwrap());
        let state = store.clone();
        let server = tokio::spawn(async move {
            let (socket, _) = listener.accept().await.unwrap();
            let mut ws = accept_codex_test_websocket(socket).await;
            ws.next().await.unwrap().unwrap();
            ws.send(Message::Text(
                json!({"type":"response.in_progress","response":{"output":[]}})
                    .to_string()
                    .into(),
            ))
            .await
            .unwrap();
            if scenario == "quota" {
                exhaust_account_quota(&state, "acct_provider_contract").await;
            }
            if scenario == "credential" {
                state.set_egress(
                    "acct_provider_contract",
                    Some(
                        gateway_core::account::OutboundProxy::parse("http://127.0.0.1:9").unwrap(),
                    ),
                    None,
                );
            }
            ws.close(Some(CloseFrame {
                code: if scenario == "code" {
                    CloseCode::Policy
                } else {
                    CloseCode::Normal
                },
                reason: "".into(),
            }))
            .await
            .unwrap();
            assert!(
                timeout(Duration::from_millis(150), listener.accept())
                    .await
                    .is_err(),
                "{scenario}"
            );
        });
        let p = CodexWsResponseRecovery {
            close_codes: if scenario == "model_filter" {
                vec![1012]
            } else {
                policy().close_codes
            },
            ..policy()
        };
        let (_, error) = collect(
            configured(&store, base, p),
            request(json!("hello"), None, None),
            context("req_replay_guard", CancellationToken::new()),
        )
        .await;
        assert!(error.is_some());
        timeout(Duration::from_secs(6), server)
            .await
            .unwrap()
            .unwrap();
    }
}

#[tokio::test]
async fn verified_full_history_replays_missing_connection_but_delta_does_not() {
    for scenario in ["full", "delta", "modified", "revision", "no_proof"] {
        let complete = scenario == "full";
        let store = Arc::new(MemoryAccountStore::default());
        create_account(&store, "acct_provider_contract").await;
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base = format!("http://{}", listener.local_addr().unwrap());
        let first_input = json!([{"role":"user","content":"first"}]);
        let output = completed("resp_seed")["response"]["output"]
            .as_array()
            .unwrap()
            .clone();
        let expected_output = output.clone();
        let server = tokio::spawn(async move {
            let (socket, _) = listener.accept().await.unwrap();
            let mut ws = accept_codex_test_websocket(socket).await;
            ws.next().await.unwrap().unwrap();
            ws.send(Message::Text(completed("resp_seed").to_string().into()))
                .await
                .unwrap();
            ws.close(Some(CloseFrame {
                code: CloseCode::Normal,
                reason: "".into(),
            }))
            .await
            .unwrap();
            if complete {
                let (socket, _) = timeout(Duration::from_secs(4), listener.accept())
                    .await
                    .unwrap()
                    .unwrap();
                let mut ws = accept_codex_test_websocket(socket).await;
                let msg = ws.next().await.unwrap().unwrap();
                let body: Value = serde_json::from_str(msg.to_text().unwrap()).unwrap();
                assert!(body.get("previous_response_id").is_none());
                assert_eq!(body["input"][0], json!({"role":"user","content":"first"}));
                assert_eq!(body["input"][1], expected_output[0]);
                assert!(body.get("x-codex-turn-state").is_none());
                assert!(body["client_metadata"].get("x-codex-turn-state").is_none());
                ws.send(Message::Text(completed("resp_second").to_string().into()))
                    .await
                    .unwrap();
            } else {
                assert!(
                    timeout(Duration::from_millis(500), listener.accept())
                        .await
                        .is_err()
                );
            }
        });
        let provider = configured(
            &store,
            base,
            CodexWsResponseRecovery {
                max_retries: 0,
                ..policy()
            },
        );
        let (events, error) = collect(
            provider.clone(),
            request(first_input.clone(), None, None),
            context("req_proof_seed", CancellationToken::new()),
        )
        .await;
        assert!(error.is_none());
        let mut state = events
            .iter()
            .find_map(|e| e.session_update())
            .unwrap()
            .clone();
        assert!(state.payload().contains_key("history"));
        assert_eq!(state.payload()["continuation_scope"], "connection_local");
        if matches!(scenario, "revision" | "no_proof") {
            let mut payload = state.payload().clone();
            if scenario == "revision" {
                payload.get_mut("history").unwrap()["revision"] = json!(999999);
            } else {
                payload.remove("history");
            }
            state = ProviderSessionState::new("openai", payload).unwrap();
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
        let mut input = if scenario != "delta" {
            first_input.as_array().unwrap().clone()
        } else {
            vec![]
        };
        if scenario != "delta" {
            input.extend(output);
        }
        if scenario == "modified" {
            input[0]["content"] = json!("changed history");
        }
        input.push(json!({"role":"user","content":"second"}));
        let (_, error) = collect(
            provider,
            request(json!(input), Some("resp_seed"), Some(state)),
            pinned_continuation_context(
                "req_proof_follow",
                "acct_provider_contract",
                "resp_seed",
                "resp_seed",
                1,
                ContinuationAttempt::Native,
            ),
        )
        .await;
        if complete {
            assert!(error.is_none(), "{error:?}");
        } else {
            let e = error.unwrap();
            assert_eq!(
                e.client_visible_upstream_error().unwrap().code(),
                Some("previous_response_not_found")
            );
            assert_eq!(e.send_state(), UpstreamSendState::NotSent);
        }
        timeout(Duration::from_secs(6), server)
            .await
            .unwrap()
            .unwrap();
    }
}
