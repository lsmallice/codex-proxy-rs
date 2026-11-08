//! 验证 WS 密文定点恢复、迟到拒绝记忆、隔离与缓存边界
use super::response_recovery::{collect, completed, configured};
use super::*;
use provider_openai::config::CodexWsResponseRecovery;
type Socket = tokio_tungstenite::WebSocketStream<TcpStream>;

fn policy() -> CodexWsResponseRecovery {
    CodexWsResponseRecovery {
        encrypted_history: true,
        ..Default::default()
    }
}
fn message(id: &str) -> Value {
    json!({"type":"message","id":id,"status":"completed","role":"assistant","phase":"final_answer","content":[{"type":"output_text","text":"visible answer","annotations":[]}],"encrypted_content":format!("cipher-{id}")})
}
fn rejected(id: Option<&str>) -> Value {
    json!({"type":"error","status":400,"error":{"type":"invalid_request_error","code":"invalid_encrypted_content","message":id.map_or("Cannot decrypt content".into(),|id|format!("The encrypted content for item {id} could not be verified. Reason: encrypted content hydration failed: Encrypted content could not be decrypted or parsed."))}})
}
fn operation(input: Value, session: &str, previous: Option<&str>) -> Operation {
    operation_transport(input, session, previous, true)
}
fn operation_transport(
    input: Value,
    session: &str,
    previous: Option<&str>,
    downstream_ws: bool,
) -> Operation {
    let mut body = json!({"model":"gpt-5.4","stream":true,"store":false,"input":input,"thread_id":session,"x-codex-turn-state":"old-turn-state","tools":[{"type":"function","name":"fixture","parameters":{"type":"object"}}]});
    if let Some(previous) = previous {
        body["previous_response_id"] = json!(previous);
    }
    let mut context = Map::from_iter([("session_id".into(), json!(session))]);
    if downstream_ws {
        context.insert("downstream_websocket_connection_id".into(), json!(session));
    }
    Operation::Generate(GenerateRequest::from_protocol_payload(
        ProtocolPayload::json_object("openai",body.as_object().unwrap().clone()).unwrap().with_context(context)
    ).with_provider_session_state(ProviderSessionState::new("openai",json!({"account_id":"acct_provider_contract","conversation_id":session,"continuation_scope":"persisted"}).as_object().unwrap().clone()).unwrap()))
}
async fn receive(listener: &TcpListener, active: &mut Option<Socket>) -> Value {
    loop {
        if let Some(ws) = active.as_mut() {
            tokio::select! {
                message=ws.next()=>match message {
                    Some(Ok(Message::Text(text)))=>return serde_json::from_str(&text).unwrap(),
                    Some(Ok(Message::Ping(_) | Message::Pong(_)))=>{},
                    _=>*active=None,
                },
                connection=listener.accept()=>{*active=Some(accept_codex_test_websocket(connection.unwrap().0).await);}
            }
        } else {
            *active = Some(accept_codex_test_websocket(listener.accept().await.unwrap().0).await);
        }
    }
}
async fn send(active: &mut Option<Socket>, event: Value) {
    active
        .as_mut()
        .unwrap()
        .send(Message::Text(event.to_string().into()))
        .await
        .unwrap();
}

#[tokio::test]
async fn encrypted_recovery_forwards_metadata_before_waiting_for_rejection() {
    let store = Arc::new(MemoryAccountStore::default());
    create_account(&store, "acct_provider_contract").await;
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("http://{}", listener.local_addr().unwrap());
    let (delivered, received) = tokio::sync::oneshot::channel();
    let server = tokio::spawn(async move {
        let mut active = None;
        receive(&listener, &mut active).await;
        send(
            &mut active,
            json!({"type":"codex.response.metadata","metadata":{}}),
        )
        .await;
        received.await.unwrap();
        send(&mut active, rejected(Some("bad"))).await;
        let clean = receive(&listener, &mut active).await;
        assert!(clean["input"][0].get("encrypted_content").is_none());
        send(&mut active, completed("resp_immediate_repair")).await;
    });
    let provider = configured(&store, base, policy());
    let mut stream = provider
        .execute(
            planned_request(
                "openai",
                operation(json!([message("bad")]), "immediate-repair", None),
            ),
            context("req_first_encrypted_event", CancellationToken::new()),
        )
        .await
        .unwrap();
    timeout(Duration::from_secs(2), async {
        loop {
            let event = stream.next().await.unwrap().unwrap();
            if let Some(wire) = event.wire_event() {
                assert_eq!(wire.event_type(), Some("codex.response.metadata"));
                assert!(event.is_replayable_prelude());
                break;
            }
        }
    })
    .await
    .expect("encrypted recovery must not hold metadata until the rejection");
    delivered.send(()).unwrap();
    let mut completions = 0;
    while let Some(event) = timeout(Duration::from_secs(3), stream.next())
        .await
        .unwrap()
    {
        let event = event.unwrap();
        completions += usize::from(
            event
                .wire_event()
                .is_some_and(|w| w.event_type() == Some("response.completed")),
        );
    }
    assert_eq!(completions, 1);
    timeout(Duration::from_secs(3), server)
        .await
        .unwrap()
        .unwrap();
}

#[tokio::test]
async fn encrypted_message_and_reasoning_recover_once_preserving_tools_and_large_history() {
    for reasoning in [false, true] {
        let store = Arc::new(MemoryAccountStore::default());
        create_account(&store, "acct_provider_contract").await;
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base = format!("http://{}", listener.local_addr().unwrap());
        let target = if reasoning {
            json!({"type":"reasoning","id":"bad","encrypted_content":"bad-cipher","summary":[{"type":"summary_text","text":"visible summary"}]})
        } else {
            message("bad")
        };
        let call = if reasoning {
            json!({"type":"custom_tool_call","call_id":"call_fixture","name":"fixture","input":"fixture input"})
        } else {
            json!({"type":"function_call","call_id":"call_fixture","name":"fixture","arguments":"{}"})
        };
        let input = json!([{"role":"user","content":"x".repeat(2*1024*1024+100)},target,call,{"type":if reasoning{"custom_tool_call_output"}else{"function_call_output"},"call_id":"call_fixture","output":"tool result"},{"role":"user","content":"continue"}]);
        let expected = input.clone();
        let server = tokio::spawn(async move {
            let mut active = None;
            let original = receive(&listener, &mut active).await;
            assert_eq!(original["input"], expected);
            send(
                &mut active,
                json!({"type":"codex.response.metadata","metadata":{}}),
            )
            .await;
            send(&mut active, rejected(Some("bad"))).await;
            let clean = receive(&listener, &mut active).await;
            assert_eq!(clean["tools"], original["tools"]);
            for index in [0, 2, 3, 4] {
                assert_eq!(clean["input"][index], original["input"][index]);
            }
            assert!(clean["input"][1].get("encrypted_content").is_none());
            assert!(clean["input"][1].get("id").is_none());
            assert!(clean["client_metadata"].get("x-codex-turn-state").is_none());
            if reasoning {
                assert_eq!(
                    clean["input"][1]["summary"],
                    original["input"][1]["summary"]
                );
            } else {
                assert_eq!(
                    clean["input"][1]["content"],
                    original["input"][1]["content"]
                );
                assert_eq!(clean["input"][1]["phase"], "final_answer");
            }
            send(&mut active, completed("resp_repaired")).await;
        });
        let (events, error) = collect(
            configured(&store, base, policy()),
            operation_transport(input, "encrypted-large", None, !reasoning),
            context("req_encrypted", CancellationToken::new()),
        )
        .await;
        assert!(error.is_none(), "{error:?}");
        assert!(events.iter().any(|e| {
            e.wire_event()
                .is_some_and(|w| w.event_type() == Some("response.completed"))
        }));
        assert!(!events.iter().any(|e| {
            e.wire_event()
                .is_some_and(|w| w.data().to_string().contains("invalid_encrypted_content"))
        }));
        timeout(Duration::from_secs(6), server)
            .await
            .unwrap()
            .unwrap();
    }
}

#[tokio::test]
async fn encrypted_late_error_is_returned_then_next_turn_cleans_only_matching_ciphertext() {
    for boundary in ["output", "window", "bytes"] {
        let store = Arc::new(MemoryAccountStore::default());
        create_account(&store, "acct_provider_contract").await;
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base = format!("http://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(async move {
            let mut active = None;
            receive(&listener, &mut active).await;
            match boundary {
                "output" => {
                    send(
                        &mut active,
                        json!({"type":"response.output_text.delta","delta":"visible"}),
                    )
                    .await
                }
                "window" => tokio::time::sleep(Duration::from_millis(100)).await,
                _ => {
                    send(
                        &mut active,
                        json!({"type":"codex.response.metadata","padding":"x".repeat(2000)}),
                    )
                    .await
                }
            }
            send(&mut active, rejected(Some("bad"))).await;
            let clean = receive(&listener, &mut active).await;
            assert!(clean["input"][0].get("encrypted_content").is_none());
            assert_eq!(clean["input"][1]["encrypted_content"], "cipher-good");
            send(&mut active, completed("resp_next")).await;
        });
        let p = CodexWsResponseRecovery {
            window_ms: if boundary == "window" { 20 } else { 5000 },
            max_prefix_bytes: 1024,
            ..policy()
        };
        let provider = configured(&store, base, p);
        let input = json!([message("bad"),message("good"),{"role":"user","content":"continue"}]);
        let (_, error) = collect(
            provider.clone(),
            operation(input.clone(), "same-session", None),
            context("req_late", CancellationToken::new()),
        )
        .await;
        let error = error.unwrap();
        assert_eq!(
            error.client_visible_upstream_error().unwrap().code(),
            Some("invalid_encrypted_content")
        );
        let (_, error) = collect(
            provider,
            operation(input, "same-session", None),
            context("req_after_late", CancellationToken::new()),
        )
        .await;
        assert!(error.is_none(), "{boundary}: {error:?}");
        timeout(Duration::from_secs(6), server)
            .await
            .unwrap()
            .unwrap();
    }
}

#[tokio::test]
async fn encrypted_unsafe_or_unidentified_items_are_not_replayed() {
    for scenario in [
        "opaque",
        "compaction",
        "duplicate",
        "unknown_id",
        "ambiguous",
        "tool_delta",
        "oversized",
        "disabled",
        "wrong_error",
        "usage",
        "quota",
        "error_output",
        "reference",
        "tool_kind",
        "outbound",
    ] {
        let store = Arc::new(MemoryAccountStore::default());
        create_account(&store, "acct_provider_contract").await;
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base = format!("http://{}", listener.local_addr().unwrap());
        let mut item = message("bad");
        if scenario == "opaque" {
            item["content"] = json!([]);
        }
        if scenario == "compaction" {
            item = json!({"type":"compaction","id":"bad","encrypted_content":"cipher-bad"});
        }
        let mut input = vec![item];
        match scenario {
            "duplicate" => input.push(message("bad")),
            "ambiguous" => input.push(message("other")),
            "tool_delta" => input
                .push(json!({"type":"function_call_output","call_id":"missing","output":"data"})),
            "oversized" => input.push(json!({"role":"user","content":"x".repeat(8*1024*1024)})),
            "reference" => input.push(json!({"type":"message","id":"remote-only"})),
            "tool_kind" => input.extend([json!({"type":"function_call","call_id":"mismatch","name":"fixture","arguments":"{}"}),json!({"type":"custom_tool_call_output","call_id":"mismatch","output":"data"})]),
            _ => {}
        }
        let state = store.clone();
        let server = tokio::spawn(async move {
            let mut active = None;
            receive(&listener, &mut active).await;
            let mut error = rejected(if scenario == "ambiguous" {
                None
            } else if scenario == "unknown_id" {
                Some("absent")
            } else {
                Some("bad")
            });
            if scenario == "wrong_error" {
                error["error"]["code"] = json!("invalid_request_error");
            }
            if scenario == "usage" {
                error["usage"] = json!({"input_tokens":10});
            }
            if scenario == "error_output" {
                error = json!({"type":"response.failed","response":{"status":"failed","error":error["error"],"output":[{"type":"message","role":"assistant","content":[{"type":"output_text","text":"visible"}]}]}});
            }
            if scenario == "quota" {
                exhaust_account_quota(&state, "acct_provider_contract").await;
            }
            send(&mut active, error).await;
            assert!(
                timeout(Duration::from_millis(200), receive(&listener, &mut active))
                    .await
                    .is_err(),
                "{scenario}"
            );
        });
        let p = CodexWsResponseRecovery {
            encrypted_history: scenario != "disabled",
            ..policy()
        };
        let attempt_context = if scenario == "outbound" {
            outbound_context("req_guard", |_| Ok(()))
        } else {
            context("req_guard", CancellationToken::new())
        };
        let (_, error) = collect(
            configured(&store, base, p),
            operation(json!(input), "guard", None),
            attempt_context,
        )
        .await;
        assert!(error.is_some(), "{scenario}");
        timeout(Duration::from_secs(6), server)
            .await
            .unwrap()
            .unwrap();
    }
}

#[tokio::test]
async fn encrypted_repair_failure_does_not_stack_another_recovery() {
    let store = Arc::new(MemoryAccountStore::default());
    create_account(&store, "acct_provider_contract").await;
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(async move {
        let mut active = None;
        receive(&listener, &mut active).await;
        send(&mut active, rejected(Some("bad"))).await;
        receive(&listener, &mut active).await;
        send(&mut active, rejected(Some("other"))).await;
        assert!(
            timeout(Duration::from_millis(200), receive(&listener, &mut active))
                .await
                .is_err()
        );
    });
    let p = CodexWsResponseRecovery {
        enabled: true,
        max_retries: 5,
        full_history: true,
        ..policy()
    };
    let (_, error) = collect(
        configured(&store, base, p),
        operation(
            json!([message("bad"), message("other")]),
            "one-repair",
            None,
        ),
        context("req_once", CancellationToken::new()),
    )
    .await;
    let error = error.unwrap();
    assert!(error.retry_is_prohibited());
    assert_eq!(error.send_state(), UpstreamSendState::Sent);
    timeout(Duration::from_secs(6), server)
        .await
        .unwrap()
        .unwrap();
}

#[tokio::test]
async fn encrypted_cache_is_session_scoped_bounded_and_expires() {
    for scenario in [
        "session",
        "key",
        "installation",
        "capacity",
        "digests",
        "expiry",
    ] {
        let store = Arc::new(MemoryAccountStore::default());
        create_account(&store, "acct_provider_contract").await;
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base = format!("http://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(async move {
            let mut active = None;
            for id in ["bad", "other"] {
                receive(&listener, &mut active).await;
                send(
                    &mut active,
                    json!({"type":"response.output_text.delta","delta":"visible"}),
                )
                .await;
                send(&mut active, rejected(Some(id))).await;
            }
            let payload = receive(&listener, &mut active).await;
            if scenario == "digests" {
                assert!(payload["input"][0].get("encrypted_content").is_none());
                assert_eq!(payload["input"][1]["encrypted_content"], "cipher-other");
            } else {
                assert_eq!(payload["input"][0]["encrypted_content"], "cipher-bad");
            }
            send(&mut active, completed("resp_bound")).await;
        });
        let p = CodexWsResponseRecovery {
            encrypted_history_max_sessions: 1,
            encrypted_history_max_digests_per_session: 1,
            ..policy()
        };
        let provider = configured(&store, base, p);
        for id in ["bad", "other"] {
            let session = if id == "other" && scenario == "capacity" {
                "second"
            } else {
                "first"
            };
            let (_, error) = collect(
                provider.clone(),
                operation(json!([message(id)]), session, None),
                context("req_record", CancellationToken::new()),
            )
            .await;
            assert!(error.is_some());
        }
        if scenario == "expiry" {
            tokio::time::pause();
            tokio::time::advance(Duration::from_secs(1801)).await;
            tokio::time::resume();
        }
        let session = if scenario == "session" {
            "different"
        } else {
            "first"
        };
        let next_context = if scenario == "key" {
            AttemptContext::new(
                RequestAttemptContext::new(
                    ModelRequestId::new("req_other_key").unwrap(),
                    ClientApiKeyId::new("different-key").unwrap(),
                ),
                NonZeroU32::new(1).unwrap(),
                SystemTime::now() + Duration::from_secs(30),
                account_policy(),
                AccountAttemptContext::new(BTreeSet::<ProviderAccountId>::new(), None, None)
                    .with_account_scope(contract_account_scope()),
                None,
                CancellationToken::new(),
            )
        } else if scenario == "installation" {
            let configuration = gateway_core::account::OpaqueProviderData::new(json!({
                "mode":"custom", "userAgent":"codex-tui/1.0.0", "installationSeed":KEY_INSTALLATION_SEED
            }).as_object().unwrap().clone());
            let profile = provider.resolve_request_profile(&configuration).unwrap();
            context_with_state_owner_and_profile(
                "req_changed_identity",
                "acct_provider_contract",
                None,
                Some(profile),
            )
        } else {
            context("req_capacity", CancellationToken::new())
        };
        let (_, error) = collect(
            provider,
            operation(json!([message("bad"), message("other")]), session, None),
            next_context,
        )
        .await;
        assert!(error.is_none(), "{scenario}: {error:?}");
        timeout(Duration::from_secs(6), server)
            .await
            .unwrap()
            .unwrap();
    }
}
