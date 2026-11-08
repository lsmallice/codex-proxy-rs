//! 验证空正文握手恢复、原生 SSE 降级与权限及续接隔离

use super::*;
use gateway_core::event::ProviderEvent;
use provider_openai::config::CodexWsOpeningRecovery;

fn policy() -> CodexWsOpeningRecovery {
    CodexWsOpeningRecovery {
        enabled: true,
        max_retries: 2,
        retry_delay_ms: 50,
        http_fallback: true,
    }
}

fn configured(
    store: &Arc<MemoryAccountStore>,
    base: String,
    policy: CodexWsOpeningRecovery,
) -> Arc<CodexProvider> {
    let provider = provider_with_base_url(store, base);
    Arc::new(
        Arc::try_unwrap(provider)
            .ok()
            .unwrap()
            .with_ws_opening_recovery(policy),
    )
}

fn request(extra: Value, websocket: bool) -> Operation {
    let mut body = json!({"model":"gpt-5.4","input":"hello","store":false,"stream":true,"session_id":"opening-fixture-session"});
    body.as_object_mut()
        .unwrap()
        .extend(extra.as_object().unwrap().clone());
    let needs_pool = websocket || body.get("generate") == Some(&json!(false));
    let mut context = Map::from_iter([("use_websocket".to_owned(), json!(true))]);
    if websocket {
        context.insert(
            "downstream_websocket_connection_id".into(),
            json!("opening-fixture"),
        );
    }
    let generate = GenerateRequest::from_protocol_payload(
        ProtocolPayload::json_object("openai", body.as_object().unwrap().clone())
            .unwrap()
            .with_context(context),
    );
    Operation::Generate(if needs_pool {
        generate.with_provider_session_state(ProviderSessionState::new("openai",json!({"account_id":"acct_provider_contract","conversation_id":"opening-fixture-session","continuation_scope":"persisted"}).as_object().unwrap().clone()).unwrap())
    } else {
        generate
    })
}

async fn respond(socket: &mut TcpStream, status: u16, headers: &str, body: &str) {
    socket.write_all(format!("HTTP/1.1 {status} Response\r\ncontent-length: {}\r\nconnection: close\r\n{headers}\r\n{body}",body.len()).as_bytes()).await.unwrap();
}

async fn denied_opening(listener: &TcpListener, body: &str, headers: &str) -> Vec<u8> {
    let (mut socket, _) = timeout(Duration::from_secs(5), listener.accept())
        .await
        .unwrap()
        .unwrap();
    let request = capture_http_request(&mut socket).await;
    assert!(String::from_utf8_lossy(&request).starts_with("GET "));
    respond(&mut socket, 403, headers, body).await;
    request
}

async fn execute(
    provider: Arc<CodexProvider>,
    operation: Operation,
    context: AttemptContext,
) -> Result<Vec<ProviderEvent>, gateway_core::error::ProviderError> {
    let mut stream = provider
        .execute(planned_request("openai", operation), context)
        .await?;
    assert_eq!(
        stream.metadata().provider_account_id().as_str(),
        "acct_provider_contract"
    );
    let mut events = Vec::new();
    while let Some(event) = timeout(Duration::from_secs(6), stream.next())
        .await
        .unwrap()
    {
        events.push(event?);
    }
    Ok(events)
}

#[tokio::test]
async fn retries_opening_then_uses_native_sse_once_with_same_identity() {
    for retries in [0, 2, 3, 5] {
        let store = Arc::new(MemoryAccountStore::default());
        create_account(&store, "acct_provider_contract").await;
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base = format!("http://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(async move {
            let mut identity = None;
            for _ in 0..=retries.min(2) {
                let head = denied_opening(&listener, "", "").await;
                let head = String::from_utf8_lossy(&head).to_ascii_lowercase();
                let values = head
                    .lines()
                    .filter(|line| {
                        line.starts_with("authorization:")
                            || line.starts_with("chatgpt-account-id:")
                    })
                    .map(str::to_owned)
                    .collect::<Vec<_>>();
                if let Some(expected) = &identity {
                    assert_eq!(&values, expected);
                } else {
                    identity = Some(values);
                }
            }
            let (mut socket, _) = listener.accept().await.unwrap();
            let head = capture_http_request(&mut socket).await;
            let text = String::from_utf8_lossy(&head).to_ascii_lowercase();
            assert!(text.starts_with("post /codex/responses"));
            for value in identity.unwrap() {
                assert!(text.contains(&value));
            }
            respond(
                &mut socket,
                200,
                "content-type: text/event-stream\r\n",
                CAPTURE_COMPLETED_SSE,
            )
            .await;
        });
        let provider = configured(
            &store,
            base,
            CodexWsOpeningRecovery {
                max_retries: retries,
                ..policy()
            },
        );
        let events = execute(
            provider,
            request(json!({}), false),
            context("req_opening_fallback", CancellationToken::new()),
        )
        .await
        .unwrap();
        assert!(events.iter().any(|event| {
            event
                .response_observation()
                .is_some_and(|o| o.transport().as_str() == "http_sse")
        }));
        assert!(events.iter().any(|event| {
            event
                .wire_event()
                .is_some_and(|wire| wire.data()["type"] == "response.completed")
        }));
        server.await.unwrap();
    }
}

#[tokio::test]
async fn successful_retry_sends_one_websocket_payload_without_http() {
    let store = Arc::new(MemoryAccountStore::default());
    create_account(&store, "acct_provider_contract").await;
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(async move {
        denied_opening(&listener, "", "").await;
        let (socket, _) = listener.accept().await.unwrap();
        let mut ws = accept_codex_test_websocket(socket).await;
        let frame = ws.next().await.unwrap().unwrap();
        let body: Value = serde_json::from_str(frame.to_text().unwrap()).unwrap();
        assert_eq!(body["type"], "response.create");
        ws.send(Message::Text(json!({"type":"response.completed","response":{"id":"resp_opening","status":"completed","model":"gpt-5.4","output":[],"usage":{"input_tokens":1,"output_tokens":1,"total_tokens":2}}}).to_string().into())).await.unwrap();
    });
    let events = execute(
        configured(&store, base, policy()),
        request(json!({}), false),
        context("req_opening_retry", CancellationToken::new()),
    )
    .await
    .unwrap();
    assert!(events.iter().any(|e| {
        e.response_observation()
            .is_some_and(|o| o.transport().as_str() == "websocket")
    }));
    server.await.unwrap();
}

#[tokio::test]
async fn http_fallback_preserves_consumed_and_outer_reserved_openings() {
    use gateway_core::engine::connection::ConnectionBudget;
    for (consumed, reserved, websocket_openings) in [
        (1, false, 2),
        (2, false, 1),
        (3, false, 0),
        (0, true, 2),
        (1, true, 1),
        (2, true, 0),
    ] {
        let store = Arc::new(MemoryAccountStore::default());
        create_account(&store, "acct_provider_contract").await;
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base = format!("http://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(async move {
            for _ in 0..websocket_openings {
                denied_opening(&listener, "", "").await;
            }
            let (mut socket, _) = timeout(Duration::from_secs(3), listener.accept())
                .await
                .unwrap()
                .unwrap();
            assert!(
                String::from_utf8_lossy(&capture_http_request(&mut socket).await)
                    .starts_with("POST ")
            );
            respond(
                &mut socket,
                200,
                "content-type: text/event-stream\r\n",
                CAPTURE_COMPLETED_SSE,
            )
            .await;
            assert!(
                timeout(Duration::from_millis(100), listener.accept())
                    .await
                    .is_err()
            );
        });
        let budget = ConnectionBudget::default();
        for _ in 0..consumed {
            budget.begin().unwrap();
        }
        let local_budget = if reserved {
            budget.clone().reserve_next_opening()
        } else {
            budget.clone()
        };
        let ctx = AttemptContext::new(
            RequestAttemptContext::new(
                ModelRequestId::new("req_shared_openings").unwrap(),
                ClientApiKeyId::new("key_openai_contract").unwrap(),
            )
            .with_connection_budget(local_budget),
            NonZeroU32::MIN,
            SystemTime::now() + Duration::from_secs(30),
            account_policy(),
            AccountAttemptContext::new(BTreeSet::new(), None, None)
                .with_account_scope(contract_account_scope()),
            None,
            CancellationToken::new(),
        );
        execute(
            configured(
                &store,
                base,
                CodexWsOpeningRecovery {
                    max_retries: 5,
                    ..policy()
                },
            ),
            request(json!({}), false),
            ctx,
        )
        .await
        .unwrap();
        assert_eq!(budget.remaining_openings(), u32::from(reserved));
        server.await.unwrap();
    }
}

#[tokio::test]
async fn slow_last_websocket_opening_leaves_budget_for_native_fast_path_fallback() {
    let store = Arc::new(MemoryAccountStore::default());
    create_account(&store, "acct_provider_contract").await;
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(async move {
        for _ in 0..2 {
            denied_opening(&listener, "", "").await;
        }
        let (mut slow, _) = listener.accept().await.unwrap();
        assert!(
            String::from_utf8_lossy(&capture_http_request(&mut slow).await).starts_with("GET ")
        );
        let (mut http, _) = timeout(Duration::from_secs(3), listener.accept())
            .await
            .unwrap()
            .unwrap();
        assert!(
            String::from_utf8_lossy(&capture_http_request(&mut http).await).starts_with("POST ")
        );
        respond(
            &mut http,
            200,
            "content-type: text/event-stream\r\n",
            CAPTURE_COMPLETED_SSE,
        )
        .await;
        drop(slow);
        assert!(
            timeout(Duration::from_millis(100), listener.accept())
                .await
                .is_err()
        );
    });
    let events = execute(
        configured(
            &store,
            base,
            CodexWsOpeningRecovery {
                max_retries: 5,
                ..policy()
            },
        ),
        request(json!({}), false),
        context("req_slow_opening_fallback", CancellationToken::new()),
    )
    .await
    .unwrap();
    assert!(events.iter().any(|event| {
        event
            .response_observation()
            .is_some_and(|o| o.transport().as_str() == "http_sse")
    }));
    server.await.unwrap();
}

#[tokio::test]
async fn disabled_policy_and_explicit_denials_do_not_retry() {
    for (enabled, body, headers) in [
        (false, "", ""),
        (
            true,
            r#"{"error":{"code":"permission_denied","message":"Access denied"}}"#,
            "content-type: application/json\r\n",
        ),
        (
            true,
            r#"{"error":{"code":"cyber_policy_session_blocked"}}"#,
            "content-type: application/json\r\n",
        ),
        (
            true,
            "<html>Forbidden</html>",
            "content-type: text/html\r\n",
        ),
        (
            true,
            "",
            "x-openai-authorization-error: account_deactivated\r\n",
        ),
        (true, "", "retry-after: 30\r\n"),
    ] {
        let store = Arc::new(MemoryAccountStore::default());
        create_account(&store, "acct_provider_contract").await;
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base = format!("http://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(async move {
            denied_opening(&listener, body, headers).await;
            assert!(
                timeout(Duration::from_millis(200), listener.accept())
                    .await
                    .is_err()
            );
        });
        let error = execute(
            configured(
                &store,
                base,
                CodexWsOpeningRecovery {
                    enabled,
                    ..policy()
                },
            ),
            request(json!({}), false),
            context("req_opening_denied", CancellationToken::new()),
        )
        .await
        .err()
        .unwrap();
        assert_eq!(error.upstream_status(), Some(403));
        server.await.unwrap();
    }
}

#[tokio::test]
async fn disabled_fallback_and_websocket_only_requests_never_post_http() {
    for (extra, websocket, settings, openings) in [
        (
            json!({}),
            false,
            CodexWsOpeningRecovery {
                http_fallback: false,
                ..policy()
            },
            3,
        ),
        (json!({}), true, policy(), 3),
        (
            json!({}),
            true,
            CodexWsOpeningRecovery {
                max_retries: 5,
                ..policy()
            },
            4,
        ),
        (
            json!({}),
            false,
            CodexWsOpeningRecovery {
                max_retries: 5,
                http_fallback: false,
                ..policy()
            },
            3,
        ),
        (json!({"generate":false}), false, policy(), 1),
    ] {
        let store = Arc::new(MemoryAccountStore::default());
        create_account(&store, "acct_provider_contract").await;
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base = format!("http://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(async move {
            for _ in 0..openings {
                denied_opening(&listener, "", "").await;
            }
            assert!(
                timeout(Duration::from_millis(200), listener.accept())
                    .await
                    .is_err()
            );
        });
        let provider = configured(&store, base, settings);
        let error = execute(
            provider,
            request(extra, websocket),
            context("req_opening_ws_only", CancellationToken::new()),
        )
        .await
        .err()
        .unwrap();
        assert_eq!(
            error.upstream_status(),
            Some(403),
            "ws={websocket}, openings={openings}, kind={:?}, detail={:?}",
            error.kind(),
            error.error_details()
        );
        assert!(error.retry_is_prohibited());
        server.await.unwrap();
    }
}

#[tokio::test]
async fn unknown_previous_scope_keeps_native_recovery_error_without_opening() {
    let store = Arc::new(MemoryAccountStore::default());
    create_account(&store, "acct_provider_contract").await;
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("http://{}", listener.local_addr().unwrap());
    let error = execute(
        configured(&store, base, policy()),
        request(json!({"previous_response_id":"resp_external"}), false),
        context("req_opening_previous", CancellationToken::new()),
    )
    .await
    .err()
    .unwrap();
    assert_eq!(
        error.kind(),
        ProviderErrorKind::ContinuationRecoveryRequired
    );
    assert!(
        timeout(Duration::from_millis(100), listener.accept())
            .await
            .is_err()
    );
}

#[tokio::test]
async fn sse_failure_returns_original_status_and_cannot_loop_or_rotate() {
    for (status, body) in [
        (
            403,
            r#"{"error":{"code":"permission_denied","message":"Forbidden"}}"#,
        ),
        (
            503,
            r#"{"error":{"code":"server_error","message":"Unavailable"}}"#,
        ),
        (
            200,
            "event: response.failed\ndata: {\"type\":\"response.failed\",\"response\":{\"id\":\"resp_failed\",\"error\":{\"code\":\"server_error\",\"message\":\"Unavailable\"}}}\n\n",
        ),
    ] {
        let store = Arc::new(MemoryAccountStore::default());
        create_account(&store, "acct_provider_contract").await;
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base = format!("http://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(async move {
            denied_opening(&listener, "", "").await;
            let (mut socket, _) = listener.accept().await.unwrap();
            assert!(
                String::from_utf8_lossy(&capture_http_request(&mut socket).await)
                    .starts_with("POST ")
            );
            respond(
                &mut socket,
                status,
                "content-type: text/event-stream\r\n",
                body,
            )
            .await;
        });
        let error = execute(
            configured(
                &store,
                base,
                CodexWsOpeningRecovery {
                    max_retries: 0,
                    ..policy()
                },
            ),
            request(json!({}), false),
            context("req_opening_sse_failure", CancellationToken::new()),
        )
        .await
        .err()
        .unwrap();
        assert!(error.retry_is_prohibited());
        if status != 200 {
            assert_eq!(error.upstream_status(), Some(status));
        }
        server.await.unwrap();
    }
}

#[tokio::test]
async fn cancellation_during_backoff_stops_new_connections() {
    let store = Arc::new(MemoryAccountStore::default());
    create_account(&store, "acct_provider_contract").await;
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("http://{}", listener.local_addr().unwrap());
    let cancellation = CancellationToken::new();
    let cancel = cancellation.clone();
    let server = tokio::spawn(async move {
        denied_opening(&listener, "", "retry-after: 3\r\n").await;
        tokio::time::sleep(Duration::from_millis(30)).await;
        cancel.cancel();
        assert!(
            timeout(Duration::from_millis(200), listener.accept())
                .await
                .is_err()
        );
    });
    let error = execute(
        configured(&store, base, policy()),
        request(json!({}), false),
        context("req_opening_cancel", cancellation),
    )
    .await
    .err()
    .unwrap();
    assert_eq!(error.kind(), ProviderErrorKind::Cancelled);
    server.await.unwrap();
}

#[tokio::test]
async fn api_key_opening_and_post_payload_denials_remain_unchanged() {
    for api_key in [false, true] {
        let store = Arc::new(MemoryAccountStore::default());
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base = format!("http://{}", listener.local_addr().unwrap());
        if api_key {
            store
                .seed_api_key(
                    "acct_provider_contract",
                    base.clone(),
                    provider_openai::credential::ResponsesTransport::PreferWebsocket,
                )
                .await;
        } else {
            create_account(&store, "acct_provider_contract").await;
        }
        let server = tokio::spawn(async move {
            if api_key {
                denied_opening(&listener, "", "").await;
            } else {
                let (socket, _) = listener.accept().await.unwrap();
                let mut ws = accept_codex_test_websocket(socket).await;
                let frame = ws.next().await.unwrap().unwrap();
                assert!(frame.is_text());
                ws.send(Message::Text(json!({"type":"error","status":403,"error":{"code":"permission_denied","message":"Forbidden"}}).to_string().into())).await.unwrap();
            }
            assert!(
                timeout(Duration::from_millis(200), listener.accept())
                    .await
                    .is_err()
            );
        });
        let error = execute(
            configured(&store, base, policy()),
            request(json!({}), false),
            context("req_opening_excluded", CancellationToken::new()),
        )
        .await
        .err()
        .unwrap();
        assert_eq!(error.upstream_status(), Some(403));
        assert!(error.retry_is_prohibited());
        server.await.unwrap();
    }
}

#[tokio::test]
async fn all_openings_and_sse_use_the_selected_account_proxy() {
    let upstream = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let upstream_addr = upstream.local_addr().unwrap();
    let proxy = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let proxy_url = format!("socks5h://{}", proxy.local_addr().unwrap());
    let proxy_task = tokio::spawn(async move {
        let mut relays = Vec::new();
        for _ in 0..4 {
            let (mut socket, _) = timeout(Duration::from_secs(5), proxy.accept())
                .await
                .unwrap()
                .unwrap();
            let mut greeting = [0; 2];
            socket.read_exact(&mut greeting).await.unwrap();
            assert_eq!(greeting[0], 5);
            let mut methods = vec![0; usize::from(greeting[1])];
            socket.read_exact(&mut methods).await.unwrap();
            socket.write_all(&[5, 0]).await.unwrap();
            let mut connect = [0; 4];
            socket.read_exact(&mut connect).await.unwrap();
            assert_eq!(connect, [5, 1, 0, 3]);
            let length = socket.read_u8().await.unwrap();
            let mut domain = vec![0; usize::from(length)];
            socket.read_exact(&mut domain).await.unwrap();
            assert_eq!(domain, b"upstream.invalid");
            assert_eq!(socket.read_u16().await.unwrap(), 80);
            let mut target = TcpStream::connect(upstream_addr).await.unwrap();
            socket
                .write_all(&[5, 0, 0, 1, 127, 0, 0, 1, 0, 80])
                .await
                .unwrap();
            relays.push(tokio::spawn(async move {
                let _ = tokio::io::copy_bidirectional(&mut socket, &mut target).await;
            }));
        }
        for relay in relays {
            relay.await.unwrap();
        }
    });
    let server = tokio::spawn(async move {
        for _ in 0..3 {
            denied_opening(&upstream, "", "").await;
        }
        let (mut socket, _) = upstream.accept().await.unwrap();
        assert!(
            String::from_utf8_lossy(&capture_http_request(&mut socket).await).starts_with("POST ")
        );
        respond(
            &mut socket,
            200,
            "content-type: text/event-stream\r\n",
            CAPTURE_COMPLETED_SSE,
        )
        .await;
    });
    let store = Arc::new(MemoryAccountStore::default());
    create_account(&store, "acct_provider_contract").await;
    store.set_egress(
        "acct_provider_contract",
        Some(gateway_core::account::OutboundProxy::parse(&proxy_url).unwrap()),
        None,
    );
    execute(
        configured(
            &store,
            "http://upstream.invalid".into(),
            CodexWsOpeningRecovery {
                max_retries: 5,
                ..policy()
            },
        ),
        request(json!({}), false),
        context("req_opening_proxy", CancellationToken::new()),
    )
    .await
    .unwrap();
    server.await.unwrap();
    proxy_task.await.unwrap();
}

#[tokio::test]
async fn request_deadline_interrupts_retry_after_without_http_fallback() {
    let store = Arc::new(MemoryAccountStore::default());
    create_account(&store, "acct_provider_contract").await;
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let base = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(async move {
        denied_opening(&listener, "", "retry-after: 3\r\n").await;
        assert!(
            timeout(Duration::from_millis(500), listener.accept())
                .await
                .is_err()
        );
    });
    let ctx = AttemptContext::new(
        RequestAttemptContext::new(
            ModelRequestId::new("req_opening_deadline").unwrap(),
            ClientApiKeyId::new("key_openai_contract").unwrap(),
        ),
        NonZeroU32::MIN,
        SystemTime::now() + Duration::from_millis(250),
        account_policy(),
        AccountAttemptContext::new(BTreeSet::new(), None, None)
            .with_account_scope(contract_account_scope()),
        None,
        CancellationToken::new(),
    );
    let error = execute(
        configured(&store, base, policy()),
        request(json!({}), false),
        ctx,
    )
    .await
    .err()
    .unwrap();
    assert_eq!(error.kind(), ProviderErrorKind::Timeout);
    server.await.unwrap();
}
