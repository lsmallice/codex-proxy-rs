//! 验证只读备用选号、无业务预连接和切号后的独占复用
use super::*;
use gateway_core::account::{AccountAffinity, AccountSelectionPolicy, RotationStrategy};
use provider_openai::{OpenAiConfig, OpenAiForkRuntime, OpenAiForkSettings};

const A: &str = "acct_affinity_switch_a";
const B: &str = "acct_affinity_switch_b";

async fn fixture(
    base: String,
) -> (
    Arc<CodexProvider>,
    Arc<CodexWebSocketPool>,
    Arc<OpenAiForkRuntime>,
    Arc<TestLeaseCoordinator>,
) {
    fixture_with_quota(base, QuotaState::allowed(SystemTime::now())).await
}

async fn fixture_with_quota(
    base: String,
    quota: QuotaState,
) -> (
    Arc<CodexProvider>,
    Arc<CodexWebSocketPool>,
    Arc<OpenAiForkRuntime>,
    Arc<TestLeaseCoordinator>,
) {
    fixture_with_config(base, quota, OpenAiConfig::default()).await
}

async fn fixture_with_config(
    base: String,
    quota: QuotaState,
    mut config: OpenAiConfig,
) -> (
    Arc<CodexProvider>,
    Arc<CodexWebSocketPool>,
    Arc<OpenAiForkRuntime>,
    Arc<TestLeaseCoordinator>,
) {
    let store = Arc::new(MemoryAccountStore::default());
    for id in [A, B] {
        create_account(&store, id).await;
        let account = store.account(id).unwrap();
        store
            .apply_quota_access(QuotaAccessChange {
                account_id: account.id().clone(),
                expected_revision: account.revision(),
                state: if id == B {
                    quota
                } else {
                    QuotaState::allowed(SystemTime::now())
                },
            })
            .await
            .unwrap();
    }
    config.ws_preconnect.enabled = true;
    config.ws_preconnect.retry_delay_ms = 100;
    let runtime = Arc::new(OpenAiForkRuntime::new(OpenAiForkSettings::from(&config)));
    config.fork_runtime = Some(runtime.clone());
    let leases = Arc::new(TestLeaseCoordinator::default());
    let (provider, _, pool, _) = provider_and_quota_with_runtime_ports(
        &store,
        Arc::new(MemorySessionAffinity::default()),
        base,
        leases.clone(),
        &config,
        Arc::new(MemoryCooldownPort::default()),
        crate::support::runtime_policy(),
    );
    (provider, pool, runtime, leases)
}

fn attempt(
    id: &str,
    exclude_a: bool,
    strategy: RotationStrategy,
    affinity: AccountAffinity,
) -> AttemptContext {
    attempt_for_key(
        id,
        exclude_a,
        strategy,
        affinity,
        "key_openai_contract",
        None,
    )
}

fn attempt_for_key(
    id: &str,
    exclude_a: bool,
    strategy: RotationStrategy,
    affinity: AccountAffinity,
    key: &str,
    control: Option<gateway_core::engine::response_control::ResponseControl>,
) -> AttemptContext {
    AttemptContext::new(
        RequestAttemptContext::new(
            ModelRequestId::new(id).unwrap(),
            ClientApiKeyId::new(key).unwrap(),
        )
        .with_response_control(control),
        NonZeroU32::new(1).unwrap(),
        SystemTime::now() + Duration::from_secs(15),
        AccountSelectionPolicy::new(strategy, NonZeroU32::new(50).unwrap())
            .with_openai_account_affinity(affinity),
        AccountAttemptContext::new(
            if exclude_a {
                BTreeSet::from([ProviderAccountId::new(A).unwrap()])
            } else {
                BTreeSet::new()
            },
            None,
            None,
        )
        .with_account_scope(contract_account_scope()),
        None,
        CancellationToken::new(),
    )
}

#[tokio::test]
async fn standby_is_not_shared_across_client_keys_even_for_identical_session_headers() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let (provider, pool, _, _) =
        fixture(format!("http://{}", listener.local_addr().unwrap())).await;
    let server = tokio::spawn(async move {
        let mut active = None;
        let mut standby = None;
        for _ in 0..2 {
            let (socket, _) = timeout(Duration::from_secs(5), listener.accept())
                .await
                .unwrap()
                .unwrap();
            let account = Arc::new(Mutex::new(String::new()));
            let capture = account.clone();
            let ws = crate::transport::accept_codex_test_websocket_with(
                socket,
                move |request, response| {
                    *capture.lock().unwrap() = request
                        .headers()
                        .get("chatgpt-account-id")
                        .unwrap()
                        .to_str()
                        .unwrap()
                        .to_owned();
                    response.headers_mut().insert(
                        "sec-websocket-extensions",
                        "permessage-deflate".parse().unwrap(),
                    );
                },
            )
            .await;
            if account.lock().unwrap().ends_with(A) {
                active = Some(ws);
            } else {
                standby = Some(ws);
            }
        }
        let mut standby = standby.unwrap();
        assert!(
            timeout(Duration::from_millis(150), standby.next())
                .await
                .is_err()
        );
        complete(&mut active.unwrap(), "resp_key_a").await;
        let (socket, _) = timeout(Duration::from_secs(5), listener.accept())
            .await
            .unwrap()
            .unwrap();
        let mut fresh = accept_codex_test_websocket(socket).await;
        complete(&mut fresh, "resp_key_b").await;
        assert!(
            timeout(Duration::from_millis(150), standby.next())
                .await
                .is_err()
        );
    });
    for (id, exclude, key) in [
        ("req_preconnect_key_a", false, "key_openai_contract"),
        ("req_preconnect_key_b", true, "key_separate_client"),
    ] {
        let mut stream = provider
            .clone()
            .execute(
                planned_request("openai", operation("same-session")),
                attempt_for_key(
                    id,
                    exclude,
                    RotationStrategy::Smart,
                    AccountAffinity::Relaxed,
                    key,
                    None,
                ),
            )
            .await
            .unwrap();
        while let Some(event) = timeout(Duration::from_secs(8), stream.next())
            .await
            .unwrap()
        {
            event.unwrap();
        }
    }
    server.await.unwrap();
    pool.shutdown().await;
}

fn operation(session: &str) -> Operation {
    Operation::Generate(GenerateRequest::from_protocol_payload(
        ProtocolPayload::json_object("openai", json!({"model":"gpt-5.4","input":"hello","stream":true,"store":false,"session_id":session}).as_object().unwrap().clone()).unwrap()
            .with_context(Map::from_iter([("use_websocket".into(), json!(true)), ("opaque_request_headers".into(), json!([["x-request-id", STANDARD.encode(uuid::Uuid::new_v4().to_string())], ["x-business-test", STANDARD.encode("stable-test-value")]]))])),
    ).with_provider_session_state(ProviderSessionState::new("openai", json!({"account_id":A,"conversation_id":format!("local-{session}"),"continuation_scope":"persisted"}).as_object().unwrap().clone()).unwrap()))
}

async fn complete(ws: &mut tokio_tungstenite::WebSocketStream<TcpStream>, id: &str) {
    let frame = timeout(Duration::from_secs(5), ws.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(
        serde_json::from_str::<Value>(frame.to_text().unwrap()).unwrap()["type"],
        "response.create"
    );
    ws.send(Message::Text(json!({"type":"response.completed","response":{"id":id,"status":"completed","model":"gpt-5.4","output":[],"usage":{"input_tokens":1,"output_tokens":1,"total_tokens":2}}}).to_string().into())).await.unwrap();
}

async fn accept_account(
    listener: &TcpListener,
) -> (String, tokio_tungstenite::WebSocketStream<TcpStream>) {
    let (socket, _) = timeout(Duration::from_secs(5), listener.accept())
        .await
        .unwrap()
        .unwrap();
    let account = Arc::new(Mutex::new(String::new()));
    let capture = account.clone();
    let ws =
        crate::transport::accept_codex_test_websocket_with(socket, move |request, response| {
            *capture.lock().unwrap() = request
                .headers()
                .get("chatgpt-account-id")
                .unwrap()
                .to_str()
                .unwrap()
                .to_owned();
            response.headers_mut().insert(
                "sec-websocket-extensions",
                "permessage-deflate".parse().unwrap(),
            );
        })
        .await;
    let id = account.lock().unwrap().clone();
    (id, ws)
}

#[tokio::test]
async fn standby_max_age_is_checked_before_claim_even_when_retention_is_longer() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut config = OpenAiConfig::default();
    config.ws_pool.max_age_ms = 500;
    let (provider, pool, _, _) = fixture_with_config(
        format!("http://{}", listener.local_addr().unwrap()),
        QuotaState::allowed(SystemTime::now()),
        config,
    )
    .await;
    let server = tokio::spawn(async move {
        let mut active = None;
        let mut standby = None;
        for _ in 0..2 {
            let (id, ws) = accept_account(&listener).await;
            if id.ends_with(A) {
                active = Some(ws);
            } else {
                standby = Some(ws);
            }
        }
        complete(&mut active.unwrap(), "resp_expiry_a").await;
        let (id, mut fresh) = accept_account(&listener).await;
        assert!(id.ends_with(B));
        complete(&mut fresh, "resp_expiry_b").await;
        // 超龄储备不能接到请求，维护回收产生 Close 也不是业务帧
        if let Ok(Some(Ok(message))) =
            timeout(Duration::from_millis(100), standby.as_mut().unwrap().next()).await
        {
            assert!(!matches!(message, Message::Text(_)));
        }
    });
    for (id, exclude) in [("req_expiry_a", false), ("req_expiry_b", true)] {
        if exclude {
            tokio::time::sleep(Duration::from_millis(650)).await;
        }
        let mut stream = provider
            .clone()
            .execute(
                planned_request("openai", operation("expiry-session")),
                attempt(
                    id,
                    exclude,
                    RotationStrategy::Smart,
                    AccountAffinity::Strict,
                ),
            )
            .await
            .unwrap();
        while let Some(event) = timeout(Duration::from_secs(5), stream.next())
            .await
            .unwrap()
        {
            event.unwrap();
        }
    }
    server.await.unwrap();
    pool.shutdown().await;
}

#[tokio::test]
#[expect(
    clippy::result_large_err,
    reason = "上游握手回调要求原始 HTTP Response 错误类型"
)]
async fn full_backoff_table_still_preserves_exhausted_retry_pause() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let mut config = OpenAiConfig::default();
    config.ws_preconnect.max_connections = 1;
    config.ws_preconnect.max_connections_per_account = 1;
    config.ws_preconnect.max_connecting = 1;
    config.ws_preconnect.max_retries = 0;
    config.ws_preconnect.failure_backoff_seconds = 5;
    let (provider, pool, _, _) = fixture_with_config(
        format!("http://{}", listener.local_addr().unwrap()),
        QuotaState::allowed(SystemTime::now()),
        config,
    )
    .await;
    let server = tokio::spawn(async move {
        let mut active = None;
        for _ in 0..2 {
            let (socket, _) = timeout(Duration::from_secs(5), listener.accept())
                .await
                .unwrap()
                .unwrap();
            let result = tokio_tungstenite::accept_hdr_async_with_config(
                socket,
                |request: &tungstenite::handshake::server::Request,
                 mut response: tungstenite::handshake::server::Response| {
                    if request
                        .headers()
                        .get("chatgpt-account-id")
                        .unwrap()
                        .to_str()
                        .unwrap()
                        .ends_with(B)
                    {
                        return Err(tungstenite::http::Response::builder()
                            .status(403)
                            .body(Some(String::new()))
                            .unwrap());
                    }
                    response.headers_mut().insert(
                        "sec-websocket-extensions",
                        "permessage-deflate".parse().unwrap(),
                    );
                    Ok(response)
                },
                Some(crate::transport::websocket_accept_config()),
            )
            .await;
            if let Ok(ws) = result {
                active = Some(ws);
            }
        }
        let mut active = active.unwrap();
        complete(&mut active, "resp_pause_a").await;
        complete(&mut active, "resp_pause_b").await;
        assert!(
            timeout(Duration::from_millis(500), listener.accept())
                .await
                .is_err(),
            "new preparation bypassed the exhausted retry pause"
        );
    });
    for id in ["req_pause_a", "req_pause_b"] {
        let mut stream = provider
            .clone()
            .execute(
                planned_request("openai", operation("pause-session")),
                attempt(id, false, RotationStrategy::Smart, AccountAffinity::Strict),
            )
            .await
            .unwrap();
        while let Some(event) = timeout(Duration::from_secs(5), stream.next())
            .await
            .unwrap()
        {
            event.unwrap();
        }
        drop(stream);
        tokio::time::sleep(Duration::from_millis(500)).await;
    }
    server.await.unwrap();
    pool.shutdown().await;
}

#[tokio::test]
async fn standby_handshake_has_no_payload_or_lease_and_is_claimed_after_switch() {
    for strategy in [
        RotationStrategy::Smart,
        RotationStrategy::RoundRobin,
        RotationStrategy::Sticky,
        RotationStrategy::QuotaResetPriority,
    ] {
        for affinity in [
            AccountAffinity::Relaxed,
            AccountAffinity::Preferred,
            AccountAffinity::Strict,
        ] {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let (provider, pool, _, leases) =
                fixture(format!("http://{}", listener.local_addr().unwrap())).await;
            let (ready_tx, ready_rx) = oneshot::channel();
            let server = tokio::spawn(async move {
                let mut active = None;
                let mut standby = None;
                for _ in 0..2 {
                    let (socket, _) = timeout(Duration::from_secs(5), listener.accept())
                        .await
                        .unwrap()
                        .unwrap();
                    let account = Arc::new(Mutex::new(String::new()));
                    let capture = account.clone();
                    let ws = crate::transport::accept_codex_test_websocket_with(
                        socket,
                        move |request, response| {
                            *capture.lock().unwrap() = request
                                .headers()
                                .get("chatgpt-account-id")
                                .unwrap()
                                .to_str()
                                .unwrap()
                                .to_owned();
                            response.headers_mut().insert(
                                "sec-websocket-extensions",
                                "permessage-deflate".parse().unwrap(),
                            );
                        },
                    )
                    .await;
                    if account.lock().unwrap().ends_with(A) {
                        active = Some(ws);
                    } else {
                        standby = Some(ws);
                    }
                }
                let mut active = active.unwrap();
                let mut standby = standby.unwrap();
                assert!(
                    timeout(Duration::from_millis(150), standby.next())
                        .await
                        .is_err(),
                    "preconnect sent a business frame"
                );
                complete(&mut active, "resp_standby_a").await;
                ready_tx.send(()).unwrap();
                complete(&mut standby, "resp_standby_b").await;
                assert!(
                    timeout(Duration::from_millis(150), listener.accept())
                        .await
                        .is_err(),
                    "switch created another connection"
                );
            });
            let mut first = provider
                .clone()
                .execute(
                    planned_request("openai", operation("standby-session")),
                    attempt("req_standby_first", false, strategy, affinity),
                )
                .await
                .unwrap();
            while let Some(event) = timeout(Duration::from_secs(8), first.next()).await.unwrap() {
                event.unwrap();
            }
            ready_rx.await.unwrap();
            drop(first);
            assert_eq!(
                leases.requests.lock().unwrap().len(),
                1,
                "background acquired a business lease"
            );
            let mut second = provider
                .execute(
                    planned_request("openai", operation("standby-session")),
                    attempt("req_standby_second", true, strategy, affinity),
                )
                .await
                .unwrap();
            assert_eq!(second.metadata().provider_account_id().as_str(), B);
            while let Some(event) = timeout(Duration::from_secs(5), second.next())
                .await
                .unwrap()
            {
                event.unwrap();
            }
            drop(second);
            server.await.unwrap();
            pool.shutdown().await;
        }
    }
}

#[tokio::test]
#[expect(
    clippy::result_large_err,
    reason = "上游握手回调要求原始 HTTP Response 错误类型"
)]
async fn standby_empty_403_retries_but_identity_rejection_does_not() {
    for case in 0..3 {
        let explicit = case != 0;
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let (provider, pool, _, leases) =
            fixture(format!("http://{}", listener.local_addr().unwrap())).await;
        let count = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let seen = count.clone();
        let server = tokio::spawn(async move {
            let mut active = None;
            let expected = if explicit { 1 } else { 3 };
            for _ in 0..expected + 1 {
                let (socket, _) = timeout(Duration::from_secs(5), listener.accept())
                    .await
                    .unwrap()
                    .unwrap();
                let seen = seen.clone();
                let result = tokio_tungstenite::accept_hdr_async_with_config(socket, move |request: &tungstenite::handshake::server::Request, mut response: tungstenite::handshake::server::Response| {
                    if request.headers().get("chatgpt-account-id").unwrap().to_str().unwrap().ends_with(B) {
                        seen.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                        let mut response = tungstenite::http::Response::builder().status(403);
                        if case == 1 { response = response.header("x-openai-authorization-error", "account_deactivated"); }
                        return Err(response.body(Some(if case == 2 { "permission denied".into() } else { String::new() })).unwrap());
                    }
                    response.headers_mut().insert("sec-websocket-extensions", "permessage-deflate".parse().unwrap());
                    Ok(response)
                }, Some(crate::transport::websocket_accept_config())).await;
                if let Ok(ws) = result {
                    active = Some(ws);
                }
            }
            complete(&mut active.unwrap(), "resp_preconnect_rejection").await;
            assert!(
                timeout(Duration::from_millis(750), listener.accept())
                    .await
                    .is_err()
            );
        });
        let mut stream = provider
            .execute(
                planned_request("openai", operation("retry-session")),
                attempt(
                    "req_preconnect_reject",
                    false,
                    RotationStrategy::Smart,
                    AccountAffinity::Relaxed,
                ),
            )
            .await
            .unwrap();
        while let Some(event) = timeout(Duration::from_secs(8), stream.next())
            .await
            .unwrap()
        {
            event.unwrap();
        }
        drop(stream);
        server.await.unwrap();
        assert_eq!(
            count.load(std::sync::atomic::Ordering::SeqCst),
            if explicit { 1 } else { 3 }
        );
        assert_eq!(leases.requests.lock().unwrap().len(), 1);
        pool.shutdown().await;
    }
}

#[tokio::test]
async fn disabling_preconnect_closes_standby_without_interrupting_business() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let (provider, pool, runtime, _) =
        fixture(format!("http://{}", listener.local_addr().unwrap())).await;
    let (ready_tx, ready_rx) = oneshot::channel();
    let (closed_tx, closed_rx) = oneshot::channel();
    let server = tokio::spawn(async move {
        let mut active = None;
        let mut standby = None;
        for _ in 0..2 {
            let (socket, _) = timeout(Duration::from_secs(5), listener.accept())
                .await
                .unwrap()
                .unwrap();
            let account = Arc::new(Mutex::new(String::new()));
            let capture = account.clone();
            let ws = crate::transport::accept_codex_test_websocket_with(
                socket,
                move |request, response| {
                    *capture.lock().unwrap() = request
                        .headers()
                        .get("chatgpt-account-id")
                        .unwrap()
                        .to_str()
                        .unwrap()
                        .to_owned();
                    response.headers_mut().insert(
                        "sec-websocket-extensions",
                        "permessage-deflate".parse().unwrap(),
                    );
                },
            )
            .await;
            if account.lock().unwrap().ends_with(A) {
                active = Some(ws);
            } else {
                standby = Some(ws);
            }
        }
        let mut standby = standby.unwrap();
        assert!(
            timeout(Duration::from_millis(150), standby.next())
                .await
                .is_err()
        );
        ready_tx.send(()).unwrap();
        assert!(matches!(
            timeout(Duration::from_secs(3), standby.next())
                .await
                .unwrap(),
            Some(Ok(Message::Close(_)))
        ));
        let _ = standby.flush().await;
        closed_tx.send(()).unwrap();
        complete(&mut active.unwrap(), "resp_preconnect_disable").await;
    });
    let execution = tokio::spawn(async move {
        let mut stream = provider
            .execute(
                planned_request("openai", operation("disable-session")),
                attempt(
                    "req_preconnect_disable",
                    false,
                    RotationStrategy::Smart,
                    AccountAffinity::Relaxed,
                ),
            )
            .await
            .unwrap();
        while let Some(event) = timeout(Duration::from_secs(8), stream.next())
            .await
            .unwrap()
        {
            event.unwrap();
        }
    });
    ready_rx.await.unwrap();
    let mut settings = runtime.snapshot().as_ref().clone();
    settings.ws_preconnect.enabled = false;
    runtime.publish(Arc::new(settings));
    pool.maintain_idle_connections().await;
    closed_rx.await.unwrap();
    execution.await.unwrap();
    server.await.unwrap();
    pool.shutdown().await;
}

#[tokio::test]
async fn unknown_or_exhausted_quota_does_not_trigger_standby_handshakes() {
    for quota in [
        QuotaState::observed_unknown(SystemTime::now()),
        QuotaState::exhausted(QuotaEvidence::UsageLimitReached, SystemTime::now(), None),
    ] {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let (provider, pool, _, leases) =
            fixture_with_quota(format!("http://{}", listener.local_addr().unwrap()), quota).await;
        let server = tokio::spawn(async move {
            let (socket, _) = timeout(Duration::from_secs(5), listener.accept())
                .await
                .unwrap()
                .unwrap();
            let mut ws = accept_codex_test_websocket(socket).await;
            complete(&mut ws, "resp_quota_gate").await;
            assert!(
                timeout(Duration::from_millis(400), listener.accept())
                    .await
                    .is_err(),
                "unhealthy quota created a standby handshake"
            );
        });
        let mut stream = provider
            .execute(
                planned_request("openai", operation("quota-gate")),
                attempt(
                    "req_quota_gate",
                    false,
                    RotationStrategy::Smart,
                    AccountAffinity::Relaxed,
                ),
            )
            .await
            .unwrap();
        while let Some(event) = timeout(Duration::from_secs(5), stream.next())
            .await
            .unwrap()
        {
            event.unwrap();
        }
        drop(stream);
        server.await.unwrap();
        assert_eq!(leases.requests.lock().unwrap().len(), 1);
        pool.shutdown().await;
    }
}

#[tokio::test]
async fn downstream_disconnect_releases_unused_standby() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let (provider, pool, _, _) =
        fixture(format!("http://{}", listener.local_addr().unwrap())).await;
    let server = tokio::spawn(async move {
        let mut active = None;
        let mut standby = None;
        for _ in 0..2 {
            let (socket, _) = timeout(Duration::from_secs(5), listener.accept())
                .await
                .unwrap()
                .unwrap();
            let account = Arc::new(Mutex::new(String::new()));
            let capture = account.clone();
            let ws = crate::transport::accept_codex_test_websocket_with(
                socket,
                move |request, response| {
                    *capture.lock().unwrap() = request
                        .headers()
                        .get("chatgpt-account-id")
                        .unwrap()
                        .to_str()
                        .unwrap()
                        .to_owned();
                    response.headers_mut().insert(
                        "sec-websocket-extensions",
                        "permessage-deflate".parse().unwrap(),
                    );
                },
            )
            .await;
            if account.lock().unwrap().ends_with(A) {
                active = Some(ws);
            } else {
                standby = Some(ws);
            }
        }
        let mut active = active.unwrap();
        let mut standby = standby.unwrap();
        assert!(
            timeout(Duration::from_millis(150), standby.next())
                .await
                .is_err()
        );
        complete(&mut active, "resp_downstream_close").await;
        assert!(matches!(
            timeout(Duration::from_secs(3), standby.next())
                .await
                .unwrap(),
            Some(Ok(Message::Close(_)))
        ));
        let _ = standby.flush().await;
    });
    let closed = CancellationToken::new();
    let control = gateway_core::engine::response_control::ResponseControl::default()
        .with_connection_cancellation(closed.clone());
    let request = operation("downstream-session")
        .replace_protocol_wire(
            None,
            Map::from_iter([
                ("use_websocket".into(), json!(true)),
                (
                    "downstream_websocket_connection_id".into(),
                    json!("standby-downstream"),
                ),
            ]),
        )
        .unwrap();
    let mut stream = provider
        .execute(
            planned_request("openai", request),
            attempt_for_key(
                "req_standby_close",
                false,
                RotationStrategy::Smart,
                AccountAffinity::Relaxed,
                "key_openai_contract",
                Some(control),
            ),
        )
        .await
        .unwrap();
    while let Some(event) = timeout(Duration::from_secs(8), stream.next())
        .await
        .unwrap()
    {
        event.unwrap();
    }
    drop(stream);
    closed.cancel();
    server.await.unwrap();
    pool.shutdown().await;
}
