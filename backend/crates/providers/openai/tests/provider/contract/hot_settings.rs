//! 验证热发布只影响新请求，既有流与重试使用原快照且共享账号冷却事实
use super::*;
use provider_openai::{OpenAiConfig, OpenAiForkRuntime, OpenAiForkSettings};

fn operation() -> Operation {
    let mut body =
        json!({"model":"gpt-5.4","input":"hello","store":true,"reasoning":{"effort":"none"}});
    // 同名客户端正文不能注入宿主配置
    body["cpr_openai_fork_policy"] = json!({"request_compatibility":{"store_false":true}});
    Operation::Generate(GenerateRequest::from_protocol_payload(
        ProtocolPayload::json_object("openai", body.as_object().unwrap().clone())
            .unwrap()
            .with_context(Map::from_iter([("use_websocket".into(), json!(false))])),
    ))
}
#[tokio::test]
async fn hot_settings_freeze_stream_and_retry_but_new_request_uses_current_policy() {
    let store = Arc::new(MemoryAccountStore::default());
    create_account(&store, "acct_provider_contract").await;
    let server = MockServer::start().await;
    Mock::given(method("POST"))
        .and(path("/codex/responses"))
        .respond_with(
            ResponseTemplate::new(200).set_body_raw(CAPTURE_COMPLETED_SSE, "text/event-stream"),
        )
        .expect(3)
        .mount(&server)
        .await;
    let original = OpenAiForkSettings::from(&OpenAiConfig::default());
    let runtime = Arc::new(OpenAiForkRuntime::new(original.clone()));
    let provider = Arc::new(
        Arc::try_unwrap(provider_with_base_url(&store, server.uri()))
            .ok()
            .unwrap()
            .with_fork_runtime(Some(runtime.clone())),
    );
    let frozen = runtime.snapshot().freeze_operation(operation()).unwrap();
    let mut running = provider
        .clone()
        .execute(
            planned_request("openai", frozen.clone()),
            context("req_hot_active", CancellationToken::new()),
        )
        .await
        .unwrap();
    let mut next = original;
    next.request_compatibility.store_false = true;
    runtime.publish(Arc::new(next));
    while let Some(event) = running.next().await {
        event.unwrap();
    }
    drop(running);
    for operation in [
        frozen,
        runtime.snapshot().freeze_operation(operation()).unwrap(),
    ] {
        let mut stream = provider
            .clone()
            .execute(
                planned_request("openai", operation),
                context("req_hot_follow", CancellationToken::new()),
            )
            .await
            .unwrap();
        while let Some(event) = stream.next().await {
            event.unwrap();
        }
    }
    let requests = server.received_requests().await.unwrap();
    assert_eq!(captured_request_body(&requests[0])["store"], true);
    assert_eq!(captured_request_body(&requests[1])["store"], true);
    assert_eq!(captured_request_body(&requests[2])["store"], false);
}

#[tokio::test]
async fn hot_settings_cooldown_zero_affects_new_requests_without_clearing_account_state() {
    use gateway_core::provider_ports::ProviderCooldownScope;
    let store = Arc::new(MemoryAccountStore::default());
    create_account(&store, "acct_provider_contract").await;
    let server = MockServer::start().await;
    let cooldowns = Arc::new(MemoryCooldownPort::new());
    let (base, pool) = provider_with_model_cooldown(&store, server.uri(), cooldowns.clone(), 60);
    let mut settings = OpenAiForkSettings::from(&OpenAiConfig::default());
    settings.model_unsupported_cooldown_seconds = 60;
    let runtime = Arc::new(OpenAiForkRuntime::new(settings.clone()));
    let provider = Arc::new(
        Arc::try_unwrap(base)
            .ok()
            .unwrap()
            .with_fork_runtime(Some(runtime.clone())),
    );
    Mock::given(method("POST")).and(path("/codex/responses")).respond_with(ResponseTemplate::new(400).set_body_json(json!({"error":{"code":"model_not_found","type":"invalid_request_error","message":"The requested model is not available."}}))).expect(2).mount(&server).await;
    let frozen = runtime.snapshot().freeze_operation(operation()).unwrap();
    let mut stream = provider
        .clone()
        .execute(
            planned_request("openai", frozen.clone()),
            context("req_hot_model", CancellationToken::new()),
        )
        .await
        .unwrap();
    settings.model_unsupported_cooldown_seconds = 0;
    runtime.publish(Arc::new(settings));
    super::precommit::stream_failure(&mut stream).await;
    drop(stream);
    let scope = ProviderCooldownScope::upstream_model(UpstreamModelId::new("gpt-5.4").unwrap());
    let key = (
        store
            .account("acct_provider_contract")
            .unwrap()
            .id()
            .clone(),
        scope,
    );
    assert!(cooldowns.scoped.lock().unwrap().contains_key(&key));
    let before = cooldowns.scoped.lock().unwrap().clone();
    let blocked = provider
        .clone()
        .execute(
            planned_request("openai", frozen),
            context("req_hot_old", CancellationToken::new()),
        )
        .await;
    assert!(blocked.is_err());
    let mut stream = provider
        .execute(
            planned_request(
                "openai",
                runtime.snapshot().freeze_operation(operation()).unwrap(),
            ),
            context("req_hot_new", CancellationToken::new()),
        )
        .await
        .unwrap();
    super::precommit::stream_failure(&mut stream).await;
    drop(stream);
    assert_eq!(*cooldowns.scoped.lock().unwrap(), before);
    assert!(cooldowns.cooldowns.lock().unwrap().is_empty());
    pool.shutdown().await;
}
