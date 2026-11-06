//! 验证 next 等待不占执行名额，硬上限拒绝请求且不跳过插件

use super::*;
use gateway_core::error::GatewayErrorKind;

fn context() -> core::Context {
    core::Context {
        plugin_instance_id: None,
        call_id: uuid::Uuid::new_v4().to_string(),
        parent_call_id: None,
        extensions: Default::default(),
        plan: None,
        request_id: uuid::Uuid::new_v4().to_string(),
        cancellation: CancellationToken::new(),
    }
}

fn plugin_request() -> core::Request {
    ::http::Request::builder()
        .uri("/v1/responses")
        .header("authorization", "Bearer fixture-secret")
        .body(core::empty_body())
        .unwrap()
}

#[tokio::test]
async fn route_filter_bypasses_saturated_plugin_before_admission() {
    let limits = RpcLimits {
        maximum_calls: 1,
        maximum_executing_calls: 1,
        maximum_queued_calls: 0,
        maximum_callbacks: 8,
        ..Default::default()
    };
    let (cache, runtime, generation, plan) = http_plugin_with_filters(
        serde_json::json!({"http":true,"mode":"passthrough"}),
        limits,
        PluginFailurePolicy::Reject,
        BTreeMap::from([(
            "http-plugin".into(),
            vec![gateway_plugin_runtime::PluginHttpRoute {
                method: "POST".into(),
                path: "/v1/responses".into(),
            }],
        )]),
    )
    .await;
    let (tx, rx) = tokio::sync::oneshot::channel();
    let blocked = tokio::spawn({
        let plan = plan.clone();
        async move {
            plan.handle_http(
                context(),
                ::http::Request::builder()
                    .method("POST")
                    .uri("/v1/responses?stream=1")
                    .header("authorization", "Bearer fixture-secret")
                    .body(core::empty_body())
                    .unwrap(),
                compose(Vec::new(), move |_: core::Request| {
                    Box::pin(async move {
                        tx.send(()).unwrap();
                        futures::future::pending().await
                    })
                }),
            )
            .await
        }
    });
    tokio::time::timeout(Duration::from_secs(5), rx)
        .await
        .unwrap()
        .unwrap();
    for (method, path, allowed) in [
        ("GET", "/api/admin/accounts", true),
        ("GET", "/healthz", true),
        ("GET", "/v1/responses", true),
        ("POST", "/v1/responses/compact", true),
        ("POST", "/v1/responses", false),
    ] {
        let expected = path.to_owned();
        let result = plan
            .handle_http(
                context(),
                ::http::Request::builder()
                    .method(method)
                    .uri(path)
                    .body(core::empty_body())
                    .unwrap(),
                compose(Vec::new(), move |request: core::Request| {
                    Box::pin(async move {
                        assert!(allowed);
                        assert_eq!(request.uri().path(), expected);
                        Ok(core::Response::new(core::empty_body()))
                    })
                }),
            )
            .await;
        assert_eq!(result.is_ok(), allowed, "{method} {path}");
    }
    blocked.abort();
    let _ = blocked.await;
    drop(plan);
    drop(generation);
    runtime.shutdown().await;
    drop(cache);
}

#[tokio::test]
async fn http_waiting_next_releases_execution_for_delegate_plugin() {
    assert_capacity(PluginFailurePolicy::Delegate, RpcLimits::default(), false).await;
}

#[tokio::test]
async fn http_waiting_next_releases_execution_for_reject_plugin() {
    assert_capacity(PluginFailurePolicy::Reject, RpcLimits::default(), false).await;
}

#[tokio::test]
async fn http_in_flight_limit_never_delegates_and_recovers_after_cancellation() {
    for policy in [PluginFailurePolicy::Delegate, PluginFailurePolicy::Reject] {
        assert_capacity(
            policy,
            RpcLimits {
                maximum_calls: 64,
                ..Default::default()
            },
            true,
        )
        .await;
    }
}

#[tokio::test]
async fn configured_512_calls_reaches_new_bound_and_releases_after_cancellation() {
    assert_capacity(
        PluginFailurePolicy::Reject,
        RpcLimits {
            maximum_calls: 512,
            maximum_callbacks: 1024,
            ..Default::default()
        },
        true,
    )
    .await;
}

async fn assert_capacity(policy: PluginFailurePolicy, limits: RpcLimits, saturated: bool) {
    let concurrent_calls = if limits.maximum_calls > 256 {
        limits.maximum_calls
    } else {
        64
    };
    let (cache, runtime, generation, plan) =
        http_plugin_with_policy("passthrough", limits, policy).await;
    let release = Arc::new(tokio::sync::Semaphore::new(0));
    let (started, mut entered) = tokio::sync::mpsc::unbounded_channel();
    let mut slow_calls = tokio::task::JoinSet::new();
    for index in 0..concurrent_calls {
        let plan = plan.clone();
        let release = release.clone();
        let started = started.clone();
        slow_calls.spawn(async move {
            plan.handle_http(
                context(),
                plugin_request(),
                compose(Vec::new(), move |request: core::Request| {
                    assert_eq!(request.uri(), "/rewritten?encoded=%2F");
                    let release = release.clone();
                    Box::pin(async move {
                        started.send(()).unwrap();
                        release.acquire().await.unwrap().forget();
                        Ok(core::Response::new(core::empty_body()))
                    })
                }),
            )
            .await
        });
        tokio::time::timeout(Duration::from_secs(5), entered.recv())
            .await
            .unwrap_or_else(|_| panic!("第 {} 个调用未能进入下游", index + 1))
            .unwrap();
    }

    for uri in [
        "/healthz",
        "/assets/index.js",
        "/api/settings",
        "/v1/responses",
    ] {
        let request = ::http::Request::builder()
            .uri(uri)
            .header("authorization", "Bearer fixture-secret")
            .body(core::empty_body())
            .unwrap();
        let result = tokio::time::timeout(
            Duration::from_secs(3),
            plan.handle_http(
                context(),
                request,
                compose(Vec::new(), move |request: core::Request| {
                    assert!(!saturated, "容量耗尽时不得执行下游");
                    assert_eq!(request.uri(), "/rewritten?encoded=%2F");
                    Box::pin(async { Ok(core::Response::new(core::empty_body())) })
                }),
            ),
        )
        .await
        .expect("下游等待不应阻塞其它请求");
        if saturated {
            assert!(matches!(result, Err(MiddlewareError::Gateway(error))
                if error.kind() == GatewayErrorKind::MiddlewareCapacityUnavailable));
        } else {
            assert_eq!(result.unwrap().headers()["x-http-plugin"], "active");
        }
    }

    if saturated {
        slow_calls.abort_all();
        while let Some(result) = slow_calls.join_next().await {
            assert!(matches!(result, Err(error) if error.is_cancelled()));
        }
    }
    release.add_permits(concurrent_calls);
    while let Some(result) = slow_calls.join_next().await {
        let response = result.unwrap().unwrap();
        assert_eq!(response.headers()["x-http-plugin"], "active");
    }
    // 取消确认到达前仍持有在途名额，探测只允许遇到明确的暂时过载
    let response = tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            let result = plan
                .handle_http(
                    context(),
                    plugin_request(),
                    compose(Vec::new(), |request: core::Request| {
                        Box::pin(async move {
                            assert_eq!(request.uri(), "/rewritten?encoded=%2F");
                            Ok(core::Response::new(core::empty_body()))
                        })
                    }),
                )
                .await;
            match result {
                Ok(response) => break response,
                Err(MiddlewareError::Gateway(error))
                    if error.kind() == GatewayErrorKind::MiddlewareCapacityUnavailable =>
                {
                    tokio::task::yield_now().await
                }
                Err(error) => panic!("unexpected error: {error}"),
            }
        }
    })
    .await
    .unwrap();
    assert_eq!(response.headers()["x-http-plugin"], "active");
    drop(response);
    drop(plan);
    drop(generation);
    runtime.shutdown().await;
    crate::generation::wait_until_empty(cache.path()).await;
}
