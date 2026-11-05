//! 同一账号租约内的空正文 WS opening 403 恢复，不重放已发送的 payload

use super::*;
use crate::config::CodexWsOpeningRecovery;

fn eligible(error: &CodexClientError) -> Option<Option<Duration>> {
    let CodexClientError::Upstream {
        status,
        body,
        diagnostics,
        transport,
        send_phase,
        retry_after_seconds,
        ..
    } = error
    else {
        return None;
    };
    if *status != reqwest::StatusCode::FORBIDDEN
        || *transport != CodexBackendTransport::WebSocket
        || *send_phase != CodexUpstreamSendPhase::BeforePayload
        || !body.is_empty()
        || diagnostics.identity_authorization_error.is_some()
        || diagnostics.identity_error_code.is_some()
    {
        return None;
    }
    Some(retry_after_seconds.map(Duration::from_secs))
}

pub(super) async fn create(
    client: &CodexBackendClient,
    request: &CodexResponsesRequest,
    context: CodexRequestContext<'_>,
    account_id: &str,
    policy: CodexWsOpeningRecovery,
    recovery_used: &mut bool,
) -> Result<CodexBackendStreamingResponse, CodexClientError> {
    let requirement = transport_requirement(request);
    let allowed = policy.enabled && request.generate() && requirement.allows_connection_restart();
    let trace = context.trace.cloned().unwrap_or_default();
    let max_retries = policy.max_retries.min(5);
    let delay = Duration::from_millis(policy.retry_delay_ms.clamp(50, 5000));
    let mut retries = 0;
    loop {
        let result = client
            .create_response_stream_with_pool_account(request, context, Some(account_id))
            .await;
        let error = match result {
            Ok(response) => {
                if *recovery_used {
                    trace.record(
                        "transport.opening_recovery.completed",
                        json!({"retries":retries,"transport":format!("{:?}",response.transport)}),
                    );
                }
                return Ok(response);
            }
            Err(error) => error,
        };
        let Some(retry_after) = allowed.then(|| eligible(&error)).flatten() else {
            return Err(error);
        };
        // 不截短上游等待建议；超出本策略的单次等待上限时直接保留拒绝
        if retry_after.is_some_and(|wait| wait > Duration::from_secs(5)) {
            return Err(error);
        }
        *recovery_used = true;
        trace.record(
            "transport.opening_recovery.rejected",
            json!({"status":403,"sendPhase":"before_payload","retry":retries}),
        );
        if retries < max_retries {
            retries += 1;
            let wait = retry_after.unwrap_or(delay);
            trace.record(
                "transport.opening_recovery.retry",
                json!({"retry":retries,"maxRetries":max_retries,"delayMs":wait.as_millis()}),
            );
            tracing::info!(
                request_id = context.request_id,
                account_id,
                retry = retries,
                max_retries,
                "Retrying an empty-body WebSocket opening rejection"
            );
            // 外层 select 同时约束握手、等待和降级，取消或到期不继续开连接
            tokio::time::sleep(wait).await;
            continue;
        }
        if !policy.http_fallback || !requirement.allows_pre_send_http_fallback() {
            trace.record("transport.opening_recovery.exhausted", json!({"retries":retries,"httpAllowed":requirement.allows_pre_send_http_fallback()}));
            return Err(error);
        }
        // 保持 client、凭据、代理和账号租约，只改变本次发送方式，不修改会话回退状态
        let mut http_request = request.clone();
        http_request.use_websocket = false;
        http_request.force_http_sse = true;
        if let Some(wait) = retry_after {
            tokio::time::sleep(wait).await;
        }
        trace.record(
            "transport.opening_recovery.fallback",
            json!({"retries":retries,"transport":"http_sse"}),
        );
        tracing::info!(
            request_id = context.request_id,
            account_id,
            retries,
            "Using native HTTP/SSE after empty-body WebSocket opening rejection"
        );
        return client
            .create_response_stream_with_pool_account(&http_request, context, Some(account_id))
            .await;
    }
}
