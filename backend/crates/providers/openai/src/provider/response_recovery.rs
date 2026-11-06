//! 同账号租约内有界 WS 补发；历史恢复只接受宿主完成记录的逐项摘要证明
use super::*;
use crate::transport::protocol::responses::is_replaceable_response_prelude;
use sha2::{Digest, Sha256};

const MAX_HISTORY_ITEMS: usize = 512;
const MAX_HISTORY_BYTES: usize = 2 * 1024 * 1024;

#[derive(Clone, Serialize, Deserialize)]
pub(super) struct HistoryProof {
    revision: u64,
    items: Vec<String>,
}
fn hashes(items: &[Value]) -> Option<Vec<String>> {
    if items.is_empty() || items.len() > MAX_HISTORY_ITEMS {
        return None;
    }
    let mut bytes = 0usize;
    items
        .iter()
        .map(|item| {
            // 按对象键排序，不依赖客户端 JSON 字段顺序；不保存历史原文
            let mut value = item.clone();
            value.sort_all_objects();
            let encoded = serde_json::to_vec(&value).ok()?;
            bytes = bytes.checked_add(encoded.len())?;
            if bytes > MAX_HISTORY_BYTES {
                return None;
            }
            Some(hex::encode(Sha256::digest(encoded)))
        })
        .collect()
}
impl HistoryProof {
    pub(super) fn matches(&self, input: &Value, revision: u64) -> bool {
        if self.revision != revision || self.items.is_empty() {
            return false;
        }
        input
            .as_array()
            .and_then(|items| hashes(items))
            .is_some_and(|items| items.starts_with(&self.items))
    }
    pub(super) fn prepare(
        input: &Value,
        previous: Option<&Self>,
        continuation: bool,
        revision: u64,
    ) -> Option<Self> {
        let mut items = hashes(input.as_array()?)?;
        if continuation {
            let previous = previous.filter(|p| p.revision == revision)?;
            if !items.starts_with(&previous.items) {
                let mut combined = previous.items.clone();
                combined.append(&mut items);
                items = combined;
            }
        }
        (items.len() <= MAX_HISTORY_ITEMS).then_some(Self { revision, items })
    }
    pub(super) fn completed(mut self, output: &[Value]) -> Option<Self> {
        if !output.is_empty() {
            self.items.extend(hashes(output)?);
        }
        (self.items.len() <= MAX_HISTORY_ITEMS).then_some(self)
    }
}

fn replayable_new_request(request: &CodexResponsesRequest) -> bool {
    let body = request.body();
    if !request.generate()
        || request.is_guardian()
        || body
            .get("background")
            .is_some_and(|v| v != false && !v.is_null())
        || [
            "previous_response_id",
            "conversation",
            "continuation",
            "tool_choice",
        ]
        .iter()
        .any(|k| body.get(*k).is_some_and(|v| !v.is_null()))
        || body
            .get("tools")
            .is_some_and(|v| !v.is_null() && v.as_array().is_none_or(|a| !a.is_empty()))
    {
        return false;
    }
    let text_message = |item: &Value| {
        let Some(object) = item.as_object() else {
            return false;
        };
        object
            .keys()
            .all(|k| matches!(k.as_str(), "role" | "type" | "content" | "phase"))
            && object.get("type").is_none_or(|v| v == "message")
            && object
                .get("role")
                .and_then(Value::as_str)
                .is_some_and(|v| matches!(v, "user" | "assistant" | "system" | "developer"))
            && match object.get("content") {
                Some(Value::String(s)) => !s.is_empty(),
                Some(Value::Array(parts)) => {
                    !parts.is_empty()
                        && parts.iter().all(|p| {
                            p.as_object().is_some_and(|o| {
                                o.keys().all(|k| matches!(k.as_str(), "type" | "text"))
                                    && matches!(
                                        o.get("type").and_then(Value::as_str),
                                        Some("input_text" | "output_text")
                                    )
                                    && o.get("text").is_some_and(Value::is_string)
                            })
                        })
                }
                _ => false,
            }
    };
    match body.get("input") {
        Some(Value::String(s)) => !s.is_empty(),
        Some(Value::Array(items)) => {
            !items.is_empty() && items.len() <= 2048 && items.iter().all(text_message)
        }
        _ => false,
    }
}

async fn account_unchanged(
    selector: &CodexCredentialSelector,
    quota: &CodexCredentialQuotaService,
    account: &ProviderAccount,
) -> bool {
    if !quota
        .cooldown(account.id())
        .await
        .is_ok_and(|cooldown| cooldown.is_none())
    {
        return false;
    }
    selector
        .current_account(account.id())
        .await
        .is_ok_and(|current| {
            current.enabled()
                && current.revision() == account.revision()
                && current.outbound_proxy() == account.outbound_proxy()
                && current.credential_state() == gateway_core::account::CredentialState::Ready
                && current.quota().access() != gateway_core::account::QuotaAccessState::Exhausted
        })
}

fn prepend(
    response: &mut CodexBackendStreamingResponse,
    frames: Vec<Result<Bytes, CodexClientError>>,
) {
    let body = std::mem::replace(&mut response.body, Box::pin(futures::stream::empty()));
    response.body = Box::pin(futures::stream::iter(frames).chain(body));
}

#[expect(clippy::too_many_arguments)]
pub(super) async fn create(
    client: &CodexBackendClient,
    request: &CodexResponsesRequest,
    mut request_context: CodexRequestContext<'_>,
    account_id: &str,
    deadline: gateway_core::lifecycle::Deadline,
    connection_budget: &gateway_core::engine::connection::ConnectionBudget,
    cancellation: &CancellationToken,
    recovery: (crate::config::CodexWsOpeningRecovery, &mut bool),
    policy: crate::config::CodexWsResponseRecovery,
    full_history_verified: bool,
    selector: &CodexCredentialSelector,
    quota: &Arc<CodexCredentialQuotaService>,
    account: &ProviderAccount,
    slots: Arc<tokio::sync::Semaphore>,
    history_replayed: &mut bool,
    payload_replayed: &mut bool,
    encrypted: Option<&encrypted_history::Attempt>,
    repaired_request: &mut Option<CodexResponsesRequest>,
) -> Result<CodexBackendStreamingResponse, CodexHandshakeAttemptError> {
    let trace = request_context.trace.cloned().unwrap_or_default();
    let mut result = create_response_attempt(
        client,
        request,
        request_context,
        account_id,
        deadline,
        cancellation,
        (recovery.0, recovery.1),
    )
    .await;
    // 返回流时业务帧已发送，沿用原生边界结束发送前建连预算，补发仍受独立次数与期限约束
    if result.is_ok() {
        connection_budget.complete();
    }
    if (!policy.enabled && !policy.full_history && encrypted.is_none()) || *recovery.1 {
        return result;
    }
    let Ok(_permit) = slots.try_acquire_owned() else {
        return result;
    };
    let missing = matches!(&result,Err(CodexHandshakeAttemptError::Client(CodexClientError::WebSocket(error)))
        if matches!(error.continuation_unavailable_reason(),Some(PreviousResponseUnavailableReason::ReusedConnectionLost|PreviousResponseUnavailableReason::FreshConnectionRequired)));
    if policy.full_history
        && full_history_verified
        && missing
        && account_unchanged(selector, quota, account).await
    {
        let mut current = request.clone();
        current.set_previous_response_id(None);
        clear_request_turn_state(&mut current);
        request_context.turn_state = None;
        *recovery.1 = true;
        *history_replayed = true;
        trace.record(
            "transport.continuation_recovery.replay",
            json!({"fullHistoryVerified":true,"replay":1,"sendState":"not_sent"}),
        );
        return create_response_attempt(
            client,
            &current,
            request_context,
            account_id,
            deadline,
            cancellation,
            (crate::config::CodexWsOpeningRecovery::default(), recovery.1),
        )
        .await;
    }
    let disconnect_replay = policy.enabled
        && policy.max_retries > 0
        && transport_requirement(request).allows_connection_restart()
        && replayable_new_request(request);
    if !disconnect_replay && encrypted.is_none() {
        return result;
    }
    let mut frames = Vec::new();
    let mut bytes = 0usize;
    let mut inspected_frames = 0usize;
    let mut retries = 0u32;
    let max_retries = policy.max_retries.min(5);
    // 整条恢复链共用窗口和检查量，不因再次建连获得新的缓冲预算
    let until = tokio::time::Instant::now() + Duration::from_millis(policy.window_ms.min(10000));
    loop {
        let Ok(response) = result.as_mut() else {
            return result;
        };
        if response.transport != CodexBackendTransport::WebSocket
            || (retries > 0 && retries >= max_retries)
        {
            return result;
        }
        let next = tokio::select! {
            biased;
            _=cancellation.cancelled()=>{prepend(response,frames);return result;},
            _=deadline.wait()=>{prepend(response,frames);return result;},
            _=tokio::time::sleep_until(until)=>{prepend(response,frames);return result;},
            chunk=response.body.next()=>chunk,
        };
        match next {
            Some(Ok(chunk)) => {
                bytes = bytes.saturating_add(chunk.len());
                inspected_frames += 1;
                let in_budget =
                    bytes <= policy.max_prefix_bytes.min(262144) && inspected_frames <= 32;
                let value = in_budget
                    .then(|| serde_json::from_slice::<Value>(&chunk).ok())
                    .flatten();
                let no_usage = value.as_ref().is_some_and(|value| {
                    [value.get("usage"), value.pointer("/response/usage")]
                        .iter()
                        .all(|usage| usage.is_none_or(Value::is_null))
                });
                let no_output = value.as_ref().is_some_and(|value| {
                    value
                        .pointer("/response/output")
                        .is_none_or(|output| output.as_array().is_some_and(Vec::is_empty))
                });
                if retries == 0
                    && no_usage
                    && no_output
                    && let Some(attempt) = encrypted
                    && let Some(index) = attempt.observe(request, &chunk)
                    && let Some(current) = attempt.repaired(request, index)
                {
                    let allowed = tokio::select! {
                        biased;
                        _=cancellation.cancelled()=>false,
                        _=deadline.wait()=>false,
                        _=tokio::time::sleep_until(until)=>false,
                        allowed=async {
                            synchronize_passive_quota_headers(quota, account, &response.rate_limit_headers).await;
                            let updates = take_rate_limit_updates(response.rate_limit_updates.as_ref()).await;
                            synchronize_passive_quota_headers(quota, account, &rate_limit_update_headers(&updates)).await;
                            account_unchanged(selector, quota, account).await
                        }=>allowed,
                    };
                    if allowed {
                        trace.record(
                            "transport.encrypted_history.replay",
                            json!({"replay":1,"bufferedBytes":bytes,"lossy":true}),
                        );
                        drop(result);
                        *recovery.1 = true;
                        *payload_replayed = true;
                        request_context.turn_state = None;
                        *repaired_request = Some(current);
                        return create_response_attempt(
                            client,
                            repaired_request.as_ref().expect("repaired request"),
                            request_context,
                            account_id,
                            deadline,
                            cancellation,
                            (crate::config::CodexWsOpeningRecovery::default(), recovery.1),
                        )
                        .await;
                    }
                }
                let safe = no_usage
                    && value.as_ref().is_some_and(|value| {
                        value
                            .get("type")
                            .and_then(Value::as_str)
                            .is_some_and(|event| is_replaceable_response_prelude(event, value))
                    });
                frames.push(Ok(chunk));
                if !safe {
                    prepend(response, frames);
                    return result;
                }
            }
            Some(Err(error)) => {
                let (code, reason) = match &error {
                    CodexClientError::WebSocket(error) => (
                        error.close_before_terminal().and_then(|c| c.code()),
                        error.transport_failure_reason(),
                    ),
                    _ => (None, None),
                };
                let allowed = code.is_some_and(|code| policy.close_codes.contains(&code))
                    || (policy.disconnects
                        && matches!(
                            reason,
                            Some(
                                "reset_without_closing_handshake"
                                    | "tcp_reset"
                                    | "connection_aborted"
                                    | "broken_pipe"
                                    | "unexpected_eof"
                            )
                        ));
                if !disconnect_replay
                    || !allowed
                    || !account_unchanged(selector, quota, account).await
                {
                    frames.push(Err(error));
                    prepend(response, frames);
                    return result;
                }
                // 保留第一轮真实额度观测，再决定是否还能复用账号；不将旧前导交给客户端
                synchronize_passive_quota_headers(quota, account, &response.rate_limit_headers)
                    .await;
                let updates = take_rate_limit_updates(response.rate_limit_updates.as_ref()).await;
                synchronize_passive_quota_headers(
                    quota,
                    account,
                    &rate_limit_update_headers(&updates),
                )
                .await;
                if !account_unchanged(selector, quota, account).await
                    || tokio::time::Instant::now() >= until
                    || cancellation.is_cancelled()
                    || deadline.is_elapsed()
                {
                    frames.push(Err(error));
                    prepend(response, frames);
                    return result;
                }
                retries += 1;
                trace.record("transport.response_recovery.replay",json!({"closeCode":code,"failureReason":reason,"bufferedBytes":bytes,"replay":retries,"maxRetries":max_retries,"possibleDuplicateExecution":true}));
                // 原流的 drop 触发原生池清理，不清整账号连接、不切换账号或代理
                drop(result);
                frames.clear();
                *recovery.1 = true;
                *payload_replayed = true;
                result = create_response_attempt(
                    client,
                    request,
                    request_context,
                    account_id,
                    deadline,
                    cancellation,
                    (crate::config::CodexWsOpeningRecovery::default(), recovery.1),
                )
                .await;
            }
            None => {
                prepend(response, frames);
                return result;
            }
        }
    }
}
