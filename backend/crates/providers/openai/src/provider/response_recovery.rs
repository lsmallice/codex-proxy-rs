//! 同账号 WS 恢复资格与预算；正常前导即时交付，历史恢复要求宿主摘要证明
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
    account: &ProviderAccount,
    slots: Arc<tokio::sync::Semaphore>,
    history_replayed: &mut bool,
) -> Result<CodexBackendStreamingResponse, CodexHandshakeAttemptError> {
    let trace = request_context.trace.cloned().unwrap_or_default();
    let result = create_response_attempt(
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
    if !policy.full_history || *recovery.1 {
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
        && selector.response_recovery_allowed(account).await
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
    result
}

pub(super) struct StreamRecovery {
    policy: crate::config::CodexWsResponseRecovery,
    disconnect_replay: bool,
    permit: Option<tokio::sync::OwnedSemaphorePermit>,
    until: tokio::time::Instant,
    bytes: usize,
    frames: usize,
    retries: u32,
}

impl StreamRecovery {
    pub(super) fn new(
        request: &CodexResponsesRequest,
        policy: crate::config::CodexWsResponseRecovery,
        encrypted: bool,
        slots: Arc<tokio::sync::Semaphore>,
    ) -> Self {
        let disconnect_replay = policy.enabled
            && policy.max_retries > 0
            && transport_requirement(request).allows_connection_restart()
            && replayable_new_request(request);
        Self {
            until: tokio::time::Instant::now() + Duration::from_millis(policy.window_ms.min(10000)),
            permit: (disconnect_replay || encrypted)
                .then(|| slots.try_acquire_owned().ok())
                .flatten(),
            policy,
            disconnect_replay,
            bytes: 0,
            frames: 0,
            retries: 0,
        }
    }

    pub(super) fn close(&mut self) {
        self.permit = None;
    }

    pub(super) async fn expired(&self) {
        if self.permit.is_some() {
            tokio::time::sleep_until(self.until).await;
        } else {
            futures::future::pending::<()>().await;
        }
    }

    fn available(&mut self) -> bool {
        if tokio::time::Instant::now() >= self.until {
            self.close();
        }
        self.permit.is_some()
    }

    // 只检查本次收到的帧，不预读或保留前导；未知内容与计费事实单向关闭窗口
    pub(super) fn inspect(
        &mut self,
        request: &CodexResponsesRequest,
        chunk: &[u8],
        encrypted: Option<&encrypted_history::Attempt>,
    ) -> Option<CodexResponsesRequest> {
        if !self.available() {
            return None;
        }
        self.bytes = self.bytes.saturating_add(chunk.len());
        self.frames += 1;
        let value = (self.bytes <= self.policy.max_prefix_bytes.min(262144) && self.frames <= 32)
            .then(|| serde_json::from_slice::<Value>(chunk).ok())
            .flatten();
        let Some(value) = value else {
            self.close();
            return None;
        };
        let no_usage = [value.get("usage"), value.pointer("/response/usage")]
            .iter()
            .all(|usage| usage.is_none_or(Value::is_null));
        let no_output = value
            .pointer("/response/output")
            .is_none_or(|output| output.as_array().is_some_and(Vec::is_empty));
        if self.retries == 0
            && no_usage
            && no_output
            && let Some(attempt) = encrypted
            && let Some(index) = attempt.observe(request, chunk)
            && let Some(repaired) = attempt.repaired(request, index)
        {
            return Some(repaired);
        }
        if !no_usage
            || !value
                .get("type")
                .and_then(Value::as_str)
                .is_some_and(|event| is_replaceable_response_prelude(event, &value))
        {
            self.close();
        }
        None
    }

    pub(super) fn disconnect(&mut self, error: &CodexClientError) -> Option<Value> {
        if !self.available()
            || !self.disconnect_replay
            || self.retries >= self.policy.max_retries.min(5)
        {
            return None;
        }
        let CodexClientError::WebSocket(error) = error else {
            return None;
        };
        let code = error.close_before_terminal().and_then(|c| c.code());
        let reason = error.transport_failure_reason();
        let allowed = code.is_some_and(|code| self.policy.close_codes.contains(&code))
            || (self.policy.disconnects
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
        allowed.then(|| json!({"closeCode":code,"failureReason":reason,"replay":self.retries + 1,"maxRetries":self.policy.max_retries.min(5),"possibleDuplicateExecution":true}))
    }

    pub(super) fn replayed(&mut self, encrypted: bool) {
        self.retries += 1;
        if encrypted || self.retries >= self.policy.max_retries.min(5) {
            self.close();
        }
    }

    pub(super) async fn allowed(
        &mut self,
        selector: &CodexCredentialSelector,
        quota: &Arc<CodexCredentialQuotaService>,
        account: &ProviderAccount,
        limits: &[ParsedRateLimits],
        context: &AttemptContext,
    ) -> bool {
        if !self.available() {
            return false;
        }
        tokio::select! {
            biased;
            _ = context.cancellation().cancelled() => false,
            _ = context.deadline().wait() => false,
            _ = tokio::time::sleep_until(self.until) => false,
            allowed = async {
                synchronize_passive_quota(quota, account, limits).await;
                selector.response_recovery_allowed(account).await
            } => allowed,
        }
    }
}
