//! 显式 SSE 网关状态提示，与真实输出共用流，不进入上游解析或计量

use super::*;
use crate::config::CodexHappyFirstToken;
use gateway_core::engine::execution::ClientTransport;
use sha2::{Digest, Sha256};

pub(super) struct Notification {
    deadline: Option<Instant>,
    ready: bool,
}

impl Notification {
    pub(super) fn new(
        policy: &CodexHappyFirstToken,
        context: &AttemptContext,
        request: &CodexResponsesRequest,
        account: &ProviderAccount,
    ) -> Self {
        let enabled = policy.enabled
            && (policy.all_client_keys
                || policy
                    .client_key_ids
                    .iter()
                    .any(|id| id == context.client_api_key_ref().as_str()))
            && context.client_transport() == ClientTransport::HttpSse
            && matches!(context.endpoint(), "/v1/responses" | "/responses")
            && context.allows_replayable_prelude()
            && context.privacy().is_none()
            && request.stream()
            && !request.is_guardian()
            && account.authentication_kind() == crate::credential::CODEX_AUTHENTICATION_KIND_OAUTH;
        if !enabled {
            return Self {
                deadline: None,
                ready: false,
            };
        }
        // 请求 ID 的固定摘要使同请求的恢复和换号沿用同一个延迟，不保存用户输入
        let digest = Sha256::digest(context.request_id().as_str().as_bytes());
        let random = u64::from_be_bytes(digest[..8].try_into().expect("eight digest bytes"));
        let span = policy
            .max_delay_ms
            .saturating_sub(policy.min_delay_ms)
            .min(60000);
        let delay = policy.min_delay_ms.min(60000) + random % (span + 1);
        Self {
            deadline: Some(context.timing_started_at() + Duration::from_millis(delay)),
            ready: false,
        }
    }

    pub(super) fn reset_readiness(&mut self) {
        self.ready = false;
    }

    pub(super) fn observe(&mut self, events: &[ProviderEvent], closed: bool) {
        if closed {
            self.deadline = None;
            return;
        }
        // 必须先交付真实前导，不能凭空提交响应头或把握手成功当本轮模型响应
        self.ready |= events.iter().any(|event| {
            event.is_replayable_prelude()
                && event
                    .wire_event()
                    .is_some_and(|wire| wire.event_type().is_some())
        });
    }

    pub(super) async fn wait(&self) {
        match self.deadline.filter(|_| self.ready) {
            Some(deadline) => tokio::time::sleep_until(deadline.into()).await,
            None => std::future::pending().await,
        }
    }

    pub(super) fn take(&mut self, context: &AttemptContext) -> Option<ProviderEvent> {
        self.deadline.take()?;
        if !context.claim_stream_notification() {
            return None;
        }
        context.trace().record(
            "gateway.status.delta",
            json!({"synthetic":true,"gatewayStatus":"upstream_connected"}),
        );
        let wire = ProtocolWireEvent::json(
            OPENAI_PROTOCOL,
            Some("gateway.status.delta".into()),
            json!({
                "type":"gateway.status.delta", "delta":"upstream_connected",
                "metadata":{"gateway_status":"upstream_connected"}
            }),
        )
        .expect("fixed protocol identifier");
        Some(ProviderEvent::local_control(wire))
    }
}
