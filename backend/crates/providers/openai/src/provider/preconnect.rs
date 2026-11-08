//! 基于真实调度候选准备会话备用握手，不取得业务租约或保存输入历史

use super::*;

impl CodexProvider {
    pub(super) fn prepare_standby(
        &self,
        client: &CodexBackendClient,
        request: &CodexResponsesRequest,
        context: &AttemptContext,
        lease: &CodexCredentialLease,
    ) {
        let Some(target) = lease.preconnect_candidate.as_ref() else {
            return;
        };
        tracing::debug!(account_id = target.id().as_str(), enabled = client.preconnect_enabled(), quota = ?target.quota().access(), has_session = request.client_session_id.is_some(), has_conversation = request.local_conversation_id.is_some(), event = "candidate", "WS standby preparation");
        if !client.preconnect_enabled()
            || target.quota().access() != gateway_core::account::QuotaAccessState::Allowed
            || target
                .plan_type()
                .is_none_or(|plan| plan.is_empty() || plan == "unknown")
            || request.local_conversation_id.is_none()
            || (request.client_thread_id.is_none() && request.client_session_id.is_none())
            || request.previous_response_id().is_some()
            || lease.transport() == crate::credential::ResponsesTransport::Http
            || request.force_http_sse
        {
            return;
        }
        let Ok(permit) = self.preconnect_slots.clone().try_acquire_owned() else {
            return;
        };
        let header_bytes: usize = request
            .passthrough_headers
            .iter()
            .map(|(name, value)| name.as_str().len() + value.as_bytes().len())
            .sum();
        if header_bytes > 8192 {
            return;
        }
        // 只复制握手所需的最小字段，不能把完整对话捕获进后台任务
        let mut next = CodexResponsesRequest::from_body(Map::from_iter([(
            "model".to_owned(),
            Value::String(request.model().to_owned()),
        )]));
        if let Some(tier) = request.service_tier() {
            next.body_mut().insert("service_tier".into(), json!(tier));
        }
        next.client_session_id = request.client_session_id.clone();
        next.client_thread_id = request.client_thread_id.clone();
        next.client_request_id = request.client_request_id.clone();
        next.local_conversation_id = request.local_conversation_id.clone();
        next.codex_window_id = request.codex_window_id.clone();
        next.parent_thread_id = request.parent_thread_id.clone();
        next.downstream_websocket_connection_id =
            request.downstream_websocket_connection_id.clone();
        next.beta_features = request.beta_features.clone();
        next.include_timing_metrics = request.include_timing_metrics.clone();
        next.version = request.version.clone();
        next.memgen_request = request.memgen_request.clone();
        next.passthrough_headers = request.passthrough_headers.clone();
        // 元数据有界复制，历史 input、工具定义与 output 从不进入后台任务
        if request
            .turn_metadata
            .as_ref()
            .is_some_and(|value| value.len() > 8192)
        {
            return;
        }
        next.turn_metadata = request.turn_metadata.clone();
        if let Some(subagent) = request
            .client_metadata()
            .and_then(|value| value.get("x-openai-subagent"))
            .and_then(Value::as_str)
        {
            if subagent.len() > 256 {
                return;
            }
            next.set_client_metadata(Some(json!({"x-openai-subagent": subagent})));
        }
        let target = target.clone();
        let response_url = self.responses_url.clone();
        let selector = Arc::clone(&self.selector);
        let loading = Arc::clone(&self.preconnect_loading);
        let client = client.clone();
        let key = context.client_api_key_ref().clone();
        let request_id = context.request_id().as_str().to_owned();
        let cancellation = context.cancellation().clone();
        let downstream = context
            .response_control()
            .and_then(
                gateway_core::engine::response_control::ResponseControl::connection_cancellation,
            )
            .cloned();
        client.clone().spawn_preconnect_preparation(async move {
            let _permit = permit;
            let prepared = async {
                let Ok(_loading) = loading.acquire_owned().await else { return; };
                if !selector.response_recovery_allowed(&target).await { tracing::debug!(event = "not_ready", "WS standby preparation"); return; }
                let Ok((account, runtime)) = selector.load_preconnect_credential(&target, &response_url).await else { tracing::debug!(event = "credential_changed", "WS standby preparation"); return; };
                if runtime.transport == crate::credential::ResponsesTransport::Http || runtime.authentication.oauth().is_none() { return; }
                let Ok(cookie_header) = build_cookie_header(&runtime.cookies) else { return; };
                let Ok(authorization) = runtime.authentication.authorization_header() else { return; };
                let installation_id = client.profile_state().effective_installation_id(&key, account.id(), &runtime.installation_id);
                scope_request_to_account(&mut next, &installation_id, RequestAccountScope::Different);
                let Ok(client) = client.for_account(&account) else { return; };
                let context = codex_request_context(&next, &request_id, &account, &installation_id, &authorization, cookie_header.as_ref(), CodexAccountSelectionTelemetry::NONE);
                if let Err(error) = client.schedule_preconnect(&next, context, account.id().as_str(), downstream.as_ref()) {
                    tracing::debug!(account_id = account.id().as_str(), error = %error, "WS standby preparation skipped");
                }
            };
            tokio::select! {
                _ = cancellation.cancelled() => {},
                _ = tokio::time::sleep(Duration::from_secs(10)) => {},
                _ = prepared => {},
            }
        });
    }
}
