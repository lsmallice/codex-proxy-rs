//! OpenAI 二开配置与请求级快照，不包含凭据、地址或用户正文
use crate::config::{
    CodexWsOpeningRecovery, CodexWsResponseRecovery, OpenAiConfig, OpenAiConfigError,
};
use gateway_core::operation::Operation;
use serde::{Deserialize, Serialize};
use std::sync::{Arc, RwLock};

const REQUEST_POLICY_KEY: &str = "cpr_openai_fork_policy";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OpenAiForkSettings {
    #[serde(default)]
    pub request_compatibility: crate::config::CodexRequestCompatibility,
    pub model_unsupported_cooldown_seconds: u64,
    pub ws_opening_recovery: CodexWsOpeningRecovery,
    #[serde(default)]
    pub ws_preconnect: crate::config::CodexWsPreconnect,
    pub ws_response_recovery: CodexWsResponseRecovery,
}
impl From<&OpenAiConfig> for OpenAiForkSettings {
    fn from(config: &OpenAiConfig) -> Self {
        Self {
            request_compatibility: config.request_compatibility,
            model_unsupported_cooldown_seconds: config.model_unsupported_cooldown_seconds,
            ws_opening_recovery: config.ws_opening_recovery,
            ws_preconnect: config.ws_preconnect,
            ws_response_recovery: config.ws_response_recovery.clone(),
        }
    }
}
impl OpenAiForkSettings {
    /// 宿主注入非 wire 上下文，客户端正文中的同名字段不能覆盖此值
    pub fn freeze_operation(
        &self,
        operation: Operation,
    ) -> Result<Operation, gateway_core::validation::OperationError> {
        let context = match &operation {
            Operation::Generate(request) => request.protocol_payload().context(),
            _ => return Ok(operation),
        };
        let mut context = context.clone();
        context.insert(REQUEST_POLICY_KEY.into(), serde_json::json!(self));
        operation.replace_protocol_wire(None, context)
    }

    pub(crate) fn from_operation(operation: &Operation) -> Option<Self> {
        let Operation::Generate(request) = operation else {
            return None;
        };
        serde_json::from_value(
            request
                .protocol_payload()
                .context()
                .get(REQUEST_POLICY_KEY)?
                .clone(),
        )
        .ok()
    }

    pub fn apply(self, config: &mut OpenAiConfig) -> Result<(), OpenAiConfigError> {
        let mut candidate = config.clone();
        candidate.request_compatibility = self.request_compatibility;
        candidate.model_unsupported_cooldown_seconds = self.model_unsupported_cooldown_seconds;
        candidate.ws_opening_recovery = self.ws_opening_recovery;
        candidate.ws_preconnect = self.ws_preconnect;
        candidate.ws_response_recovery = self.ws_response_recovery;
        candidate.resolve_and_validate(std::path::Path::new("."))?;
        config.model_unsupported_cooldown_seconds = candidate.model_unsupported_cooldown_seconds;
        config.request_compatibility = candidate.request_compatibility;
        config.ws_opening_recovery = candidate.ws_opening_recovery;
        config.ws_preconnect = candidate.ws_preconnect;
        config.ws_response_recovery = candidate.ws_response_recovery;
        Ok(())
    }
}

/// 新请求共享当前值，已冻结的请求与重试继续持有旧值
#[derive(Debug)]
pub struct OpenAiForkRuntime(RwLock<Arc<OpenAiForkSettings>>);
impl PartialEq for OpenAiForkRuntime {
    fn eq(&self, other: &Self) -> bool {
        std::ptr::eq(self, other)
    }
}
impl Eq for OpenAiForkRuntime {}
impl OpenAiForkRuntime {
    pub fn new(settings: OpenAiForkSettings) -> Self {
        Self(RwLock::new(Arc::new(settings)))
    }
    pub fn snapshot(&self) -> Arc<OpenAiForkSettings> {
        self.0
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone()
    }
    /// 校验发生在持久化之前；提交只替换 Arc，不分配连接、清缓存或执行异步操作
    pub fn publish(&self, settings: Arc<OpenAiForkSettings>) {
        *self
            .0
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = settings;
    }
}
