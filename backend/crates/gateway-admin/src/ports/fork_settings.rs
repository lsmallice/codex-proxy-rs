//! 二开配置管理合同，分别投影持久化值与当前进程生效值
use crate::model::{AdminError, MutationContext};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ForkSettingsSnapshot {
    pub revision: String,
    pub active: Value,
    pub saved: Value,
    pub restart_required: bool,
}

#[async_trait]
pub trait ForkSettingsService: Send + Sync {
    async fn load(&self) -> Result<ForkSettingsSnapshot, AdminError>;
    async fn save(
        &self,
        revision: String,
        values: Value,
        context: &MutationContext,
    ) -> Result<ForkSettingsSnapshot, AdminError>;
}
