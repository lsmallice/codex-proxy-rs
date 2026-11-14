//! 共享账号交接的存储端口，保持值对象与基础设施错误方向单向
use super::ProviderStoreError;
use crate::account::{AccountHandoff, AccountHandoffPolicy, AccountHandoffSnapshot};
use futures::future::BoxFuture;

/// 存储端只负责幂等、CAS、共享计数与租约原子约束，不决定交接阶段
pub trait AccountHandoffStore: Send + Sync {
    fn history(
        &self,
        page: u32,
        page_size: u32,
    ) -> BoxFuture<'_, Result<crate::account::AccountHandoffPage, ProviderStoreError>>;
    fn policy(&self) -> BoxFuture<'_, Result<AccountHandoffPolicy, ProviderStoreError>>;
    fn save_policy(
        &self,
        policy: AccountHandoffPolicy,
        expected_revision: u64,
    ) -> BoxFuture<'_, Result<bool, ProviderStoreError>>;
    fn get(
        &self,
        id: String,
    ) -> BoxFuture<'_, Result<Option<AccountHandoffSnapshot>, ProviderStoreError>>;
    fn list(&self) -> BoxFuture<'_, Result<Vec<AccountHandoffSnapshot>, ProviderStoreError>>;
    fn start(
        &self,
        state: AccountHandoff,
    ) -> BoxFuture<'_, Result<AccountHandoffSnapshot, ProviderStoreError>>;
    fn update(
        &self,
        state: AccountHandoff,
        expected_revision: u64,
        expected_failures: u64,
    ) -> BoxFuture<'_, Result<bool, ProviderStoreError>>;
    fn feedback(
        &self,
        account_id: String,
        success: bool,
        started_at_ms: Option<u64>,
    ) -> BoxFuture<'_, Result<(), ProviderStoreError>>;
}
