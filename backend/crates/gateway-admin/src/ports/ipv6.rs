//! IPv6 控制协议与本地分配日志端口，控制令牌不进入管理 API

use async_trait::async_trait;

use super::{proxy::ProxyImportGuard, store::AdminStoreResult};
use crate::model::{AdminError, MutationContext, Revision, ipv6::*};

#[async_trait]
pub trait Ipv6Controller: Send + Sync {
    fn id(&self) -> &str;
    async fn status(&self) -> Result<Ipv6Status, AdminError>;
    async fn allocate(&self) -> Result<Ipv6RemoteLease, AdminError>;
    async fn activate(&self, lease: &Ipv6RemoteLease) -> Result<(), AdminError>;
    async fn retire(&self, lease: &Ipv6RemoteLease) -> Result<(), AdminError>;
}

#[async_trait]
pub trait Ipv6ProxyStore: Send + Sync {
    /// 会话锁跨越外部调用，取消时关闭连接释放，不持有空闲事务
    async fn lock(&self, request_id: &str) -> AdminStoreResult<Box<dyn ProxyImportGuard>>;
    async fn begin(
        &self,
        input: Ipv6Allocation,
        context: &MutationContext,
    ) -> AdminStoreResult<(Ipv6Lease, bool)>;
    async fn get(&self, request_id: &str) -> AdminStoreResult<Ipv6Lease>;
    async fn list(&self) -> AdminStoreResult<Vec<Ipv6LeaseView>>;
    async fn policy(&self) -> AdminStoreResult<Ipv6ImportPolicy>;
    async fn set_policy(
        &self,
        controller_id: Option<&str>,
        revision: u64,
        context: &MutationContext,
    ) -> AdminStoreResult<Ipv6ImportPolicy>;
    async fn start_allocation(&self, request_id: &str) -> AdminStoreResult<()>;
    async fn postpone(&self, request_id: &str) -> AdminStoreResult<()>;
    async fn pending(&self, controller_id: &str) -> AdminStoreResult<Vec<String>>;
    async fn save_remote(&self, request_id: &str, lease: Ipv6RemoteLease) -> AdminStoreResult<()>;
    async fn mark_unknown(&self, request_id: &str) -> AdminStoreResult<()>;
    async fn complete(
        &self,
        request_id: &str,
        context: &MutationContext,
    ) -> AdminStoreResult<Revision>;
    async fn begin_retire(
        &self,
        request_id: &str,
        cancel: bool,
        context: &MutationContext,
    ) -> AdminStoreResult<()>;
    async fn finish_retire(
        &self,
        request_id: &str,
        context: &MutationContext,
    ) -> AdminStoreResult<Revision>;
}
