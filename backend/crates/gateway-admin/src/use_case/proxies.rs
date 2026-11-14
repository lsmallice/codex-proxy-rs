//! 账号出站代理的管理、账号解绑与连通性测试用例

use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};

use async_trait::async_trait;
use futures::{StreamExt, stream};
use gateway_core::{
    account::{OutboundProxy, ProviderAccountId},
    routing::ProviderKind,
    runtime::SnapshotControl,
};
use tokio::sync::Semaphore;

use super::{map_store_error, publish_committed};
use crate::{
    model::{
        AdminError, AdminErrorKind, MutationContext, Revision,
        ipv6::{Ipv6Allocation, Ipv6ImportPolicy, Ipv6LeaseView, Ipv6Status},
        provider_credentials::{ProviderQuotaRequest, explicit_plan_type},
        proxies::*,
    },
    ports::{
        ipv6::{Ipv6Controller, Ipv6ProxyStore},
        provider::ProviderAdminRegistry,
        proxy::{PluginProxyAccess, ProbeRoute, ProxyProbe, ProxyStore},
        store::AdminStoreErrorKind,
    },
};

pub(crate) struct DefaultPluginProxyAccess {
    pub(crate) store: Arc<dyn ProxyStore>,
}

#[async_trait]
impl PluginProxyAccess for DefaultPluginProxyAccess {
    async fn list(&self, query: ProxyListQuery) -> Result<ProxyPage, AdminError> {
        list_proxies(self.store.as_ref(), query).await
    }

    async fn resolve(&self, id: &str) -> Result<OutboundProxy, AdminError> {
        if id.is_empty() || id.len() > 128 || id.chars().any(char::is_control) {
            return Err(AdminError::invalid("代理 ID 不合法"));
        }
        let record = self
            .store
            .get(id)
            .await
            .map_err(|error| map_store_error(error, "proxy"))?;
        // 显式选择代理时必须有可用配置，不能沿用账号停用代理后使用部署默认出口的语义
        if !record.enabled {
            return Err(AdminError::invalid("代理已禁用，请启用后再选择"));
        }
        Ok(record.proxy)
    }
}

async fn list_proxies(
    store: &dyn ProxyStore,
    query: ProxyListQuery,
) -> Result<ProxyPage, AdminError> {
    if query.page == 0 || query.search.len() > 256 || query.search.chars().any(char::is_control) {
        return Err(AdminError::invalid("代理查询参数不合法"));
    }
    store
        .list(query)
        .await
        .map_err(|error| map_store_error(error, "proxy"))
}

#[async_trait]
pub trait ProxiesService: Send + Sync {
    async fn ipv6_policy(&self) -> Result<Ipv6ImportPolicy, AdminError>;
    async fn set_ipv6_policy(
        &self,
        enabled: bool,
        revision: u64,
        context: &MutationContext,
    ) -> Result<Ipv6ImportPolicy, AdminError>;
    async fn ipv6_status(&self) -> Result<Ipv6Status, AdminError>;
    async fn list_ipv6(&self) -> Result<Vec<Ipv6LeaseView>, AdminError>;
    async fn allocate_ipv6(
        &self,
        request_id: String,
        name: String,
        account_id: Option<String>,
        context: &MutationContext,
    ) -> Result<Ipv6LeaseView, AdminError>;
    async fn retire_ipv6(
        &self,
        request_id: &str,
        context: &MutationContext,
    ) -> Result<Ipv6LeaseView, AdminError>;
    async fn create_batch(
        &self,
        commands: Vec<Result<NewProxy, AdminError>>,
        context: &MutationContext,
    ) -> Result<Vec<ProxyBatchResult<ProxyMutation>>, AdminError>;
    async fn test_batch(
        &self,
        commands: Vec<Result<TestProxy, AdminError>>,
        context: &MutationContext,
    ) -> Result<Vec<ProxyBatchResult<ProxyRecord>>, AdminError>;
    async fn list(&self, query: ProxyListQuery) -> Result<ProxyPage, AdminError>;
    async fn list_accounts(
        &self,
        query: ProxyAccountListQuery,
    ) -> Result<ProxyAccountPage, AdminError>;
    async fn remove_account(
        &self,
        proxy_id: &str,
        account_id: &str,
        context: &MutationContext,
    ) -> Result<Revision, AdminError>;
    async fn create(
        &self,
        command: NewProxy,
        context: &MutationContext,
    ) -> Result<ProxyMutation, AdminError>;
    async fn update(
        &self,
        command: UpdateProxy,
        context: &MutationContext,
    ) -> Result<ProxyMutation, AdminError>;
    async fn delete(
        &self,
        id: &str,
        revision: Revision,
        context: &MutationContext,
    ) -> Result<Revision, AdminError>;
    async fn test(
        &self,
        id: &str,
        revision: Revision,
        detect_location: bool,
        context: &MutationContext,
    ) -> Result<ProxyRecord, AdminError>;
    /// 探测未保存的连接地址、部署默认出口或服务器直连出口，不写入配置或修改账号绑定
    async fn probe(
        &self,
        proxy: ProbeRoute<'_>,
        detect_location: bool,
    ) -> Result<ProxyTestResult, AdminError>;
}

pub(crate) struct DefaultProxiesService {
    store: Arc<dyn ProxyStore>,
    probe: Arc<dyn ProxyProbe>,
    pub(super) snapshot: Arc<dyn SnapshotControl>,
    providers: ProviderAdminRegistry,
    upstream_info: Option<Arc<dyn crate::ports::peer::PeerUpstreamService>>,
    test_slots: Semaphore,
    pub(super) ipv6_store: Option<Arc<dyn Ipv6ProxyStore>>,
    pub(super) ipv6_controller: Option<Arc<dyn Ipv6Controller>>,
}

impl DefaultProxiesService {
    pub(crate) fn new(
        store: Arc<dyn ProxyStore>,
        probe: Arc<dyn ProxyProbe>,
        snapshot: Arc<dyn SnapshotControl>,
        providers: ProviderAdminRegistry,
        upstream_info: Option<Arc<dyn crate::ports::peer::PeerUpstreamService>>,
        ipv6_store: Option<Arc<dyn Ipv6ProxyStore>>,
        ipv6_controller: Option<Arc<dyn Ipv6Controller>>,
    ) -> Self {
        Self {
            store,
            probe,
            snapshot,
            providers,
            upstream_info,
            test_slots: Semaphore::new(4),
            ipv6_store,
            ipv6_controller,
        }
    }
}

#[async_trait]
impl ProxiesService for DefaultProxiesService {
    async fn ipv6_status(&self) -> Result<Ipv6Status, AdminError> {
        match self.ipv6_controller.as_ref() {
            Some(controller) => controller.status().await,
            None => Ok(Ipv6Status::default()),
        }
    }

    async fn ipv6_policy(&self) -> Result<Ipv6ImportPolicy, AdminError> {
        let store = self
            .ipv6_store
            .as_ref()
            .ok_or_else(|| AdminError::unavailable("IPv6 存储不可用"))?;
        store.policy().await.map_err(super::ipv6::store_error)
    }

    async fn set_ipv6_policy(
        &self,
        enabled: bool,
        revision: u64,
        context: &MutationContext,
    ) -> Result<Ipv6ImportPolicy, AdminError> {
        let store = self
            .ipv6_store
            .as_ref()
            .ok_or_else(|| AdminError::unavailable("IPv6 存储不可用"))?;
        let controller_id = if enabled {
            let controller = self.ipv6_ports()?.1;
            if !controller.status().await?.available {
                return Err(AdminError::unavailable("IPv6 分配服务不可用"));
            }
            Some(controller.id())
        } else {
            None
        };
        store
            .set_policy(controller_id, revision, context)
            .await
            .map_err(super::ipv6::store_error)
    }

    async fn list_ipv6(&self) -> Result<Vec<Ipv6LeaseView>, AdminError> {
        match self.ipv6_store.as_ref() {
            Some(store) => store.list().await.map_err(super::ipv6::store_error),
            None => Ok(Vec::new()),
        }
    }

    async fn allocate_ipv6(
        &self,
        request_id: String,
        name: String,
        account_id: Option<String>,
        context: &MutationContext,
    ) -> Result<Ipv6LeaseView, AdminError> {
        let (store, controller) = self.ipv6_ports()?;
        let request_id = uuid::Uuid::parse_str(&request_id)
            .map_err(|_| AdminError::invalid("IPv6 请求标识须为 UUID"))?
            .to_string();
        let name = validate_name(&name)?;
        if account_id
            .as_ref()
            .is_some_and(|id| id.is_empty() || id.len() > 128 || id.chars().any(char::is_control))
        {
            return Err(AdminError::invalid("账号 ID 不合法"));
        }
        let _guard = store
            .lock(&request_id)
            .await
            .map_err(super::ipv6::store_error)?;
        store
            .begin(
                Ipv6Allocation {
                    request_id: request_id.clone(),
                    controller_id: controller.id().to_owned(),
                    name,
                    account_id,
                },
                context,
            )
            .await
            .map_err(super::ipv6::store_error)?;
        self.reconcile_ipv6_locked(&request_id, context).await?;
        Ok(store
            .get(&request_id)
            .await
            .map_err(super::ipv6::store_error)?
            .view)
    }

    async fn retire_ipv6(
        &self,
        request_id: &str,
        context: &MutationContext,
    ) -> Result<Ipv6LeaseView, AdminError> {
        let (store, controller) = self.ipv6_ports()?;
        if request_id.is_empty()
            || request_id.len() > 128
            || request_id.chars().any(char::is_control)
        {
            return Err(AdminError::invalid("IPv6 请求标识不合法"));
        }
        let _guard = store
            .lock(request_id)
            .await
            .map_err(super::ipv6::store_error)?;
        let current = store
            .get(request_id)
            .await
            .map_err(super::ipv6::store_error)?;
        if current.view.controller_id != controller.id() {
            return Err(AdminError::conflict(
                "此租约属于其他地址池，需恢复对应控制服务配置",
            ));
        }
        if current.view.state != crate::model::ipv6::Ipv6LeaseState::Retired {
            store
                .begin_retire(request_id, true, context)
                .await
                .map_err(super::ipv6::store_error)?;
            self.reconcile_ipv6_locked(request_id, context).await?;
        }
        Ok(store
            .get(request_id)
            .await
            .map_err(super::ipv6::store_error)?
            .view)
    }
    async fn create_batch(
        &self,
        commands: Vec<Result<NewProxy, AdminError>>,
        context: &MutationContext,
    ) -> Result<Vec<ProxyBatchResult<ProxyMutation>>, AdminError> {
        validate_batch_size(commands.len())?;
        let mut stopped = false;
        let mut results = Vec::with_capacity(commands.len());
        for command in commands {
            if stopped {
                results.push(ProxyBatchResult::Skipped);
                continue;
            }
            let result = match command {
                Ok(command) => self.create(command, context).await,
                Err(error) => Err(error),
            };
            stopped = result.as_ref().is_err_and(batch_should_stop);
            results.push(ProxyBatchResult::Completed(result));
        }
        Ok(results)
    }

    async fn test_batch(
        &self,
        commands: Vec<Result<TestProxy, AdminError>>,
        context: &MutationContext,
    ) -> Result<Vec<ProxyBatchResult<ProxyRecord>>, AdminError> {
        validate_batch_size(commands.len())?;
        let mut ids = std::collections::HashSet::with_capacity(commands.len());
        if commands
            .iter()
            .filter_map(|command| command.as_ref().ok())
            .any(|command| !ids.insert(&command.id))
        {
            return Err(AdminError::invalid("批量测试不能包含重复代理"));
        }
        let stopped = AtomicBool::new(false);
        // 与单条测试共用全局容量，批次最多占用两槽，保留交互操作的容量
        Ok(stream::iter(commands)
            .map(|command| {
                let stopped = &stopped;
                async move {
                    if stopped.load(Ordering::Relaxed) {
                        return ProxyBatchResult::Skipped;
                    }
                    let result = match command {
                        Ok(command) => {
                            self.test(
                                &command.id,
                                command.revision,
                                command.detect_location,
                                context,
                            )
                            .await
                        }
                        Err(error) => Err(error),
                    };
                    if result.as_ref().is_err_and(batch_should_stop) {
                        stopped.store(true, Ordering::Relaxed);
                    }
                    ProxyBatchResult::Completed(result)
                }
            })
            .buffered(2)
            .collect()
            .await)
    }

    async fn remove_account(
        &self,
        proxy_id: &str,
        account_id: &str,
        context: &MutationContext,
    ) -> Result<Revision, AdminError> {
        if proxy_id.is_empty() || proxy_id.len() > 128 || proxy_id.chars().any(char::is_control) {
            return Err(AdminError::invalid("代理 ID 不合法"));
        }
        let account_id = ProviderAccountId::new(account_id.to_owned())
            .map_err(|_| AdminError::invalid("账号 ID 不合法"))?;
        let revision = self
            .store
            .remove_account(proxy_id, &account_id, context)
            .await
            .map_err(|error| {
                if error.kind() == AdminStoreErrorKind::Conflict {
                    AdminError::conflict("账号的代理绑定已变化，请刷新后重试")
                } else {
                    map_store_error(error, "proxy account")
                }
            })?;
        publish_committed(self.snapshot.as_ref(), revision).await?;
        Ok(revision)
    }

    async fn list_accounts(
        &self,
        query: ProxyAccountListQuery,
    ) -> Result<ProxyAccountPage, AdminError> {
        if query.proxy_id.is_empty()
            || query.proxy_id.len() > 128
            || query.proxy_id.chars().any(char::is_control)
            || query.page == 0
            || query.search.len() > 256
            || query.search.chars().any(char::is_control)
        {
            return Err(AdminError::invalid("关联账号查询参数不合法"));
        }
        let mut page = self
            .store
            .list_accounts(query)
            .await
            .map_err(|error| map_store_error(error, "proxy"))?;
        page.items = stream::iter(page.items)
            .map(|mut account| async {
                // 与账号目录一致，缺失套餐时读取已有额度快照；限制并发且不触发上游刷新
                let mut cached_quota = None;
                if let Ok(kind) = ProviderKind::new(account.provider_kind.clone())
                    && let Ok(provider) = self.providers.require(&kind)
                    && let Ok(account_id) = ProviderAccountId::new(account.id.clone())
                {
                    if provider.supports_peer_connection(&account.authentication_kind)
                        && let Some(peer) = self.upstream_info.as_deref()
                    {
                        account.upstream = peer
                            .read_summary(&account_id)
                            .await
                            .ok()
                            .flatten()
                            .and_then(|quota| quota.upstream);
                    }
                    if explicit_plan_type(account.plan_type.as_deref()).is_none() {
                        cached_quota = provider
                            .quota(ProviderQuotaRequest {
                                account_id,
                                refresh: false,
                                rolling_usage: None,
                            })
                            .await
                            .ok();
                    }
                }
                account.plan_type_display = self.providers.resolve_account_plan(
                    &account.provider_kind,
                    &mut account.plan_type,
                    cached_quota.as_ref(),
                );
                account
            })
            .buffered(8)
            .collect()
            .await;
        Ok(page)
    }

    async fn list(&self, query: ProxyListQuery) -> Result<ProxyPage, AdminError> {
        list_proxies(self.store.as_ref(), query).await
    }

    async fn create(
        &self,
        mut command: NewProxy,
        context: &MutationContext,
    ) -> Result<ProxyMutation, AdminError> {
        command.name = validate_name(&command.name)?;
        command.location = command
            .location
            .map(|location| location.normalized())
            .transpose()
            .map_err(|_| AdminError::invalid("代理位置不合法"))?;
        command.test = if command.auto_location {
            Some(self.probe(ProbeRoute::Proxy(&command.proxy), true).await?)
        } else {
            None
        };
        let result = self
            .store
            .create(command, context)
            .await
            .map_err(|error| map_store_error(error, "proxy"))?;
        publish_committed(self.snapshot.as_ref(), result.config_revision).await?;
        Ok(result)
    }

    async fn update(
        &self,
        mut command: UpdateProxy,
        context: &MutationContext,
    ) -> Result<ProxyMutation, AdminError> {
        command.name = validate_name(&command.name)?;
        command.location = command
            .location
            .map(|location| location.map(|value| value.normalized()).transpose())
            .transpose()
            .map_err(|_| AdminError::invalid("代理位置不合法"))?;
        // 只在开启自动模式或连接地址变化时检测，普通编辑不刷新已识别位置
        command.test = None;
        let current = self
            .store
            .get(&command.id)
            .await
            .map_err(|e| map_store_error(e, "proxy"))?;
        if current.managed_ipv6.is_some()
            && (command.proxy.is_some()
                || command
                    .enabled
                    .is_some_and(|enabled| enabled != current.enabled))
        {
            return Err(AdminError::invalid(
                "独占 IPv6 代理不可修改出口或禁用，请解除绑定后退役",
            ));
        }
        if command.auto_location.is_some() || command.proxy.is_some() {
            let current = self
                .store
                .get(&command.id)
                .await
                .map_err(|error| map_store_error(error, "proxy"))?;
            if current.revision != command.revision {
                return Err(AdminError::conflict("代理已被修改，请刷新后重试"));
            }
            let proxy = command.proxy.as_ref().unwrap_or(&current.proxy);
            if command.auto_location.unwrap_or(current.auto_location)
                && (!current.auto_location || proxy != &current.proxy)
            {
                command.test = Some(self.probe(ProbeRoute::Proxy(proxy), true).await?);
            }
        }
        let result = self
            .store
            .update(command, context)
            .await
            .map_err(|error| map_store_error(error, "proxy"))?;
        publish_committed(self.snapshot.as_ref(), result.config_revision).await?;
        Ok(result)
    }

    async fn delete(
        &self,
        id: &str,
        revision: Revision,
        context: &MutationContext,
    ) -> Result<Revision, AdminError> {
        if self
            .store
            .get(id)
            .await
            .map_err(|e| map_store_error(e, "proxy"))?
            .managed_ipv6
            .is_some()
        {
            return Err(AdminError::conflict("请在 IPv6 出口中退役此代理"));
        }
        let result = self
            .store
            .delete(id, revision, context)
            .await
            .map_err(|error| map_store_error(error, "proxy"))?;
        publish_committed(self.snapshot.as_ref(), result).await?;
        Ok(result)
    }

    async fn probe(
        &self,
        proxy: ProbeRoute<'_>,
        detect_location: bool,
    ) -> Result<ProxyTestResult, AdminError> {
        let _permit = self
            .test_slots
            .try_acquire()
            .map_err(|_| AdminError::new(AdminErrorKind::RateLimited, "代理测试已达并发上限"))?;
        Ok(self.probe.test(proxy, detect_location).await)
    }

    async fn test(
        &self,
        id: &str,
        revision: Revision,
        detect_location: bool,
        context: &MutationContext,
    ) -> Result<ProxyRecord, AdminError> {
        let _permit = self
            .test_slots
            .try_acquire()
            .map_err(|_| AdminError::new(AdminErrorKind::RateLimited, "代理测试已达并发上限"))?;
        let record = self
            .store
            .get(id)
            .await
            .map_err(|error| map_store_error(error, "proxy"))?;
        if record.revision != revision {
            return Err(AdminError::conflict("代理已被修改，请刷新后重新测试"));
        }
        // 手动解析只为本次测试请求位置，不改变自动跟随的持久配置
        let result = self
            .probe
            .test(
                ProbeRoute::Proxy(&record.proxy),
                record.auto_location || detect_location,
            )
            .await;
        let mutation = self
            .store
            .record_test(id, revision, result, context)
            .await
            .map_err(|error| map_store_error(error, "proxy"))?;
        if record.effective_location() != mutation.record.effective_location() {
            publish_committed(self.snapshot.as_ref(), mutation.config_revision).await?;
        }
        Ok(mutation.record)
    }
}

fn validate_name(value: &str) -> Result<String, AdminError> {
    let value = value.trim();
    if value.is_empty() || value.chars().count() > 100 || value.chars().any(char::is_control) {
        return Err(AdminError::invalid("代理名称需要 1 至 100 个字符"));
    }
    Ok(value.to_owned())
}

fn validate_batch_size(size: usize) -> Result<(), AdminError> {
    if size == 0 || size > MAX_PROXY_BATCH_SIZE {
        return Err(AdminError::invalid("一次需要提交 1 至 100 条代理"));
    }
    Ok(())
}

fn batch_should_stop(error: &AdminError) -> bool {
    matches!(
        error.kind(),
        AdminErrorKind::Unauthorized
            | AdminErrorKind::Forbidden
            | AdminErrorKind::BadGateway
            | AdminErrorKind::UpstreamResultUnknown
            | AdminErrorKind::Unavailable
            | AdminErrorKind::Internal
    )
}
