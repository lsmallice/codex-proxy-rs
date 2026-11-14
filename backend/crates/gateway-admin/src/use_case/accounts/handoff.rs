//! 账号交接的管理校验、五秒观测与后台生命周期，不改写账号配置或购买账号

use super::*;
use gateway_core::{
    account::*,
    provider_ports::{AccountHandoffStore, ProviderStoreError, ProviderStoreErrorKind},
    task::*,
};

fn store_error(error: ProviderStoreError) -> AdminError {
    match error.kind() {
        ProviderStoreErrorKind::Conflict => {
            AdminError::conflict("账号已参与其他交接或请求标识已被使用")
        }
        _ => AdminError::unavailable("账号交接状态读取或保存失败").with_source(error),
    }
}

impl DefaultAccountsService {
    pub(super) async fn handoff_load_policy(&self) -> Result<AccountHandoffPolicy, AdminError> {
        self.handoff_store()?.policy().await.map_err(store_error)
    }

    pub(super) async fn handoff_save_policy(
        &self,
        enabled: bool,
        config: AccountHandoffConfig,
        revision: u64,
        context: &MutationContext,
    ) -> Result<AccountHandoffPolicy, AdminError> {
        config.validate().map_err(AdminError::invalid)?;
        let current = self.handoff_load_policy().await?;
        if current.revision != revision {
            return Err(AdminError::conflict("交接设置已变化，请刷新后重试"));
        }
        let policy = AccountHandoffPolicy {
            enabled,
            config,
            revision: revision
                .checked_add(1)
                .ok_or_else(|| AdminError::invalid("交接设置版本超出范围"))?,
            enabled_after_ms: if enabled && !current.enabled {
                Utc::now().timestamp_millis() as u64
            } else {
                current.enabled_after_ms
            },
        };
        if !self
            .handoff_store()?
            .save_policy(policy.clone(), revision)
            .await
            .map_err(store_error)?
        {
            return Err(AdminError::conflict("交接设置已变化，请刷新后重试"));
        }
        tracing::info!(request_id = %context.request_id, enabled, "自动账号交接设置已保存");
        Ok(policy)
    }
    pub(crate) fn with_handoffs(mut self, handoffs: Option<Arc<dyn AccountHandoffStore>>) -> Self {
        self.handoffs = handoffs;
        self
    }

    fn handoff_store(&self) -> Result<&dyn AccountHandoffStore, AdminError> {
        self.handoffs
            .as_deref()
            .ok_or_else(|| AdminError::unavailable("账号交接服务不可用"))
    }

    pub(super) async fn handoff_list(&self) -> Result<Vec<AccountHandoffSnapshot>, AdminError> {
        self.handoff_store()?.list().await.map_err(store_error)
    }

    pub(super) async fn handoff_records_page(
        &self,
        history: bool,
        page: u32,
        page_size: u32,
    ) -> Result<AccountHandoffPage, AdminError> {
        validate_handoff_page(page, page_size).map_err(AdminError::invalid)?;
        let mut items = self.handoff_list().await?;
        let active_total = items.len() as u64;
        let occupied_account_ids = items
            .iter()
            .flat_map(|s| {
                [
                    s.state.old_account_id.clone(),
                    s.state.new_account_id.clone(),
                ]
            })
            .collect::<Vec<_>>();
        if history {
            let mut result = self
                .handoff_store()?
                .history(page, page_size)
                .await
                .map_err(store_error)?;
            result.active_total = active_total;
            result.occupied_account_ids = occupied_account_ids;
            return Ok(result);
        }
        items.sort_by(|a, b| {
            b.state
                .created_at_ms
                .cmp(&a.state.created_at_ms)
                .then_with(|| b.state.id.cmp(&a.state.id))
        });
        let total = items.len() as u64;
        let page = page.min(
            u32::try_from(total.div_ceil(u64::from(page_size)))
                .unwrap_or(u32::MAX)
                .max(1),
        );
        let start = ((page - 1) * page_size) as usize;
        Ok(AccountHandoffPage {
            items: items
                .into_iter()
                .skip(start)
                .take(page_size as usize)
                .collect(),
            total,
            page,
            page_size,
            active_total,
            occupied_account_ids,
        })
    }

    pub(super) async fn handoff_start(
        &self,
        command: StartAccountHandoff,
        context: &MutationContext,
    ) -> Result<AccountHandoffSnapshot, AdminError> {
        command.config.validate().map_err(AdminError::invalid)?;
        let id = uuid::Uuid::parse_str(&command.request_id)
            .map_err(|_| AdminError::invalid("交接请求标识不合法"))?
            .to_string();
        if let Some(existing) = self
            .handoff_store()?
            .get(id.clone())
            .await
            .map_err(store_error)?
        {
            if existing.state.old_account_id != command.old_account_id
                || existing.state.new_account_id != command.new_account_id
                || existing.state.config != command.config
            {
                return Err(AdminError::conflict("交接请求标识已被其他参数使用"));
            }
            return Ok(existing);
        }
        if command.old_account_id == command.new_account_id {
            return Err(AdminError::invalid("新老账号不能相同"));
        }
        let old_id = ProviderAccountId::new(command.old_account_id.clone())
            .map_err(|_| AdminError::invalid("老账号 ID 不合法"))?;
        let new_id = ProviderAccountId::new(command.new_account_id.clone())
            .map_err(|_| AdminError::invalid("新账号 ID 不合法"))?;
        let old = self.load_account(&old_id).await?;
        let new = self.load_account(&new_id).await?;
        if old.account.provider_kind.as_str() != "openai"
            || new.account.provider_kind != old.account.provider_kind
            || old.account.authentication_kind != "oauth"
            || new.account.authentication_kind != "oauth"
        {
            return Err(AdminError::invalid("账号交接仅支持 OpenAI OAuth 账号"));
        }
        if old.projection.status != AccountStatus::Normal
            || new.projection.status != AccountStatus::Normal
        {
            return Err(AdminError::invalid("新老账号必须处于正常状态"));
        }
        if !old
            .account
            .groups
            .iter()
            .any(|g| g.enabled && new.account.groups.iter().any(|n| n.enabled && n.id == g.id))
        {
            return Err(AdminError::invalid("新老账号必须属于同一启用分组"));
        }
        if new.account.plan_type.as_deref().is_none_or(|p| {
            p.is_empty() || p.eq_ignore_ascii_case("free") || p.eq_ignore_ascii_case("unknown")
        }) || new.account.credential_state != CredentialState::Ready
            || new.account.quota.access() != QuotaAccessState::Allowed
            || (new.account.outbound_proxy_id.is_some() && !new.account.outbound_proxy_enabled)
        {
            return Err(AdminError::invalid("新账号订阅或代理尚未就绪"));
        }
        let floor = command.config.minimum_old_concurrency;
        if old
            .capacity
            .total_slots
            .is_some_and(|n| n < u64::from(floor))
        {
            return Err(AdminError::invalid("老账号已有并发上限低于交接保留值"));
        }
        let old_used = old
            .capacity
            .used_slots
            .ok_or_else(|| AdminError::unavailable("老账号实时并发不可用"))?;
        let new_used = new
            .capacity
            .used_slots
            .ok_or_else(|| AdminError::unavailable("新账号实时并发不可用"))?;
        let now = Utc::now().timestamp_millis() as u64;
        let mut command = command;
        command.request_id = id;
        let state = AccountHandoff::start(
            command,
            [old.account.name, new.account.name],
            [
                u32::try_from(old_used).unwrap_or(u32::MAX),
                u32::try_from(new_used).unwrap_or(u32::MAX),
            ],
            old.capacity
                .total_slots
                .map(|n| u32::try_from(n).unwrap_or(u32::MAX)),
            now,
        )
        .map_err(AdminError::invalid)?;
        let result = self
            .handoff_store()?
            .start(state)
            .await
            .map_err(store_error)?;
        tracing::info!(request_id = %context.request_id, handoff_id = %result.state.id, "账号并发交接已启动");
        Ok(result)
    }

    pub(super) async fn handoff_cancel(
        &self,
        id: String,
        revision: u64,
        context: &MutationContext,
    ) -> Result<(), AdminError> {
        let Some(mut current) = self
            .handoff_list()
            .await?
            .into_iter()
            .find(|s| s.state.id == id)
        else {
            return Ok(());
        };
        if current.state.revision != revision {
            return Err(AdminError::conflict("交接状态已变化，请刷新后重试"));
        }
        current.state.phase = AccountHandoffPhase::Cancelled;
        current.state.message = "管理员取消交接，恢复账号原始并发设置".into();
        if !self
            .handoff_store()?
            .update(current.state, revision, current.feedback.failures)
            .await
            .map_err(store_error)?
        {
            return Err(AdminError::conflict("交接状态已变化，请刷新后重试"));
        }
        tracing::info!(request_id = %context.request_id, handoff_id = %id, "账号并发交接已取消");
        Ok(())
    }

    pub(super) async fn handoff_reconcile(&self) -> Result<(), AdminError> {
        let Some(store) = &self.handoffs else {
            return Ok(());
        };
        for mut item in store.list().await.map_err(store_error)? {
            let old_id = ProviderAccountId::new(item.state.old_account_id.clone())
                .map_err(|_| AdminError::internal("交接账号 ID 无效"))?;
            let new_id = ProviderAccountId::new(item.state.new_account_id.clone())
                .map_err(|_| AdminError::internal("交接账号 ID 无效"))?;
            let ids = vec![
                item.state.old_account_id.clone(),
                item.state.new_account_id.clone(),
            ];
            let runtime = self
                .account_runtime
                .account_runtime(&ids)
                .await
                .map_err(|e| map_store_error(e, "handoff runtime"))?;
            let in_flight = runtime
                .in_flight
                .as_ref()
                .ok_or_else(|| AdminError::unavailable("账号实时并发不可用"))?;
            let old_count = in_flight.get(&ids[0]).copied().unwrap_or(0);
            let new_count = in_flight.get(&ids[1]).copied().unwrap_or(0);
            let old = self
                .accounts
                .load_account(old_id.as_str(), runtime.clone())
                .await
                .map_err(|e| map_store_error(e, "handoff account"))?;
            let new = self
                .accounts
                .load_account(new_id.as_str(), runtime)
                .await
                .map_err(|e| map_store_error(e, "handoff account"))?;
            let old_finished = old.as_ref().is_none_or(|a| {
                !a.account.enabled
                    || a.account.quota.is_exhausted()
                    || matches!(
                        a.account.credential_state,
                        CredentialState::Invalid | CredentialState::Banned
                    )
            });
            let old_available = old
                .as_ref()
                .is_some_and(|a| a.projection.status == AccountStatus::Normal);
            let new_available = new
                .as_ref()
                .is_some_and(|a| a.projection.status == AccountStatus::Normal);
            let revision = item.state.revision;
            item.state.advance(
                AccountHandoffObservation {
                    now_ms: Utc::now().timestamp_millis() as u64,
                    old_available,
                    new_available,
                    old_finished,
                    old_in_flight: u32::try_from(old_count).unwrap_or(u32::MAX),
                    new_in_flight: u32::try_from(new_count).unwrap_or(u32::MAX),
                },
                item.feedback,
            );
            if item.state.phase == AccountHandoffPhase::Completed {
                item.state.message = match old.as_ref() {
                    None => "老账号已删除",
                    Some(a) if !a.account.enabled => "老账号已停用",
                    Some(a) if a.account.quota.is_exhausted() => "老账号额度已耗尽",
                    Some(_) => "老账号凭据已失效或账号已封禁",
                }
                .into();
            }
            store
                .update(item.state, revision, item.feedback.failures)
                .await
                .map_err(store_error)?;
        }
        Ok(())
    }
}

pub(crate) struct AccountHandoffTask(pub Arc<dyn AccountsService>);
impl AccountHandoffTask {
    pub(crate) fn into_worker(self) -> Result<WorkerContribution, AdminError> {
        let id = WorkerId::try_new(WorkerKind::AccountHandoff, "admin")
            .map_err(|_| AdminError::internal("账号交接 Worker ID 无效"))?;
        let schedule = WorkerSchedule::try_new(
            std::time::Duration::from_secs(5),
            std::time::Duration::from_secs(5),
            std::time::Duration::from_secs(30),
            std::time::Duration::from_secs(45),
            std::time::Duration::from_secs(10),
        )
        .map_err(|_| AdminError::internal("账号交接调度配置无效"))?;
        let lease = WorkerLeaseRequest::try_new(id.clone(), std::time::Duration::from_secs(45))
            .map_err(|_| AdminError::internal("账号交接租约无效"))?;
        let registration = WorkerRegistration::try_new(
            id,
            WorkerRunnable::Scheduled {
                schedule,
                lease: Some(lease),
                task: Box::new(self),
            },
        )
        .map_err(|_| AdminError::internal("账号交接 Worker 注册失败"))?;
        Ok(WorkerContribution::Registration(registration))
    }
}
impl ScheduledTask for AccountHandoffTask {
    fn run_cycle(
        &self,
        context: WorkerCycleContext,
    ) -> futures::future::BoxFuture<'_, Result<(), WorkerTaskError>> {
        Box::pin(async move {
            tokio::select! {
                () = context.cancellation().cancelled() => Ok(()),
                result = self.0.reconcile_handoffs() => result.map_err(|_| WorkerTaskError::safe("账号交接观测失败，保持当前档位")),
            }
        })
    }
}
