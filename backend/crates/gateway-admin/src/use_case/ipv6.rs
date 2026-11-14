//! IPv6 分配恢复与退役编排，只对已持久化租约重试幂等操作

use super::{proxies::DefaultProxiesService, publish_committed};
use crate::{
    model::{AdminError, AdminErrorKind, MutationActor, MutationContext, ipv6::Ipv6LeaseState},
    ports::{
        ipv6::{Ipv6Controller, Ipv6ProxyStore},
        store::{AdminStoreError, AdminStoreErrorKind},
    },
};
use futures::StreamExt as _;
use gateway_core::task::{
    ScheduledTask, WorkerContribution, WorkerCycleContext, WorkerId, WorkerKind,
    WorkerRegistration, WorkerRunnable, WorkerSchedule, WorkerTaskError,
};
use std::{sync::Arc, time::Duration};

pub(super) fn store_error(error: AdminStoreError) -> AdminError {
    if error.kind() == AdminStoreErrorKind::Conflict {
        AdminError::conflict("IPv6 任务正被处理、账号已有代理或租约仍被占用，请刷新核对")
    } else {
        super::map_store_error(error, "IPv6 lease")
    }
}

impl DefaultProxiesService {
    pub(super) fn ipv6_ports(
        &self,
    ) -> Result<(&dyn Ipv6ProxyStore, &dyn Ipv6Controller), AdminError> {
        Ok((
            self.ipv6_store
                .as_deref()
                .ok_or_else(|| AdminError::unavailable("IPv6 存储不可用"))?,
            self.ipv6_controller
                .as_deref()
                .ok_or_else(|| AdminError::unavailable("IPv6 分配服务尚未配置"))?,
        ))
    }

    pub(super) async fn reconcile_ipv6_locked(
        &self,
        id: &str,
        context: &MutationContext,
    ) -> Result<(), AdminError> {
        let (store, controller) = self.ipv6_ports()?;
        let mut lease = store.get(id).await.map_err(store_error)?;
        if lease.view.controller_id != controller.id() {
            return Err(AdminError::conflict("IPv6 租约所属地址池与当前配置不一致"));
        }
        if lease.view.state == Ipv6LeaseState::Allocating {
            // 已有进程拿到此会话锁，说明上次创建调用已失去所有者
            store.mark_unknown(id).await.map_err(store_error)?;
            return Ok(());
        }
        if matches!(
            lease.view.state,
            Ipv6LeaseState::Queued | Ipv6LeaseState::Unknown
        ) {
            store.start_allocation(id).await.map_err(store_error)?;
            let remote = match controller.allocate().await {
                Ok(remote) => remote,
                Err(error) => {
                    store.mark_unknown(id).await.map_err(store_error)?;
                    return Err(error);
                }
            };
            // 创建成功未能持久化时不能激活，代理端会在十五分钟后回收 pending 租约
            store.save_remote(id, remote).await.map_err(store_error)?;
            lease = store.get(id).await.map_err(store_error)?;
        }
        if lease.view.state == Ipv6LeaseState::Pending {
            let remote = lease
                .remote
                .as_ref()
                .ok_or_else(|| AdminError::conflict("IPv6 租约尚未保存"))?;
            if let Err(error) = controller.activate(remote).await {
                if error.kind() == AdminErrorKind::Conflict {
                    store
                        .begin_retire(id, false, context)
                        .await
                        .map_err(store_error)?;
                }
                return Err(error);
            }
            match store.complete(id, context).await {
                Ok(revision) => publish_committed(self.snapshot.as_ref(), revision).await?,
                Err(error) if error.kind() == AdminStoreErrorKind::Conflict => {
                    if store.get(id).await.map_err(store_error)?.view.state
                        == Ipv6LeaseState::Active
                    {
                        return Ok(());
                    }
                    // 目标账号被删除或已手动换出口，保留租约供退役任务完成清理
                    store
                        .begin_retire(id, false, context)
                        .await
                        .map_err(store_error)?;
                    return Err(store_error(error));
                }
                // 提交结果未知时只重读本地状态，不撤销可能已经绑定成功的出口
                Err(error) => return Err(store_error(error)),
            }
        } else if lease.view.state == Ipv6LeaseState::Retiring {
            if let Some(remote) = &lease.remote {
                controller.retire(remote).await?;
            }
            let revision = store
                .finish_retire(id, context)
                .await
                .map_err(store_error)?;
            publish_committed(self.snapshot.as_ref(), revision).await?;
        }
        Ok(())
    }
}

pub(crate) struct Ipv6Task(pub(crate) Arc<DefaultProxiesService>);

impl Ipv6Task {
    pub(crate) fn into_worker(self) -> Result<WorkerContribution, AdminError> {
        let id = WorkerId::try_new(WorkerKind::Ipv6Egress, "admin")
            .map_err(|_| AdminError::internal("IPv6 Worker ID 不合法"))?;
        let schedule = WorkerSchedule::try_new(
            Duration::from_secs(2),
            Duration::from_secs(5),
            Duration::from_secs(60),
            Duration::from_secs(90),
            Duration::from_secs(30),
        )
        .map_err(|_| AdminError::internal("IPv6 Worker 调度配置不合法"))?;
        let registration = WorkerRegistration::try_new(
            id,
            WorkerRunnable::Scheduled {
                schedule,
                lease: None,
                task: Box::new(self),
            },
        )
        .map_err(|_| AdminError::internal("IPv6 Worker 注册失败"))?;
        Ok(WorkerContribution::Registration(registration))
    }
}

impl ScheduledTask for Ipv6Task {
    fn run_cycle(
        &self,
        context: WorkerCycleContext,
    ) -> futures::future::BoxFuture<'_, Result<(), WorkerTaskError>> {
        Box::pin(async move {
            let Some(controller) = &self.0.ipv6_controller else {
                return Ok(());
            };
            let Some(store) = &self.0.ipv6_store else {
                return Ok(());
            };
            let work = async {
                let ids = store
                    .pending(controller.id())
                    .await
                    .map_err(|_| WorkerTaskError::safe("IPv6 待处理记录读取失败"))?;
                let results = futures::stream::iter(ids)
                    .map(|id| async move {
                        let _guard = match store.lock(&id).await {
                            Ok(guard) => guard,
                            Err(error) if error.kind() == AdminStoreErrorKind::Conflict => {
                                return Ok(());
                            }
                            Err(_) => return Err(WorkerTaskError::safe("IPv6 任务锁获取失败")),
                        };
                        let mutation = MutationContext {
                            actor: MutationActor::System,
                            request_id: uuid::Uuid::now_v7().to_string(),
                        };
                        if let Err(error) = self.0.reconcile_ipv6_locked(&id, &mutation).await {
                            store
                                .postpone(&id)
                                .await
                                .map_err(|_| WorkerTaskError::safe("IPv6 重试状态保存失败"))?;
                            tracing::warn!(
                                lease_request_id = id,
                                error_kind = error.kind().as_str(),
                                "IPv6 分配或退役等待恢复"
                            );
                        }
                        Ok(())
                    })
                    .buffer_unordered(2)
                    .collect::<Vec<_>>()
                    .await;
                for result in results {
                    result?;
                }
                Ok(())
            };
            tokio::select! {
                () = context.cancellation().cancelled() => Ok(()),
                result = work => result,
            }
        })
    }
}
