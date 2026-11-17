//! 账号发送配额端口与可取消的有界等待，不持久化或重放请求
use super::{ProviderStoreError, ProviderStoreErrorKind};
use crate::{
    account::{AccountTrafficPolicy, AccountTrafficRuntime, ProviderAccountId},
    concurrency::{ConcurrencyWaitBudget, ConcurrencyWaitQueue, QueueRejection},
    diagnostics::TraceContext,
    error::{ProviderError, ProviderErrorKind},
    lifecycle::{CancellationToken, Deadline},
    upstream::UpstreamSendState,
};
use futures::{FutureExt, future::BoxFuture, pin_mut, select_biased};
use futures_timer::Delay;
use std::{
    sync::Arc,
    time::{Duration, Instant},
};

pub trait AccountDispatchStore: Send + Sync {
    fn ready<'a>(
        &'a self,
        _account: &'a ProviderAccountId,
        _policy: AccountTrafficPolicy,
    ) -> BoxFuture<'a, Result<bool, ProviderStoreError>> {
        Box::pin(async { Ok(true) })
    }
    /// 返回零表示获得一次发送资格，非零表示未消费配额及可重试时间
    fn try_start<'a>(
        &'a self,
        account: &'a ProviderAccountId,
        policy: AccountTrafficPolicy,
    ) -> BoxFuture<'a, Result<Duration, ProviderStoreError>>;
}

pub struct AccountDispatchLimiter {
    store: Arc<dyn AccountDispatchStore>,
    runtime: Arc<AccountTrafficRuntime>,
    waiting: ConcurrencyWaitQueue<ProviderAccountId>,
}
impl AccountDispatchLimiter {
    /// 选号只读配额，真实发送仍须原子消费，不能将该观测当作发送许可
    pub async fn ready(&self, account: &ProviderAccountId) -> Result<bool, ProviderStoreError> {
        let policy = self.runtime.snapshot();
        if !policy.enabled {
            return Ok(true);
        }
        self.store.ready(account, policy).await
    }
    pub fn new(store: Arc<dyn AccountDispatchStore>, runtime: Arc<AccountTrafficRuntime>) -> Self {
        Self {
            store,
            runtime,
            waiting: ConcurrencyWaitQueue::default(),
        }
    }
    pub fn gate(
        self: &Arc<Self>,
        account: ProviderAccountId,
        deadline: Deadline,
        cancellation: CancellationToken,
        budget: ConcurrencyWaitBudget,
        trace: TraceContext,
    ) -> AccountDispatchGate {
        AccountDispatchGate {
            limiter: self.clone(),
            account,
            deadline,
            cancellation,
            budget,
            trace,
        }
    }
}

#[derive(Clone)]
pub struct AccountDispatchGate {
    limiter: Arc<AccountDispatchLimiter>,
    account: ProviderAccountId,
    deadline: Deadline,
    cancellation: CancellationToken,
    budget: ConcurrencyWaitBudget,
    trace: TraceContext,
}
impl AccountDispatchGate {
    pub async fn wait(&self) -> Result<(), ProviderError> {
        let policy = self.limiter.runtime.snapshot();
        if !policy.enabled {
            return Ok(());
        }
        policy
            .validate()
            .map_err(|_| failure(ProviderErrorKind::Protocol))?;
        if self.cancellation.is_cancelled() {
            return Err(failure(ProviderErrorKind::Cancelled));
        }
        let started = Instant::now();
        let mut waiting_deadline = None;
        let mut ticket: Option<crate::concurrency::WaitTicket<ProviderAccountId>> = None;
        let result = async {
            loop {
                let current = self.limiter.runtime.snapshot();
                if !current.enabled {
                    return Ok(());
                }
                if let Some(ticket) = &ticket {
                    ticket.turn().await.map_err(queue_error)?;
                }
                let current = self.limiter.runtime.snapshot();
                if !current.enabled {
                    return Ok(());
                }
                if self.cancellation.is_cancelled() {
                    return Err(failure(ProviderErrorKind::Cancelled));
                }
                if waiting_deadline.is_some_and(|at| Instant::now() >= at) {
                    return Err(queue_error(QueueRejection::Timeout));
                }
                if ticket.is_some() || !self.limiter.waiting.has_waiters(&self.account) {
                    let retry = self
                        .limiter
                        .store
                        .try_start(&self.account, current)
                        .await
                        .map_err(|_: ProviderStoreError| {
                            failure(ProviderErrorKind::AccountCapacityUnavailable)
                        })?;
                    if retry.is_zero() {
                        return Ok(());
                    }
                    if ticket.is_some() {
                        let remaining = waiting_deadline
                            .unwrap()
                            .saturating_duration_since(Instant::now());
                        Delay::new(retry.min(Duration::from_millis(100)).min(remaining)).await;
                        continue;
                    }
                }
                let at = self.budget.deadline(
                    Duration::from_secs(u64::from(policy.max_wait_seconds)),
                    self.deadline.at(),
                );
                waiting_deadline = Some(at);
                ticket = Some(
                    self.limiter
                        .waiting
                        .enqueue(std::slice::from_ref(&self.account), policy.max_waiting, at)
                        .map_err(queue_error)?,
                );
            }
        }
        .fuse();
        let cancelled = self.cancellation.cancelled().fuse();
        let timeout = Delay::new(
            self.deadline
                .bounded(Duration::from_secs(u64::from(policy.max_wait_seconds))),
        )
        .fuse();
        pin_mut!(result, cancelled, timeout);
        let outcome = select_biased! {
            () = cancelled => Err(failure(ProviderErrorKind::Cancelled)),
            () = timeout => Err(queue_error(QueueRejection::Timeout)),
            result = result => result,
        };
        self.trace.record("account.traffic_wait", serde_json::json!({"waitMs":started.elapsed().as_millis() as u64,"allowed":outcome.is_ok()}));
        outcome
    }
}
fn failure(kind: ProviderErrorKind) -> ProviderError {
    ProviderError::new(kind, UpstreamSendState::NotSent).with_retry_prohibited()
}
fn queue_error(error: QueueRejection) -> ProviderError {
    failure(error.provider_kind())
}

pub struct UnavailableDispatchStore;
impl AccountDispatchStore for UnavailableDispatchStore {
    fn try_start<'a>(
        &'a self,
        _: &'a ProviderAccountId,
        _: AccountTrafficPolicy,
    ) -> BoxFuture<'a, Result<Duration, ProviderStoreError>> {
        Box::pin(async {
            Err(ProviderStoreError::new(
                ProviderStoreErrorKind::Unavailable,
                "account dispatch unavailable",
            ))
        })
    }
}
