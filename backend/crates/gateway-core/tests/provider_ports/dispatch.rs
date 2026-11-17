//! 所有账号发送等待的开关、取消、队满、预算与失败边界
use futures::{executor::block_on, future::BoxFuture};
use futures_timer::Delay;
use gateway_core::{
    account::{AccountTrafficPolicy, AccountTrafficRuntime, ProviderAccountId},
    concurrency::ConcurrencyWaitBudget,
    diagnostics::TraceContext,
    error::ProviderErrorKind,
    lifecycle::{CancellationToken, Deadline},
    provider_ports::{
        AccountDispatchLimiter, AccountDispatchStore, ProviderStoreError, ProviderStoreErrorKind,
    },
};
use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::{Duration, Instant, SystemTime},
};

#[derive(Default)]
struct Store {
    blocked: AtomicBool,
    failed: AtomicBool,
    calls: AtomicUsize,
    granted: AtomicUsize,
}
impl AccountDispatchStore for Store {
    fn try_start<'a>(
        &'a self,
        _: &'a ProviderAccountId,
        _: AccountTrafficPolicy,
    ) -> BoxFuture<'a, Result<Duration, ProviderStoreError>> {
        Box::pin(async move {
            self.calls.fetch_add(1, Ordering::SeqCst);
            if self.failed.load(Ordering::SeqCst) {
                return Err(ProviderStoreError::new(
                    ProviderStoreErrorKind::Unavailable,
                    "fixture",
                ));
            }
            if self.blocked.load(Ordering::SeqCst) {
                return Ok(Duration::from_millis(10));
            }
            self.granted.fetch_add(1, Ordering::SeqCst);
            Ok(Duration::ZERO)
        })
    }
}
fn setup() -> (
    Arc<Store>,
    Arc<AccountTrafficRuntime>,
    Arc<AccountDispatchLimiter>,
) {
    let store = Arc::new(Store::default());
    let runtime = Arc::new(AccountTrafficRuntime::default());
    let limiter = Arc::new(AccountDispatchLimiter::new(store.clone(), runtime.clone()));
    (store, runtime, limiter)
}
fn gate(
    limiter: &Arc<AccountDispatchLimiter>,
    cancel: CancellationToken,
    budget: ConcurrencyWaitBudget,
) -> gateway_core::provider_ports::AccountDispatchGate {
    limiter.gate(
        ProviderAccountId::new("acct_existing").unwrap(),
        Deadline::from(SystemTime::now() + Duration::from_secs(10)),
        cancel,
        budget,
        TraceContext::default(),
    )
}
fn enable(runtime: &AccountTrafficRuntime, max_waiting: u32) {
    runtime.publish(AccountTrafficPolicy {
        enabled: true,
        max_wait_seconds: 1,
        max_waiting,
        ..Default::default()
    });
}

#[test]
fn traffic_policy_validates_positive_bounded_values() {
    let base = AccountTrafficPolicy::default();
    assert!(!base.enabled);
    assert_eq!((base.burst, base.requests_per_second), (30, 20));
    for value in [
        AccountTrafficPolicy { burst: 0, ..base },
        AccountTrafficPolicy {
            requests_per_second: 0,
            ..base
        },
        AccountTrafficPolicy {
            requests_per_second: 1001,
            ..base
        },
        AccountTrafficPolicy {
            max_wait_seconds: 31,
            ..base
        },
        AccountTrafficPolicy {
            max_waiting: 1025,
            ..base
        },
    ] {
        assert!(value.validate().is_err());
    }
}
#[test]
fn disabled_gate_does_not_access_store_and_enabled_errors_fail_closed() {
    block_on(async {
        let (store, runtime, limiter) = setup();
        store.failed.store(true, Ordering::SeqCst);
        gate(&limiter, Default::default(), Default::default())
            .wait()
            .await
            .unwrap();
        assert_eq!(store.calls.load(Ordering::SeqCst), 0);
        enable(&runtime, 10);
        let error = gate(&limiter, Default::default(), Default::default())
            .wait()
            .await
            .unwrap_err();
        assert_eq!(error.kind(), ProviderErrorKind::AccountCapacityUnavailable);
        assert_eq!(store.granted.load(Ordering::SeqCst), 0);
    });
}
#[test]
fn cancelled_waiter_releases_position_and_never_sends_later() {
    block_on(async {
        let (store, runtime, limiter) = setup();
        enable(&runtime, 1);
        store.blocked.store(true, Ordering::SeqCst);
        let cancel = CancellationToken::new();
        let first = gate(&limiter, cancel.clone(), Default::default());
        let task = std::thread::spawn(move || block_on(first.wait()));
        Delay::new(Duration::from_millis(30)).await;
        let error = gate(&limiter, Default::default(), Default::default())
            .wait()
            .await
            .unwrap_err();
        assert_eq!(error.kind(), ProviderErrorKind::ConcurrencyQueueFull);
        cancel.cancel();
        assert_eq!(
            task.join().unwrap().unwrap_err().kind(),
            ProviderErrorKind::Cancelled
        );
        store.blocked.store(false, Ordering::SeqCst);
        gate(&limiter, Default::default(), Default::default())
            .wait()
            .await
            .unwrap();
        Delay::new(Duration::from_millis(30)).await;
        assert_eq!(store.granted.load(Ordering::SeqCst), 1);
    });
}
#[test]
fn timeout_budget_survives_new_gate_and_hot_disable_resumes_waiters() {
    block_on(async {
        let (store, runtime, limiter) = setup();
        enable(&runtime, 10);
        store.blocked.store(true, Ordering::SeqCst);
        let budget = ConcurrencyWaitBudget::default();
        let error = gate(&limiter, Default::default(), budget.clone())
            .wait()
            .await
            .unwrap_err();
        assert_eq!(error.kind(), ProviderErrorKind::ConcurrencyQueueTimeout);
        let start = Instant::now();
        assert!(
            gate(&limiter, Default::default(), budget)
                .wait()
                .await
                .is_err()
        );
        assert!(start.elapsed() < Duration::from_millis(200));
        let pending = gate(&limiter, Default::default(), Default::default());
        let task = std::thread::spawn(move || block_on(pending.wait()));
        Delay::new(Duration::from_millis(30)).await;
        runtime.publish(Default::default());
        task.join().unwrap().unwrap();
        assert_eq!(store.granted.load(Ordering::SeqCst), 0);
    });
}
#[test]
fn repeated_sends_use_one_account_bucket_instead_of_one_per_connection() {
    block_on(async {
        let (store, runtime, limiter) = setup();
        enable(&runtime, 10);
        let gate = gate(&limiter, Default::default(), Default::default());
        for _ in 0..4 {
            gate.wait().await.unwrap();
        }
        assert_eq!(store.granted.load(Ordering::SeqCst), 4);
    });
}
