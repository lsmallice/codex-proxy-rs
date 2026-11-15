//! 账号交接容量投影、终态收尾、暂态恢复与观测失败边界

use super::*;
use gateway_core::{
    account::{
        AccountHandoff, AccountHandoffPage, AccountHandoffPhase, AccountHandoffPolicy,
        AccountHandoffSnapshot, StartAccountHandoff,
    },
    provider_ports::{AccountHandoffStore, ProviderStoreError, ProviderStoreErrorKind},
};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

struct HandoffProjectionStore {
    snapshots: Mutex<Vec<AccountHandoffSnapshot>>,
    reads: AtomicUsize,
    fail: AtomicBool,
}

impl AccountHandoffStore for HandoffProjectionStore {
    fn list(&self) -> BoxFuture<'_, Result<Vec<AccountHandoffSnapshot>, ProviderStoreError>> {
        self.reads.fetch_add(1, Ordering::SeqCst);
        Box::pin(async {
            if self.fail.load(Ordering::SeqCst) {
                return Err(ProviderStoreError::new(
                    ProviderStoreErrorKind::Unavailable,
                    "handoff projection",
                ));
            }
            Ok(self.snapshots.lock().unwrap().clone())
        })
    }

    fn history(
        &self,
        _: u32,
        _: u32,
    ) -> BoxFuture<'_, Result<AccountHandoffPage, ProviderStoreError>> {
        unreachable!()
    }
    fn policy(&self) -> BoxFuture<'_, Result<AccountHandoffPolicy, ProviderStoreError>> {
        unreachable!()
    }
    fn save_policy(
        &self,
        _: AccountHandoffPolicy,
        _: u64,
    ) -> BoxFuture<'_, Result<bool, ProviderStoreError>> {
        unreachable!()
    }
    fn get(
        &self,
        _: String,
    ) -> BoxFuture<'_, Result<Option<AccountHandoffSnapshot>, ProviderStoreError>> {
        unreachable!()
    }
    fn start(
        &self,
        _: AccountHandoff,
    ) -> BoxFuture<'_, Result<AccountHandoffSnapshot, ProviderStoreError>> {
        unreachable!()
    }
    fn update(
        &self,
        mut state: AccountHandoff,
        expected_revision: u64,
        expected_failures: u64,
    ) -> BoxFuture<'_, Result<bool, ProviderStoreError>> {
        Box::pin(async move {
            let mut snapshots = self.snapshots.lock().unwrap();
            let Some(snapshot) = snapshots.iter_mut().find(|s| s.state.id == state.id) else {
                return Ok(false);
            };
            if snapshot.state.revision != expected_revision
                || snapshot.feedback.failures != expected_failures
            {
                return Ok(false);
            }
            state.revision += 1;
            snapshot.state = state;
            Ok(true)
        })
    }
    fn feedback(
        &self,
        _: String,
        _: bool,
        _: Option<u64>,
    ) -> BoxFuture<'_, Result<(), ProviderStoreError>> {
        unreachable!()
    }
}

async fn reconcile_fixture(
    old: Option<AccountRecord>,
    new: Option<AccountRecord>,
) -> (
    AdminServices,
    Arc<FakeAccountStore>,
    Arc<HandoffProjectionStore>,
) {
    let accounts = FakeAccountStore::with_account(account_record("openai"), events());
    accounts.set_accounts(old.into_iter().chain(new).collect());
    let mut state = AccountHandoff::start(
        StartAccountHandoff {
            request_id: "reconcile".into(),
            old_account_id: "acct_old".into(),
            new_account_id: "acct_new".into(),
            config: Default::default(),
        },
        ["Old".into(), "New".into()],
        [150, 0],
        None,
        1000,
    )
    .unwrap();
    state.phase = AccountHandoffPhase::Paused;
    let store = Arc::new(HandoffProjectionStore {
        snapshots: Mutex::new(vec![AccountHandoffSnapshot {
            state,
            feedback: Default::default(),
        }]),
        reads: AtomicUsize::new(0),
        fail: AtomicBool::new(false),
    });
    let services = super::super::AdminHarness::new()
        .accounts(accounts.clone())
        .account_runtime(Arc::new(CapacityRuntime {
            counts: Some(BTreeMap::from([
                ("acct_old".into(), 3),
                ("acct_new".into(), 2),
            ])),
            fail: false,
            reads: Mutex::default(),
        }))
        .handoffs(store.clone())
        .settings(Arc::new(StaticSettingsStore::default()))
        .provider(FakeProviderAdmin::new("openai", events()))
        .build()
        .await;
    (services, accounts, store)
}

fn named_account(id: &str) -> AccountRecord {
    let mut account = account_record("openai");
    account.id = id.into();
    account
}

#[tokio::test]
async fn reconcile_exits_terminal_accounts_without_waiting_for_quota_refresh() {
    for old_terminal in [true, false] {
        for reason in [
            "凭据已过期",
            "凭据已失效",
            "已封禁",
            "已停用",
            "额度已耗尽",
            "已删除",
        ] {
            let mut old = Some(named_account("acct_old"));
            let mut new = Some(named_account("acct_new"));
            let target = if old_terminal { &mut old } else { &mut new };
            let account = target.as_mut().unwrap();
            // 模拟上游提前撤销令牌，本地 AT 时间仍在未来且额度快照仍可用
            account.next_refresh_at = None;
            match reason {
                "凭据已过期" => {
                    account.credential_state = CredentialState::Expired;
                    account.last_error_reason =
                        Some(gateway_core::account::AccountErrorReason::AccessTokenExpired);
                }
                "凭据已失效" => account.credential_state = CredentialState::Invalid,
                "已封禁" => account.credential_state = CredentialState::Banned,
                "已停用" => account.enabled = false,
                "额度已耗尽" => {
                    account.quota = QuotaState::exhausted(
                        QuotaEvidence::UsageLimitReached,
                        Utc::now().into(),
                        None,
                    )
                }
                "已删除" => *target = None,
                _ => unreachable!(),
            }
            let (services, accounts, store) = reconcile_fixture(old.clone(), new.clone()).await;
            services.accounts().reconcile_handoffs().await.unwrap();
            let snapshot = store.snapshots.lock().unwrap()[0].clone();
            assert_eq!(
                snapshot.state.phase,
                if old_terminal {
                    AccountHandoffPhase::Completed
                } else {
                    AccountHandoffPhase::Cancelled
                }
            );
            assert!(snapshot.state.message.starts_with(if old_terminal {
                "老账号"
            } else {
                "新账号"
            }));
            assert!(snapshot.state.message.contains(reason));
            assert_eq!(
                (snapshot.state.old_in_flight, snapshot.state.new_in_flight),
                (3, 2)
            );
            assert!(
                services
                    .accounts()
                    .list(query())
                    .await
                    .unwrap()
                    .items
                    .iter()
                    .all(|a| a.handoff_limit.is_none())
            );
            for expected in old.into_iter().chain(new) {
                let actual = accounts
                    .load_account(&expected.id, Default::default())
                    .await
                    .unwrap()
                    .unwrap()
                    .account;
                assert_eq!(actual, expected);
            }
        }
    }
}

#[tokio::test]
async fn reconcile_keeps_temporary_account_states_paused_and_resumes_after_native_recovery() {
    for old_unavailable in [true, false] {
        for reason in ["凭据待验证", "访问令牌到期", "达到额度停调阈值"] {
            let mut old = named_account("acct_old");
            let mut new = named_account("acct_new");
            let target = if old_unavailable { &mut old } else { &mut new };
            match reason {
                "凭据待验证" => {
                    target.credential_state = CredentialState::Unknown;
                    target.quota = QuotaState::unknown();
                }
                "访问令牌到期" => {
                    target.access_token_expires_at = Some(Utc::now() - TimeDelta::minutes(1))
                }
                "达到额度停调阈值" => target.quota_paused = true,
                _ => unreachable!(),
            }
            let (services, accounts, store) = reconcile_fixture(Some(old), Some(new)).await;
            services.accounts().reconcile_handoffs().await.unwrap();
            let paused = store.snapshots.lock().unwrap()[0].clone();
            assert_eq!(paused.state.phase, AccountHandoffPhase::Paused);
            assert!(paused.state.message.starts_with(if old_unavailable {
                "老账号"
            } else {
                "新账号"
            }));
            assert!(paused.state.message.contains(reason));
            accounts.set_accounts(vec![named_account("acct_old"), named_account("acct_new")]);
            services.accounts().reconcile_handoffs().await.unwrap();
            let recovered = store.snapshots.lock().unwrap()[0].clone();
            assert_eq!(recovered.state.phase, AccountHandoffPhase::Observing);
            assert!(recovered.state.stable_since_ms.is_some());
            assert_eq!(recovered.state.old_limit, paused.state.old_limit);
        }
    }
}

#[tokio::test]
async fn reconcile_failed_observation_does_not_end_or_rewrite_handoffs() {
    let (services, _, store) = reconcile_fixture(
        Some(named_account("acct_old")),
        Some(named_account("acct_new")),
    )
    .await;
    let before = store.snapshots.lock().unwrap().clone();
    store.fail.store(true, Ordering::SeqCst);
    assert!(services.accounts().reconcile_handoffs().await.is_err());
    assert_eq!(*store.snapshots.lock().unwrap(), before);
}

fn query() -> AccountListQuery {
    AccountListQuery {
        page: 1,
        page_size: gateway_admin::model::PageSize::new(20).unwrap(),
        provider_kind: None,
        group_filter: None,
        search: None,
        status: None,
        sort: None,
    }
}

#[tokio::test]
async fn handoff_capacity_is_scoped_to_old_account_and_preserves_configured_limits() {
    for configured in [None, Some(30), Some(100)] {
        let mut old = account_record("openai");
        old.concurrency_limit =
            configured.and_then(gateway_core::account::AccountConcurrencyLimit::new);
        let mut new = old.clone();
        new.id = "acct_new".into();
        let mut unrelated = old.clone();
        unrelated.id = "acct_unrelated".into();
        let accounts = FakeAccountStore::with_account(old.clone(), events());
        accounts.set_accounts(vec![old.clone(), new, unrelated]);
        let state = AccountHandoff::start(
            StartAccountHandoff {
                request_id: "handoff_projection".into(),
                old_account_id: old.id.clone(),
                new_account_id: "acct_new".into(),
                config: Default::default(),
            },
            ["Old".into(), "New".into()],
            [100, 0],
            None,
            1000,
        )
        .unwrap();
        let store = Arc::new(HandoffProjectionStore {
            snapshots: Mutex::new(vec![AccountHandoffSnapshot {
                state,
                feedback: Default::default(),
            }]),
            reads: AtomicUsize::new(0),
            fail: AtomicBool::new(false),
        });
        let services = super::super::AdminHarness::new()
            .accounts(accounts.clone())
            .account_runtime(Arc::new(CapacityRuntime {
                counts: Some(BTreeMap::from([(old.id.clone(), 50)])),
                fail: false,
                reads: Mutex::default(),
            }))
            .handoffs(store.clone())
            .settings(Arc::new(StaticSettingsStore::default()))
            .provider(FakeProviderAdmin::new("openai", events()))
            .build()
            .await;
        for phase in [
            AccountHandoffPhase::Observing,
            AccountHandoffPhase::Paused,
            AccountHandoffPhase::Retaining,
            AccountHandoffPhase::Completed,
            AccountHandoffPhase::Cancelled,
        ] {
            store.snapshots.lock().unwrap()[0].state.phase = phase;
            let reads = store.reads.load(Ordering::SeqCst);
            let page = services.accounts().list(query()).await.unwrap();
            assert_eq!(store.reads.load(Ordering::SeqCst), reads + 1);
            for item in &page.items {
                assert_eq!(item.capacity.total_slots, configured.map(u64::from));
                assert_eq!(item.account.concurrency_limit, old.concurrency_limit);
                let expected = (phase.active() && item.account.id == old.id)
                    .then_some(configured.map_or(50, |n| u64::from(n.min(50))));
                assert_eq!(item.handoff_limit, expected);
            }
            let detail = services
                .accounts()
                .quota(&ProviderAccountId::new(old.id.clone()).unwrap(), false)
                .await
                .unwrap();
            assert_eq!(
                detail.handoff_limit,
                page.items
                    .iter()
                    .find(|i| i.account.id == old.id)
                    .unwrap()
                    .handoff_limit
            );
        }
        store.snapshots.lock().unwrap()[0].state.phase = AccountHandoffPhase::Retaining;
        store.fail.store(true, Ordering::SeqCst);
        let page = services.accounts().list(query()).await.unwrap();
        assert!(page.items.iter().all(|i| i.handoff_limit.is_none()));
        assert_eq!(
            page.items
                .iter()
                .find(|i| i.account.id == old.id)
                .unwrap()
                .capacity
                .used_slots,
            Some(50)
        );
        store.fail.store(false, Ordering::SeqCst);
        store.snapshots.lock().unwrap().clear();
        assert!(
            services
                .accounts()
                .list(query())
                .await
                .unwrap()
                .items
                .iter()
                .all(|i| i.handoff_limit.is_none())
        );
        let reads = store.reads.load(Ordering::SeqCst);
        accounts.set_accounts(Vec::new());
        assert!(
            services
                .accounts()
                .list(query())
                .await
                .unwrap()
                .items
                .is_empty()
        );
        assert_eq!(store.reads.load(Ordering::SeqCst), reads);
    }
}
