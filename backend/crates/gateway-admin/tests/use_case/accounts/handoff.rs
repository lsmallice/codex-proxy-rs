//! 账号目录的交接容量投影、刷新与观测失败边界

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
        _: AccountHandoff,
        _: u64,
        _: u64,
    ) -> BoxFuture<'_, Result<bool, ProviderStoreError>> {
        unreachable!()
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
