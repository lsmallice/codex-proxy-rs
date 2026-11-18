//! 分层选号保留健康会话，容量迁移稳定绑定且不改变固定续接

use super::*;
use gateway_core::account::{AccountConcurrency, AccountHandoffPolicy, AccountRuntimeSignals};
use gateway_core::account::{
    AccountHandoffConfig, AccountTransferGroup, AccountTransferSnapshot, TransferMember,
};

fn signal(in_flight: u32) -> AccountRuntimeSignals {
    AccountRuntimeSignals {
        in_flight,
        last_started_at: None,
        quota_reset_at: None,
        quota_reset_windows: None,
        quota_remaining_rank: None,
        cooldown: None,
        failure_rate_basis_points: None,
        first_output_latency_ms: None,
    }
}

fn pool_policy() -> AccountHandoffPolicy {
    AccountHandoffPolicy {
        enabled: true,
        config: AccountHandoffConfig {
            transfer_group: Some(Default::default()),
            new_account_protection: Some(Default::default()),
            ..Default::default()
        },
        ..Default::default()
    }
}

fn transfer_snapshot() -> AccountTransferSnapshot {
    let member = |id: &str, baseline, limit| TransferMember {
        id: id.into(),
        name: id.into(),
        baseline,
        limit,
        available: true,
        terminal: false,
        in_flight: 0,
        remaining_basis_points: None,
        remaining_seconds: None,
        ceiling: 150,
    };
    AccountTransferSnapshot {
        state: AccountTransferGroup {
            id: "transfer_pool".into(),
            group_id: "grp_pool".into(),
            automatic: false,
            policy_revision: 0,
            revision: 1,
            config: pool_policy().config,
            created_at_ms: 1000,
            checked_at_ms: 1000,
            old: vec![member("acct_first", 96, 66)],
            new: vec![member("acct_second", 0, 50)],
            budget: 30,
            previous_budget: 0,
            stable_since_ms: None,
            observed_successes: 0,
            observed_failures: 0,
            finished: false,
            message: String::new(),
        },
        feedback: Default::default(),
    }
}

fn pool_attempt() -> AttemptContext {
    AttemptContext::new(
        RequestAttemptContext::new(
            ModelRequestId::new("req_pool_capacity").unwrap(),
            ClientApiKeyId::new("key_codex_contract").unwrap(),
        ),
        NonZeroU32::new(1).unwrap(),
        SystemTime::now() + Duration::from_secs(10),
        AccountSelectionPolicy::new(
            RotationStrategy::QuotaResetPriority,
            AccountConcurrency::new(0),
        )
        .with_openai_account_affinity(AccountAffinity::Preferred),
        AccountAttemptContext::new(BTreeSet::new(), None, None)
            .with_account_scope(contract_account_scope()),
        None,
        CancellationToken::new(),
    )
}

#[tokio::test]
async fn pool_capacity_migration_keeps_new_binding_after_old_recovers() {
    for mode in ["pool", "warmup", "transfer", "lease_busy", "disabled"] {
        let store = Arc::new(MemoryAccountStore::default());
        for id in ["acct_first", "acct_second"] {
            create_account(&store, id, "pool-test-only");
            store.set_created_at(id, SystemTime::now() - Duration::from_secs(300));
        }
        store.set_created_at("acct_first", SystemTime::now() - Duration::from_secs(6100));
        let first = ProviderAccountId::new("acct_first").unwrap();
        let second = ProviderAccountId::new("acct_second").unwrap();
        let leases = Arc::new(TestLeaseCoordinator::default());
        if matches!(mode, "pool" | "lease_busy") {
            *leases.handoff_policy.lock().unwrap() = pool_policy();
        }
        match mode {
            "transfer" => leases.transfers.lock().unwrap().push(transfer_snapshot()),
            "warmup" => {
                leases
                    .warmups
                    .lock()
                    .unwrap()
                    .insert(first.clone(), Some(66));
            }
            "lease_busy" => {
                leases.busy_accounts.lock().unwrap().insert(first.clone());
            }
            _ => store.set_scheduling(
                "acct_first",
                Some(AccountConcurrencyLimit::new(66).unwrap()),
                AccountWeight::DEFAULT,
            ),
        }
        leases.signals.lock().unwrap().extend([
            (
                first.clone(),
                signal(if mode == "lease_busy" { 0 } else { 66 }),
            ),
            (second.clone(), signal(0)),
        ]);
        let affinity = Arc::new(MemorySessionAffinity::default());
        let provider = ProviderKind::new("openai").unwrap();
        let key = ProviderSessionAffinityKey::try_new("pool-capacity-session").unwrap();
        affinity
            .bind(&provider, &key, &first, Duration::from_secs(60))
            .await
            .unwrap();
        let selector = selector_with_affinity(&store, leases.clone(), affinity.clone());
        let ctx = pool_attempt();
        let url = Url::parse(OFFICIAL_CODEX_BASE_URL).unwrap();
        let request = SelectCodexCredential {
            upstream_model: "gpt-5.4",
            request_url: &url,
            attempt: &ctx,
            session_affinity_key: Some(&key),
        };
        // 并发续轮共用 CAS，完成请求不能把绑定写回已满载的老号
        let results = futures::future::join_all((0..4).map(|_| selector.select(&request))).await;
        for result in results {
            assert_eq!(result.unwrap().account_id(), &second, "capacity: {mode}");
        }
        let expected = if mode == "disabled" { &first } else { &second };
        assert_eq!(
            affinity.load(&provider, &key).await.unwrap().as_ref(),
            Some(expected),
            "binding: {mode}"
        );
        leases.busy_accounts.lock().unwrap().clear();
        leases
            .signals
            .lock()
            .unwrap()
            .extend([(first.clone(), signal(0)), (second.clone(), signal(20))]);
        tokio::time::sleep(Duration::from_millis(210)).await;
        let selected = selector.select(&request).await.unwrap();
        assert_eq!(selected.account_id(), expected, "after recovery: {mode}");
    }
}

#[tokio::test]
async fn transfer_affinity_uses_only_retained_capacity_and_does_not_bypass_full_receivers() {
    let store = Arc::new(MemoryAccountStore::default());
    for id in ["acct_first", "acct_second"] {
        create_account(&store, id, "pool-test-only");
    }
    let first = ProviderAccountId::new("acct_first").unwrap();
    let second = ProviderAccountId::new("acct_second").unwrap();
    let leases = Arc::new(TestLeaseCoordinator::default());
    leases.transfers.lock().unwrap().push(transfer_snapshot());
    let affinity = Arc::new(MemorySessionAffinity::default());
    let provider = ProviderKind::new("openai").unwrap();
    let key = ProviderSessionAffinityKey::try_new("transfer-session").unwrap();
    affinity
        .bind(&provider, &key, &first, Duration::from_secs(60))
        .await
        .unwrap();
    let selector = selector_with_affinity(&store, leases.clone(), affinity.clone());
    let ctx = pool_attempt();
    let url = Url::parse(OFFICIAL_CODEX_BASE_URL).unwrap();
    let request = SelectCodexCredential {
        upstream_model: "gpt-5.4",
        request_url: &url,
        attempt: &ctx,
        session_affinity_key: Some(&key),
    };
    leases
        .signals
        .lock()
        .unwrap()
        .extend([(first.clone(), signal(65)), (second.clone(), signal(0))]);
    assert_eq!(
        selector.select(&request).await.unwrap().account_id(),
        &first
    );
    assert_eq!(
        leases
            .requests
            .lock()
            .unwrap()
            .last()
            .unwrap()
            .max_concurrent()
            .get(),
        66
    );
    leases
        .signals
        .lock()
        .unwrap()
        .extend([(first.clone(), signal(66)), (second.clone(), signal(50))]);
    tokio::time::sleep(Duration::from_millis(210)).await;
    assert!(matches!(
        selector.select(&request).await,
        Err(CredentialSelectionError::CapacityUnavailable { .. })
    ));
    assert_eq!(affinity.load(&provider, &key).await.unwrap(), Some(first));
    assert_eq!(
        leases.requests.lock().unwrap().len(),
        1,
        "满载时不得创建额外租约"
    );
}

#[tokio::test]
async fn pool_consumption_and_balance_survive_without_active_transfer_and_respect_identity() {
    for mode in [
        "balanced",
        "consume",
        "warming",
        "fixed",
        "strict",
        "disabled",
        "fresh_quota",
        "stale_quota",
        "future_quota",
    ] {
        let store = Arc::new(MemoryAccountStore::default());
        for id in ["acct_first", "acct_second"] {
            create_account(&store, id, "pool-test-only");
            store.set_created_at(id, SystemTime::now() - Duration::from_secs(300));
        }
        if mode.ends_with("quota") {
            let now = SystemTime::now();
            let observed = match mode {
                "stale_quota" => now - Duration::from_secs(301),
                "future_quota" => now + Duration::from_secs(60),
                _ => now,
            };
            let account = store.account("acct_first").unwrap();
            let data = json!({"rate_limit": {"allowed": true, "primary_window": {
                "used_percent": 90,
                "reset_at": (now + Duration::from_secs(3600)).duration_since(SystemTime::UNIX_EPOCH).unwrap().as_secs(),
                "limit_window_seconds": 18000
            }}});
            store
                .compare_and_swap_quota(QuotaObservation {
                    quota_pause: None,
                    expected_connection: None,
                    plan_type: None,
                    account_id: account.id().clone(),
                    expected_revision: account.revision(),
                    quota: OpaqueProviderData::new(data.as_object().unwrap().clone()),
                    observed_at: observed,
                    state: QuotaState::allowed(observed),
                })
                .await
                .unwrap();
            store.set_created_at("acct_first", now - Duration::from_secs(300));
        }
        let first = ProviderAccountId::new("acct_first").unwrap();
        let second = ProviderAccountId::new("acct_second").unwrap();
        if matches!(mode, "consume" | "warming") {
            store.set_created_at("acct_first", SystemTime::now() - Duration::from_secs(6100));
        }
        let leases = Arc::new(TestLeaseCoordinator::default());
        *leases.handoff_policy.lock().unwrap() = AccountHandoffPolicy {
            enabled: mode != "disabled",
            config: gateway_core::account::AccountHandoffConfig {
                transfer_group: Some(Default::default()),
                new_account_protection: Some(Default::default()),
                ..Default::default()
            },
            ..Default::default()
        };
        leases
            .signals
            .lock()
            .unwrap()
            .extend([(first.clone(), signal(40)), (second.clone(), signal(0))]);
        if mode == "warming" {
            leases
                .warmups
                .lock()
                .unwrap()
                .insert(first.clone(), Some(50));
        }
        let affinity = Arc::new(MemorySessionAffinity::default());
        let provider = ProviderKind::new("openai").unwrap();
        let key = ProviderSessionAffinityKey::try_new("pool-policy-session").unwrap();
        affinity
            .bind(&provider, &key, &first, Duration::from_secs(60))
            .await
            .unwrap();
        let selector = selector_with_affinity(&store, leases, affinity.clone());
        let policy = AccountSelectionPolicy::new(
            RotationStrategy::QuotaResetPriority,
            AccountConcurrency::new(0),
        )
        .with_openai_account_affinity(if mode == "strict" {
            AccountAffinity::Strict
        } else {
            AccountAffinity::Preferred
        });
        let ctx = AttemptContext::new(
            RequestAttemptContext::new(
                ModelRequestId::new("req_pool").unwrap(),
                ClientApiKeyId::new("key_codex_contract").unwrap(),
            ),
            NonZeroU32::new(1).unwrap(),
            SystemTime::now() + Duration::from_secs(30),
            policy,
            AccountAttemptContext::new(
                BTreeSet::new(),
                (mode == "fixed").then(|| first.clone()),
                None,
            )
            .with_account_scope(contract_account_scope()),
            None,
            CancellationToken::new(),
        );
        let selected = selector
            .select(&SelectCodexCredential {
                upstream_model: "gpt-5.4",
                request_url: &Url::parse(OFFICIAL_CODEX_BASE_URL).unwrap(),
                attempt: &ctx,
                session_affinity_key: Some(&key),
            })
            .await
            .unwrap();
        assert_eq!(selected.account_id(), &first, "healthy binding: {mode}");
        drop(selected);
        assert_eq!(
            affinity.load(&provider, &key).await.unwrap(),
            Some(first.clone())
        );
        let selected = selector
            .select(&SelectCodexCredential {
                upstream_model: "gpt-5.4",
                request_url: &Url::parse(OFFICIAL_CODEX_BASE_URL).unwrap(),
                attempt: &ctx,
                session_affinity_key: None,
            })
            .await
            .unwrap();
        let expected = if matches!(
            mode,
            "balanced" | "warming" | "strict" | "stale_quota" | "future_quota"
        ) {
            &second
        } else {
            &first
        };
        assert_eq!(selected.account_id(), expected, "unbound: {mode}");
    }
}
