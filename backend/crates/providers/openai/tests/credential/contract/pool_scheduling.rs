//! 交接结束后的分层选号、软亲和分流及固定续接保护

use super::*;
use gateway_core::account::{AccountConcurrency, AccountHandoffPolicy, AccountRuntimeSignals};

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
        let selector = selector_with_affinity(&store, leases, affinity);
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
        let expected = if matches!(
            mode,
            "balanced" | "warming" | "stale_quota" | "future_quota"
        ) {
            &second
        } else {
            &first
        };
        assert_eq!(selected.account_id(), expected, "{mode}");
    }
}
