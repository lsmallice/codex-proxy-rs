//! Redis 交接幂等、并发竞争、失败反馈与实际租约上限

use gateway_core::{account::*, provider_ports::*, routing::ProviderKind};
use gateway_store::redis::RedisAccountHandoffStore;
use std::time::{Duration, SystemTime};
use uuid::Uuid;

async fn stores() -> Option<RedisAccountHandoffStore> {
    let (store, _, _) = store_connection().await?;
    Some(store)
}

async fn store_connection() -> Option<(
    RedisAccountHandoffStore,
    redis::aio::ConnectionManager,
    String,
)> {
    let url = crate::support::test_env("CPR_TEST_REDIS_URL")?;
    let connection = redis::Client::open(url)
        .unwrap()
        .get_connection_manager()
        .await
        .unwrap();
    let namespace = format!("handoff-test-{}", Uuid::new_v4());
    Some((
        RedisAccountHandoffStore::new(connection.clone(), &namespace).unwrap(),
        connection,
        namespace,
    ))
}

fn state() -> AccountHandoff {
    let now = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64;
    AccountHandoff::start(
        StartAccountHandoff {
            request_id: Uuid::new_v4().to_string(),
            old_account_id: "acct_old".into(),
            new_account_id: "acct_new".into(),
            config: AccountHandoffConfig::default(),
        },
        ["acct_old".into(), "acct_new".into()],
        [150, 0],
        None,
        now,
    )
    .unwrap()
}

fn transfer_group() -> AccountTransferGroup {
    let member = |id: &str, baseline, ceiling, remaining| TransferMember {
        id: id.into(),
        name: id.into(),
        baseline,
        ceiling,
        remaining_basis_points: Some(remaining),
        remaining_seconds: None,
        limit: 0,
        in_flight: baseline,
        available: true,
        terminal: false,
    };
    AccountTransferGroup {
        id: Uuid::new_v4().to_string(),
        group_id: "grp_pool".into(),
        automatic: false,
        policy_revision: 0,
        revision: 0,
        config: AccountHandoffConfig {
            transfer_group: Some(Default::default()),
            new_account_protection: Some(Default::default()),
            ..Default::default()
        },
        created_at_ms: 1,
        checked_at_ms: 1,
        old: vec![
            member("acct_o1", 120, 200, 1700),
            member("acct_o2", 100, 200, 3700),
        ],
        new: vec![member("acct_n1", 0, 50, 9000)],
        budget: 50,
        previous_budget: 0,
        stable_since_ms: None,
        observed_successes: 0,
        observed_failures: 0,
        finished: false,
        message: String::new(),
    }
}

#[tokio::test]
async fn handoff_group_redis_join_cas_and_shared_cap_survive_restarts() {
    use gateway_store::redis::{
        CredentialBoundedLeaseAcquisition, CredentialBoundedLeaseRequest, CredentialLeaseScope,
        RedisCredentialLeaseRepository,
    };
    let Some((store, connection, namespace)) = store_connection().await else {
        return;
    };
    let mut g = transfer_group();
    g.rebalance();
    assert!(store.save_transfer_group(g.clone(), 0, 0).await.unwrap());
    assert_eq!(store.warmups().await.unwrap().len(), 1);
    let mut duplicate = g.clone();
    duplicate.id = Uuid::new_v4().to_string();
    assert!(!store.save_transfer_group(duplicate, 0, 0).await.unwrap());
    g = store.transfer_groups().await.unwrap().remove(0).state;
    let repo = RedisCredentialLeaseRepository::new(connection.clone(), &namespace).unwrap();
    let mut previous_guards = Vec::new();
    for _ in 0..50 {
        let acquired = repo
            .try_acquire_bounded_lease_with_metadata(
                &CredentialBoundedLeaseRequest {
                    scope: CredentialLeaseScope::ProviderAccount,
                    resource_id: "acct_n1".into(),
                    owner_id: Uuid::new_v4().to_string(),
                    max_concurrent: 0,
                    ttl: Duration::from_secs(60),
                },
                None,
            )
            .await
            .unwrap();
        if let CredentialBoundedLeaseAcquisition::Acquired(guard) = acquired {
            previous_guards.push(guard)
        } else {
            panic!("initial capacity missing")
        }
    }
    let mut second = g.new[0].clone();
    second.id = "acct_n2".into();
    second.name = second.id.clone();
    g.new.push(second);
    // 管理员缩小保护容量后，旧在途仍占共享上限，不能给新增成员重复放行
    g.new.iter_mut().for_each(|m| m.ceiling = 25);
    g.rebalance();
    assert!(store.save_transfer_group(g.clone(), 1, 0).await.unwrap());
    assert!(matches!(
        repo.try_acquire_bounded_lease_with_metadata(
            &CredentialBoundedLeaseRequest {
                scope: CredentialLeaseScope::ProviderAccount,
                resource_id: "acct_n2".into(),
                owner_id: Uuid::new_v4().to_string(),
                max_concurrent: 0,
                ttl: Duration::from_secs(60)
            },
            None
        )
        .await
        .unwrap(),
        CredentialBoundedLeaseAcquisition::Busy { .. }
    ));
    for guard in previous_guards {
        guard.release().await.unwrap();
    }
    let mut guards = vec![];
    for id in ["acct_n1", "acct_n2"] {
        let results = futures::future::join_all((0..80).map(|_| async {
            repo.try_acquire_bounded_lease_with_metadata(
                &CredentialBoundedLeaseRequest {
                    scope: CredentialLeaseScope::ProviderAccount,
                    resource_id: id.into(),
                    owner_id: Uuid::new_v4().to_string(),
                    max_concurrent: 0,
                    ttl: Duration::from_secs(60),
                },
                None,
            )
            .await
        }))
        .await;
        let mut n = 0;
        for r in results {
            if let CredentialBoundedLeaseAcquisition::Acquired(guard) = r.unwrap() {
                n += 1;
                guards.push(guard)
            }
        }
        assert_eq!(n, 25);
    }
    store.feedback("acct_n1".into(), false, None).await.unwrap();
    g.revision = 2;
    assert!(!store.save_transfer_group(g.clone(), 2, 0).await.unwrap());
    let recreated = RedisAccountHandoffStore::new(connection, &namespace).unwrap();
    let snapshot = recreated.transfer_groups().await.unwrap().remove(0);
    assert_eq!(snapshot.feedback.failures, 1);
    assert_eq!(snapshot.state.new.len(), 2);
    let mut retry = snapshot.state.clone();
    let mut finished = snapshot.state;
    finished.finished = true;
    assert!(recreated.save_transfer_group(finished, 2, 1).await.unwrap());
    assert!(recreated.transfer_groups().await.unwrap().is_empty());
    assert_eq!(recreated.warmups().await.unwrap().len(), 2);
    let ids = retry
        .new
        .iter()
        .map(|m| ProviderAccountId::new(m.id.clone()).unwrap())
        .collect::<Vec<_>>();
    assert_eq!(recreated.handled(&ids).await.unwrap().len(), 2);
    assert!(
        recreated
            .save_policy(
                AccountHandoffPolicy {
                    enabled: true,
                    revision: 1,
                    config: retry.config.clone(),
                    ..Default::default()
                },
                0
            )
            .await
            .unwrap()
    );
    retry.id = Uuid::new_v4().to_string();
    retry.revision = 0;
    retry.automatic = true;
    retry.policy_revision = 1;
    let mut siblings = retry.clone();
    siblings.old = vec![siblings.new.remove(0)];
    // 原老号退出后，两个承接账号不能自动被拼成新的 A -> B 交接
    assert!(!recreated.save_transfer_group(siblings, 0, 0).await.unwrap());
    // 即使调用方仍使用取消前的候选快照，原子回执也禁止自动重建
    assert!(
        !recreated
            .save_transfer_group(retry.clone(), 0, 0)
            .await
            .unwrap()
    );
    retry.automatic = false;
    assert!(
        recreated
            .save_transfer_group(retry.clone(), 0, 0)
            .await
            .unwrap()
    );
    retry.finished = true;
    assert!(recreated.save_transfer_group(retry, 1, 0).await.unwrap());
    for guard in guards {
        guard.release().await.unwrap();
    }
}

#[tokio::test]
async fn handoff_receiver_promotion_is_atomic_and_keeps_warmup_completion_receipts() {
    let Some((store, _, _)) = store_connection().await else {
        return;
    };
    let mut g = transfer_group();
    g.automatic = true;
    g.policy_revision = 1;
    let mut next = g.new[0].clone();
    next.id = "acct_n2".into();
    next.name = next.id.clone();
    g.new.push(next);
    assert!(
        store
            .save_policy(
                AccountHandoffPolicy {
                    enabled: true,
                    revision: 1,
                    config: g.config.clone(),
                    ..Default::default()
                },
                0
            )
            .await
            .unwrap()
    );
    g.rebalance();
    assert!(store.save_transfer_group(g, 0, 0).await.unwrap());
    let mut finished = store
        .warmups()
        .await
        .unwrap()
        .into_iter()
        .find(|w| w.state.account_id == "acct_n1")
        .unwrap()
        .state;
    finished.finished = true;
    assert!(store.update_warmup(finished, 0).await.unwrap());
    let warm_before = store.warmups().await.unwrap();
    let mut snapshot = store.transfer_groups().await.unwrap().remove(0);
    snapshot.state.new[0].remaining_basis_points = Some(1000);
    let now = 10_000_000;
    let roles = std::collections::BTreeMap::from([
        (
            "acct_n1".into(),
            gateway_core::account::TransferRoleObservation {
                created_at_ms: now - 1_800_000,
                protected: false,
            },
        ),
        (
            "acct_n2".into(),
            gateway_core::account::TransferRoleObservation {
                created_at_ms: now - 1000,
                protected: true,
            },
        ),
    ]);
    assert!(
        snapshot
            .state
            .promote_receivers(&roles, now, snapshot.feedback)
    );
    assert!(
        !store
            .save_transfer_group(snapshot.state.clone(), snapshot.state.revision, 1)
            .await
            .unwrap()
    );
    assert!(
        store
            .save_transfer_group(snapshot.state.clone(), snapshot.state.revision, 0)
            .await
            .unwrap()
    );
    assert!(
        !store
            .save_transfer_group(snapshot.state.clone(), snapshot.state.revision, 0)
            .await
            .unwrap()
    );
    let saved = store.transfer_groups().await.unwrap().remove(0);
    assert_eq!(saved.state.new.len(), 1);
    assert_eq!(saved.state.new[0].id, "acct_n2");
    assert!(saved.state.old.iter().any(|m| m.id == "acct_n1"));
    assert_eq!(store.warmups().await.unwrap(), warm_before);
    assert_eq!(
        store
            .warmup_limits(&[ProviderAccountId::new("acct_n1").unwrap()])
            .await
            .unwrap()
            .values()
            .next(),
        Some(&None)
    );
    assert!(
        store
            .handled(&[ProviderAccountId::new("acct_n1").unwrap()])
            .await
            .unwrap()
            .contains(&ProviderAccountId::new("acct_n1").unwrap())
    );
}

#[tokio::test]
async fn handoff_group_redis_accepts_growth_without_multiplying_per_account_protection() {
    use gateway_store::redis::{
        CredentialBoundedLeaseAcquisition, CredentialBoundedLeaseRequest, CredentialLeaseScope,
        RedisCredentialLeaseRepository,
    };
    let Some((store, connection, namespace)) = store_connection().await else {
        return;
    };
    let mut group = transfer_group();
    let mut second = group.new[0].clone();
    second.id = "acct_n2".into();
    second.name = second.id.clone();
    group.new.push(second);
    group.rebalance();
    assert_eq!(group.budget, 50);
    assert!(store.save_transfer_group(group, 0, 0).await.unwrap());
    let repo = RedisCredentialLeaseRepository::new(connection, &namespace).unwrap();
    let attempts = (0..160).map(|index| {
        let repo = &repo;
        async move {
            let account = if index % 2 == 0 { "acct_n1" } else { "acct_n2" };
            let result = repo
                .try_acquire_bounded_lease_with_metadata(
                    &CredentialBoundedLeaseRequest {
                        scope: CredentialLeaseScope::ProviderAccount,
                        resource_id: account.into(),
                        owner_id: Uuid::new_v4().to_string(),
                        max_concurrent: 0,
                        ttl: Duration::from_secs(60),
                    },
                    None,
                )
                .await
                .unwrap();
            (account, result)
        }
    });
    let mut counts = std::collections::BTreeMap::new();
    let mut guards = Vec::new();
    for (id, result) in futures::future::join_all(attempts).await {
        if let CredentialBoundedLeaseAcquisition::Acquired(guard) = result {
            *counts.entry(id).or_insert(0) += 1;
            guards.push(guard);
        }
    }
    assert_eq!(counts.get("acct_n1"), Some(&50));
    assert_eq!(counts.get("acct_n2"), Some(&50));
    for guard in guards {
        guard.release().await.unwrap();
    }
}

#[tokio::test]
async fn handoff_warmup_caps_stale_unlimited_leases_and_survives_old_completion() {
    use gateway_store::redis::{
        CredentialBoundedLeaseAcquisition, CredentialBoundedLeaseRequest, CredentialLeaseScope,
        RedisCredentialLeaseRepository,
    };
    let Some((store, connection, namespace)) = store_connection().await else {
        return;
    };
    let policy = AccountHandoffPolicy {
        enabled: true,
        revision: 1,
        config: AccountHandoffConfig {
            new_account_protection: Some(AccountWarmupConfig::default()),
            ..Default::default()
        },
        ..Default::default()
    };
    assert!(store.save_policy(policy, 0).await.unwrap());
    let start = state();
    store.start(start.clone()).await.unwrap();
    for id in ["acct_new", "acct_other"] {
        store
            .start_warmup(AccountWarmup::new(
                id.into(),
                id.into(),
                AccountWarmupConfig::default(),
                1,
                start.created_at_ms,
            ))
            .await
            .unwrap();
    }
    let repo = RedisCredentialLeaseRepository::new(connection.clone(), &namespace).unwrap();
    let mut guards = vec![];
    for id in ["acct_new", "acct_other"] {
        let results = futures::future::join_all((0..150).map(|_| async {
            repo.try_acquire_bounded_lease_with_metadata(
                &CredentialBoundedLeaseRequest {
                    scope: CredentialLeaseScope::ProviderAccount,
                    resource_id: id.into(),
                    owner_id: Uuid::new_v4().to_string(),
                    max_concurrent: 0,
                    ttl: Duration::from_secs(60),
                },
                None,
            )
            .await
        }))
        .await;
        let mut admitted = 0;
        for result in results {
            if let CredentialBoundedLeaseAcquisition::Acquired(guard) = result.unwrap() {
                guards.push(guard);
                admitted += 1;
            }
        }
        assert_eq!(admitted, 50);
    }
    let mut finished = start;
    finished.phase = AccountHandoffPhase::Completed;
    assert!(store.update(finished, 1, 0).await.unwrap());
    let recreated = RedisAccountHandoffStore::new(connection, &namespace).unwrap();
    assert_eq!(recreated.warmups().await.unwrap().len(), 2);
    let snapshot = recreated
        .warmups()
        .await
        .unwrap()
        .into_iter()
        .find(|x| x.state.account_id == "acct_new")
        .unwrap();
    recreated
        .feedback("acct_new".into(), false, None)
        .await
        .unwrap();
    let mut next = snapshot.state.clone();
    next.limit = 80;
    assert!(!recreated.update_warmup(next, 0).await.unwrap());
    assert_eq!(
        recreated
            .warmup_limits(&[ProviderAccountId::new("acct_new").unwrap()])
            .await
            .unwrap()
            .values()
            .next(),
        Some(&Some(50))
    );
    let mut final_state = snapshot.state;
    final_state.finished = true;
    assert!(recreated.update_warmup(final_state, 1).await.unwrap());
    assert_eq!(
        recreated
            .warmup_limits(&[ProviderAccountId::new("acct_new").unwrap()])
            .await
            .unwrap()
            .values()
            .next(),
        Some(&None)
    );
    for guard in guards {
        guard.release().await.unwrap();
    }
}

#[tokio::test]
async fn handoff_start_is_idempotent_and_accounts_cannot_join_overlapping_pairs() {
    let Some(store) = stores().await else {
        return;
    };
    let start = state();
    let results = futures::future::join_all((0..20).map(|_| store.start(start.clone()))).await;
    assert!(results.iter().all(Result::is_ok));
    assert_eq!(store.list().await.unwrap().len(), 1);
    let mut conflicting = start.clone();
    conflicting.id = Uuid::new_v4().to_string();
    assert_eq!(
        store.start(conflicting).await.unwrap_err().kind(),
        ProviderStoreErrorKind::Conflict
    );
    let mut changed = start.clone();
    changed.new_account_id = "other".into();
    assert_eq!(
        store.start(changed).await.unwrap_err().kind(),
        ProviderStoreErrorKind::Conflict
    );
    let mut cancelled = start.clone();
    cancelled.phase = AccountHandoffPhase::Cancelled;
    assert!(store.update(cancelled, 1, 0).await.unwrap());
    assert!(store.list().await.unwrap().is_empty());
    assert_eq!(
        store.start(start).await.unwrap().state.phase,
        AccountHandoffPhase::Cancelled
    );
    assert_eq!(store.history(1, 20).await.unwrap().total, 1);
}

#[tokio::test]
async fn handoff_history_pages_completed_and_cancelled_snapshots_with_terminal_feedback() {
    let Some((store, connection, namespace)) = store_connection().await else {
        return;
    };
    let active = state();
    let mut ended = Vec::new();
    for index in 0..45 {
        let mut s = state();
        s.old_account_id = format!("acct_old_{index}");
        s.new_account_id = format!("acct_new_{index}");
        store.start(s.clone()).await.unwrap();
        store
            .feedback(s.new_account_id.clone(), true, Some(s.created_at_ms))
            .await
            .unwrap();
        store
            .feedback(s.new_account_id.clone(), false, None)
            .await
            .unwrap();
        s.phase = if index % 2 == 0 {
            AccountHandoffPhase::Completed
        } else {
            AccountHandoffPhase::Cancelled
        };
        s.old_limit = 50;
        s.message = format!("结束原因 {index}");
        assert!(!store.update(s.clone(), 1, 0).await.unwrap());
        assert!(store.update(s.clone(), 1, 1).await.unwrap());
        assert!(!store.update(s.clone(), 1, 1).await.unwrap());
        ended.push(s.id);
    }
    store.start(active.clone()).await.unwrap();
    let first = store.history(1, 20).await.unwrap();
    let second = store.history(2, 20).await.unwrap();
    let third = store.history(3, 20).await.unwrap();
    assert_eq!(
        (
            first.total,
            first.items.len(),
            second.items.len(),
            third.items.len()
        ),
        (45, 20, 20, 5)
    );
    let all: Vec<_> = first
        .items
        .into_iter()
        .chain(second.items)
        .chain(third.items)
        .collect();
    assert_eq!(
        all.iter()
            .map(|s| &s.state.id)
            .collect::<std::collections::BTreeSet<_>>()
            .len(),
        45
    );
    assert!(all.iter().all(|s| !s.state.phase.active()
        && s.state.finished_at_ms.is_some()
        && s.state.old_limit == 50
        && s.state.message.starts_with("结束原因")
        && s.feedback.successes == 1
        && s.feedback.failures == 1));
    assert!(
        all.windows(2)
            .all(|w| (w[0].state.finished_at_ms, &w[0].state.id)
                >= (w[1].state.finished_at_ms, &w[1].state.id))
    );
    assert_eq!(store.list().await.unwrap().len(), 1);
    assert_eq!(store.history(u32::MAX, 20).await.unwrap().page, 3);
    for (page, size) in [(0, 20), (1, 0), (1, 101)] {
        assert!(store.history(page, size).await.is_err());
    }
    let restored = RedisAccountHandoffStore::new(connection, &namespace).unwrap();
    assert_eq!(restored.history(1, 20).await.unwrap().total, 45);
    assert_eq!(restored.list().await.unwrap()[0].state.id, active.id);
    for id in ended {
        assert!(
            restored
                .get(id)
                .await
                .unwrap()
                .unwrap()
                .state
                .finished_at_ms
                .is_some()
        );
    }
}

#[tokio::test]
async fn handoff_history_prunes_expired_and_excess_records_without_touching_active_or_receipts() {
    let Some((store, mut connection, namespace)) = store_connection().await else {
        return;
    };
    let s = state();
    store.start(s.clone()).await.unwrap();
    let mut finished = s.clone();
    finished.phase = AccountHandoffPhase::Cancelled;
    assert!(store.update(finished, 1, 0).await.unwrap());
    let record = store.history(1, 20).await.unwrap().items.remove(0);
    let index = format!("{namespace}:scheduler:{{handoff}}:active:history-index");
    let data = format!("{namespace}:scheduler:{{handoff}}:active:history-data");
    let now = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64;
    let _: () = redis::cmd("ZADD")
        .arg(&index)
        .arg(now - (HANDOFF_HISTORY_RETENTION_SECONDS + 1) * 1000)
        .arg(&s.id)
        .query_async(&mut connection)
        .await
        .unwrap();
    assert_eq!(store.history(1, 20).await.unwrap().total, 0);
    assert!(store.get(s.id.clone()).await.unwrap().is_some());
    let mut pipeline = redis::pipe();
    for i in 0..=MAX_HANDOFF_HISTORY_RECORDS {
        let mut fixture = record.clone();
        fixture.state.id = format!("history_{i:05}");
        fixture.state.finished_at_ms = Some(now);
        pipeline
            .cmd("ZADD")
            .arg(&index)
            .arg(now)
            .arg(&fixture.state.id)
            .ignore()
            .cmd("HSET")
            .arg(&data)
            .arg(&fixture.state.id)
            .arg(serde_json::to_string(&fixture).unwrap())
            .ignore();
    }
    let _: () = pipeline.query_async(&mut connection).await.unwrap();
    let active = state();
    store.start(active.clone()).await.unwrap();
    let page = store.history(1, 20).await.unwrap();
    assert_eq!(page.total, MAX_HANDOFF_HISTORY_RECORDS as u64);
    assert_eq!(page.items.len(), 20);
    let n: usize = redis::cmd("HLEN")
        .arg(&data)
        .query_async(&mut connection)
        .await
        .unwrap();
    assert_eq!(n, MAX_HANDOFF_HISTORY_RECORDS);
    assert_eq!(store.list().await.unwrap()[0].state.id, active.id);
    assert_eq!(
        store.get(s.id).await.unwrap().unwrap().state.phase,
        AccountHandoffPhase::Cancelled
    );
}

#[tokio::test]
async fn handoff_429_prevents_stale_advancement_and_late_success_does_not_hide_failure() {
    let Some(store) = stores().await else {
        return;
    };
    let start = state();
    store.start(start.clone()).await.unwrap();
    store
        .feedback("acct_new".into(), true, Some(start.created_at_ms - 1))
        .await
        .unwrap();
    assert_eq!(store.list().await.unwrap()[0].feedback.successes, 0);
    store
        .feedback("acct_new".into(), false, None)
        .await
        .unwrap();
    store
        .feedback("acct_new".into(), true, Some(start.created_at_ms))
        .await
        .unwrap();
    let mut next = start.clone();
    next.old_limit = 70;
    assert!(!store.update(next.clone(), 1, 0).await.unwrap());
    let snapshot = store.list().await.unwrap().remove(0);
    assert_eq!(
        (snapshot.feedback.failures, snapshot.feedback.successes),
        (1, 1)
    );
    assert!(store.update(next.clone(), 1, 1).await.unwrap());
    assert!(!store.update(next, 1, 1).await.unwrap());
}

#[tokio::test]
async fn handoff_latest_limit_is_atomic_even_with_stale_unlimited_admission() {
    let (Some(database), Some(redis)) = (
        crate::support::test_env("CPR_TEST_DATABASE_URL"),
        crate::support::test_env("CPR_TEST_REDIS_URL"),
    ) else {
        return;
    };
    let mut config: gateway_store::StoreConfig = serde_json::from_value(serde_json::json!({
        "database": crate::support::config_connection(url::Url::parse(&database).unwrap()),
        "redis": crate::support::config_connection(url::Url::parse(&redis).unwrap()),
    }))
    .unwrap();
    let directory = tempfile::tempdir().unwrap();
    config.resolve_and_validate(directory.path()).unwrap();
    let bundle = gateway_store::initialize(config).await.unwrap();
    let store = bundle.provider_ports().handoffs().unwrap();
    let leases = bundle.provider_ports().leases();
    let mut start = state();
    // Bundle 的命名空间来自数据库，重复运行需隔离账号，避免上一轮未回收租约干扰
    let run_id = Uuid::new_v4();
    let old_id = format!("acct_old_{run_id}");
    let new_id = format!("acct_new_{run_id}");
    start.old_account_id.clone_from(&old_id);
    start.new_account_id.clone_from(&new_id);
    start.old_limit = 50;
    store.start(start.clone()).await.unwrap();
    let request = |id: &str, handoff: Option<String>| {
        ProviderLeaseRequest::Scheduling(
            ProviderSchedulingLeaseRequest::new(
                ProviderKind::new("openai").unwrap(),
                ProviderAccountId::new(id).unwrap(),
                CredentialRevision::new(1).unwrap(),
                AccountConcurrency::new(0),
                SystemTime::now() + Duration::from_secs(60),
            )
            .with_account_handoff(handoff),
        )
    };
    let results = futures::future::join_all(
        (0..150).map(|_| leases.try_acquire(request(&old_id, Some(start.id.clone())))),
    )
    .await;
    let old_guards = results
        .into_iter()
        .filter_map(|r| match r.unwrap() {
            ProviderLeaseAcquisition::Acquired(g) => Some(g),
            ProviderLeaseAcquisition::Busy { .. } => None,
        })
        .collect::<Vec<_>>();
    assert_eq!(old_guards.len(), 50);
    let results =
        futures::future::join_all((0..150).map(|_| leases.try_acquire(request(&new_id, None))))
            .await;
    let new_guards = results
        .into_iter()
        .map(|r| match r.unwrap() {
            ProviderLeaseAcquisition::Acquired(g) => g,
            _ => panic!("new account must remain unlimited"),
        })
        .collect::<Vec<_>>();
    assert_eq!(new_guards.len(), 150);
    let mut cancelled = start;
    cancelled.phase = AccountHandoffPhase::Cancelled;
    assert!(store.update(cancelled, 1, 0).await.unwrap());
    assert!(matches!(
        leases.try_acquire(request(&old_id, None)).await.unwrap(),
        ProviderLeaseAcquisition::Acquired(_)
    ));
    drop((old_guards, new_guards));
}

#[tokio::test]
async fn handoff_policy_cas_and_disable_block_stale_automatic_start() {
    let Some(store) = stores().await else {
        return;
    };
    assert!(!store.policy().await.unwrap().enabled);
    let policy = AccountHandoffPolicy {
        enabled: true,
        revision: 1,
        ..Default::default()
    };
    assert!(store.save_policy(policy.clone(), 0).await.unwrap());
    assert!(!store.save_policy(policy.clone(), 0).await.unwrap());
    let mut disabled = policy;
    disabled.enabled = false;
    disabled.revision = 2;
    assert!(store.save_policy(disabled, 1).await.unwrap());
    let mut candidate = state();
    candidate.automatic = true;
    candidate.automatic_policy_revision = 1;
    assert_eq!(
        store.start(candidate).await.unwrap_err().kind(),
        ProviderStoreErrorKind::Conflict
    );
}
