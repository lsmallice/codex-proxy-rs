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
