//! 多实例共享发送预算、突发平滑、热调参和重启保留
use gateway_core::{
    account::{AccountTrafficPolicy, ProviderAccountId},
    provider_ports::AccountDispatchStore,
};
use gateway_store::redis::RedisAccountDispatchStore;
use std::time::{Duration, Instant};
use uuid::Uuid;

async fn stores() -> Option<(RedisAccountDispatchStore, RedisAccountDispatchStore)> {
    let url = crate::support::test_env("CPR_TEST_REDIS_URL")?;
    let client = redis::Client::open(url).unwrap();
    let ns = format!("traffic-test-{}", Uuid::new_v4());
    Some((
        RedisAccountDispatchStore::new(client.get_connection_manager().await.unwrap(), &ns)
            .unwrap(),
        RedisAccountDispatchStore::new(client.get_connection_manager().await.unwrap(), &ns)
            .unwrap(),
    ))
}
fn policy() -> AccountTrafficPolicy {
    AccountTrafficPolicy {
        enabled: true,
        ..Default::default()
    }
}

#[tokio::test]
async fn burst_and_refill_are_shared_across_independent_connections() {
    let Some((one, two)) = stores().await else {
        return;
    };
    let id = ProviderAccountId::new("acct_existing").unwrap();
    let start = Instant::now();
    let mut jobs = Vec::new();
    for i in 0..100 {
        let store = if i % 2 == 0 { one.clone() } else { two.clone() };
        let id = id.clone();
        jobs.push(tokio::spawn(async move {
            store.try_start(&id, policy()).await.unwrap()
        }));
    }
    let mut allowed = 0;
    for job in jobs {
        if job.await.unwrap().is_zero() {
            allowed += 1;
        }
    }
    let upper = 30 + (start.elapsed().as_millis() * 20 / 1000) as usize;
    assert!(
        (30..=upper).contains(&allowed),
        "allowed={allowed}, upper={upper}"
    );
    assert!(allowed < 100);
    tokio::time::sleep(Duration::from_millis(110)).await;
    assert!(two.try_start(&id, policy()).await.unwrap().is_zero());
    let other = ProviderAccountId::new("acct_new").unwrap();
    assert!(one.try_start(&other, policy()).await.unwrap().is_zero());
}
#[tokio::test]
async fn hundred_starts_follow_burst_plus_rate_envelope_and_policy_changes_do_not_refill() {
    let Some((one, two)) = stores().await else {
        return;
    };
    let id = ProviderAccountId::new("acct_unlimited").unwrap();
    let start = Instant::now();
    for i in 1_u64..=100 {
        loop {
            let delay = one.try_start(&id, policy()).await.unwrap();
            if delay.is_zero() {
                break;
            }
            tokio::time::sleep(delay).await;
        }
        let earliest = Duration::from_millis((i.saturating_sub(30_u64)) * 50);
        assert!(start.elapsed() + Duration::from_millis(2) >= earliest);
    }
    assert!(start.elapsed() >= Duration::from_millis(3490));
    let larger = AccountTrafficPolicy {
        burst: 60,
        ..policy()
    };
    assert!(
        !two.try_start(&id, larger).await.unwrap().is_zero(),
        "config edits must not refill an empty bucket"
    );
    let disabled = AccountTrafficPolicy {
        enabled: false,
        ..policy()
    };
    assert!(two.try_start(&id, disabled).await.unwrap().is_zero());
    assert!(
        !two.try_start(&id, policy()).await.unwrap().is_zero(),
        "toggle must not clear existing token debt"
    );
}
