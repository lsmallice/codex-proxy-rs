//! 新号保护的稳定升档、失败回退和闲置恢复测试
use super::{candidate, context};
use gateway_core::account::*;

fn state() -> AccountWarmup {
    AccountWarmup::new(
        "new".into(),
        "new".into(),
        AccountWarmupConfig {
            release_concurrency: 80,
            ..Default::default()
        },
        1,
        1000,
    )
}
fn feedback(successes: u64, failures: u64, now: u64) -> AccountHandoffFeedback {
    AccountHandoffFeedback {
        successes,
        failures,
        last_success_at_ms: now,
        last_failure_at_ms: if failures > 0 { now } else { 0 },
    }
}

#[test]
fn handoff_warmup_requires_each_stable_stage_before_release() {
    let mut s = state();
    for i in 1..=13 {
        let now = 1000 + i * 5000;
        s.advance(now, true, false, 50, None, feedback(i * 5, 0, now));
    }
    assert_eq!(s.limit, 80);
    assert!(!s.finished);
    for i in 14..=25 {
        let now = 1000 + i * 5000;
        s.advance(now, true, false, 70, None, feedback(i * 5, 0, now));
    }
    assert!(s.finished);
}
#[test]
fn handoff_warmup_failure_rolls_back_and_idle_or_gaps_do_not_release() {
    let mut s = state();
    s.limit = 80;
    s.previous_limit = 50;
    s.advance(6000, true, false, 80, None, feedback(20, 1, 6000));
    assert_eq!(s.limit, 50);
    assert!(s.stable_since_ms.is_none());
    s.advance(11000, true, false, 0, None, feedback(20, 1, 6000));
    s.advance(100000, true, false, 0, None, feedback(20, 1, 6000));
    assert!(!s.finished);
    assert!(s.stable_since_ms.is_none());
    s.advance(105000, false, false, 0, None, feedback(20, 1, 6000));
    assert_eq!(s.limit, 50);
}
#[test]
fn handoff_warmup_terminal_and_existing_lower_ceiling_are_respected() {
    let mut s = state();
    for i in 1..=13 {
        let now = 1000 + i * 5000;
        s.advance(now, true, false, 20, Some(20), feedback(i * 5, 0, now));
    }
    assert!(s.finished);
    let mut s = state();
    s.advance(6000, false, true, 5, None, Default::default());
    assert!(s.finished);
    assert!(
        AccountWarmupConfig {
            initial_concurrency: 0,
            ..Default::default()
        }
        .validate()
        .is_err()
    );
    assert!(
        AccountWarmupConfig {
            release_concurrency: 40,
            ..Default::default()
        }
        .validate()
        .is_err()
    );
}

#[test]
fn handoff_warmup_applies_without_pair_and_survives_peer_exclusion() {
    let mut fresh = candidate("acct_fresh", 49, None);
    fresh.warmup_limit = Some(50);
    let mut ctx = context(RotationStrategy::RoundRobin);
    ctx.policy =
        AccountSelectionPolicy::new(RotationStrategy::RoundRobin, AccountConcurrency::new(0));
    assert_eq!(ctx.concurrency_limit(&fresh).get(), 50);
    ctx.excluded_accounts
        .insert(ProviderAccountId::new("acct_old").unwrap());
    assert!(AccountSelector.select(&[fresh.clone()], &ctx).is_some());
    fresh.signals.in_flight = 50;
    assert!(AccountSelector.select(&[fresh.clone()], &ctx).is_none());
    fresh.account = fresh.account.with_scheduling(
        AccountConcurrencyLimit::new(20),
        AccountWeight::new(100).unwrap(),
    );
    assert_eq!(ctx.concurrency_limit(&fresh).get(), 20);
}
