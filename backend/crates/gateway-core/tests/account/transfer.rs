//! 多老号共享承接、动态均摊、角色判定和故障回退测试
use super::{candidate, context};

#[test]
fn handoff_transfer_departed_donor_does_not_double_reduce_remaining_old() {
    let mut g = group();
    g.old[0].terminal = true;
    g.budget = 50;
    g.rebalance();
    assert_eq!(
        g.old.iter().find(|m| m.id == "acct_old2").unwrap().limit,
        100
    );
    assert_eq!(g.new[0].limit, 50);
}
use gateway_core::account::*;

#[test]
fn handoff_transfer_prediction_expires_and_recomputes_from_observation_time() {
    let mut points = vec![
        TransferQuotaSample {
            id: "acct_a".into(),
            observed: 1000,
            reset: 900000,
            remaining: 9000,
        },
        TransferQuotaSample {
            id: "acct_a".into(),
            observed: 61000,
            reset: 900000,
            remaining: 6000,
        },
    ];
    assert_eq!(
        TransferQuotaSample::remaining_seconds_at(&points, 61000),
        Some(120)
    );
    assert_eq!(
        TransferQuotaSample::remaining_seconds_at(&points, 71000),
        Some(110)
    );
    assert_eq!(
        TransferQuotaSample::remaining_seconds_at(&points, 60000),
        None
    );
    assert_eq!(
        TransferQuotaSample::remaining_seconds_at(&points, 361001),
        None
    );
    points[1].reset = 60000;
    assert_eq!(
        TransferQuotaSample::remaining_seconds_at(&points, 61000),
        None
    );
}

#[test]
fn handoff_transfer_targets_balance_after_old_floor_and_do_not_override_pinned_scope() {
    let mut g = group();
    g.new.push(member("acct_new2", 0, 50, 9000));
    g.rebalance();
    let caps = g.capacities();
    let mut a = candidate("acct_old1", 70, None);
    a.transfer = caps.get("acct_old1").cloned();
    let mut b = candidate("acct_new1", 20, None);
    b.transfer = caps.get("acct_new1").cloned();
    let mut c = candidate("acct_new2", 0, None);
    c.transfer = caps.get("acct_new2").cloned();
    let mut ctx = context(RotationStrategy::QuotaResetPriority);
    ctx.policy = AccountSelectionPolicy::new(
        RotationStrategy::QuotaResetPriority,
        AccountConcurrency::new(0),
    );
    assert_eq!(
        AccountSelector
            .select(&[a, b.clone(), c.clone()], &ctx)
            .unwrap()
            .candidate()
            .account
            .id()
            .as_str(),
        "acct_new2"
    );
    ctx.excluded_accounts
        .insert(ProviderAccountId::new("acct_new2").unwrap());
    assert_eq!(
        AccountSelector
            .select(&[b, c], &ctx)
            .unwrap()
            .candidate()
            .account
            .id()
            .as_str(),
        "acct_new1"
    );
}

pub fn member(id: &str, baseline: u32, ceiling: u32, remaining: u64) -> TransferMember {
    TransferMember {
        id: id.into(),
        name: id.into(),
        baseline,
        limit: 0,
        available: true,
        terminal: false,
        in_flight: baseline,
        remaining_basis_points: Some(remaining),
        remaining_seconds: None,
        ceiling,
    }
}
pub fn group() -> AccountTransferGroup {
    AccountTransferGroup {
        id: "group".into(),
        group_id: "grp_pool".into(),
        automatic: false,
        policy_revision: 0,
        revision: 1,
        config: AccountHandoffConfig {
            transfer_group: Some(Default::default()),
            new_account_protection: Some(Default::default()),
            ..Default::default()
        },
        created_at_ms: 1000,
        checked_at_ms: 1000,
        old: vec![
            member("acct_old1", 120, 200, 1700),
            member("acct_old2", 100, 200, 3700),
        ],
        new: vec![member("acct_new1", 0, 50, 9500)],
        budget: 50,
        previous_budget: 0,
        stable_since_ms: None,
        observed_successes: 0,
        observed_failures: 0,
        finished: false,
        message: String::new(),
    }
}
#[test]
fn handoff_transfer_budget_controls_old_drain_and_new_capacity_accepts_growth() {
    let mut g = group();
    g.rebalance();
    assert_eq!(g.old.iter().map(|m| m.baseline - m.limit).sum::<u32>(), 50);
    assert_eq!(g.old[0].id, "acct_old1");
    assert_eq!(g.new[0].limit, 50);
    g.new.push(member("acct_new2", 0, 50, 9900));
    g.rebalance();
    assert_eq!(g.new.iter().map(|m| m.limit).collect::<Vec<_>>(), [50, 50]);
    assert_eq!(g.budget, 50);
    assert_eq!(g.old.iter().map(|m| m.baseline - m.limit).sum::<u32>(), 50);
}
#[test]
fn handoff_transfer_growth_capacity_respects_warmup_and_old_floors() {
    let mut g = group();
    g.budget = 100;
    g.new[0].ceiling = 20;
    g.new.push(member("acct_new2", 0, 100, 9900));
    g.rebalance();
    assert_eq!(g.new[0].limit, 20);
    assert_eq!(g.new[1].limit, 100);
    assert!(g.old.iter().all(|m| m.limit >= 50));
    g.new[1].available = false;
    g.rebalance();
    assert_eq!(g.new[1].limit, 0);
    assert_eq!(g.new[0].limit, 20);
}

fn pool_candidate(id: &str, used: u32, consume: bool, limit: Option<u32>) -> AccountCandidate {
    let mut c = candidate(id, used, None);
    c.pool_priority = Some(AccountPoolPriority {
        consume,
        reference_concurrency: 150,
    });
    c.warmup_limit = limit;
    c
}

#[test]
fn handoff_pool_fills_old_retained_capacity_and_balances_growing_load() {
    let mut ctx = context(RotationStrategy::QuotaResetPriority);
    ctx.policy = AccountSelectionPolicy::new(
        RotationStrategy::QuotaResetPriority,
        AccountConcurrency::new(0),
    );
    let mut candidates = vec![
        pool_candidate("acct_old", 0, true, Some(66)),
        pool_candidate("acct_a", 0, false, Some(80)),
        pool_candidate("acct_b", 0, false, Some(80)),
    ];
    // 承接账号重置时间不同也不能让其中一个长期独占新增流量
    candidates[1].signals.quota_reset_at = Some(ctx.now + std::time::Duration::from_secs(60));
    candidates[2].signals.quota_reset_at = Some(ctx.now + std::time::Duration::from_secs(3600));
    for _ in 0..210 {
        let selected = AccountSelector
            .select(&candidates, &ctx)
            .unwrap()
            .candidate()
            .account
            .id()
            .clone();
        candidates
            .iter_mut()
            .find(|c| c.account.id() == &selected)
            .unwrap()
            .signals
            .in_flight += 1;
    }
    assert_eq!(
        candidates
            .iter()
            .map(|c| c.signals.in_flight)
            .collect::<Vec<_>>(),
        [66, 72, 72]
    );
    candidates[0].signals.in_flight -= 1;
    assert_eq!(
        AccountSelector
            .select(&candidates, &ctx)
            .unwrap()
            .candidate()
            .account
            .id()
            .as_str(),
        "acct_old"
    );
    candidates[0].signals.in_flight = 66;
    candidates[1].signals.in_flight = 80;
    candidates[2].signals.in_flight = 80;
    assert!(AccountSelector.select(&candidates, &ctx).is_none());
}

#[test]
fn handoff_pool_balance_survives_group_end_and_does_not_create_pairing() {
    let mut ctx = context(RotationStrategy::QuotaResetPriority);
    ctx.policy = AccountSelectionPolicy::new(
        RotationStrategy::QuotaResetPriority,
        AccountConcurrency::new(0),
    );
    let mut a = pool_candidate("acct_a", 50, false, None);
    let b = pool_candidate("acct_b", 10, false, None);
    a.signals.quota_reset_at = Some(ctx.now + std::time::Duration::from_secs(60));
    a.transfer = Some(AccountTransferCapacity {
        group_id: "active".into(),
        donor: false,
        limit: 80,
        peers: vec!["acct_retiring".into()],
    });
    assert_eq!(
        AccountSelector
            .select(&[a.clone(), b.clone()], &ctx)
            .unwrap()
            .candidate()
            .account
            .id()
            .as_str(),
        "acct_a"
    );
    a.transfer = None;
    assert_eq!(
        AccountSelector
            .select(&[a.clone(), b.clone()], &ctx)
            .unwrap()
            .candidate()
            .account
            .id()
            .as_str(),
        "acct_b"
    );
    // 进入消耗层只改变优先级，不创建 A/B 新老关系或重置独立保护
    a.pool_priority.as_mut().unwrap().consume = true;
    let pool = [a.clone(), b.clone()];
    let selected = AccountSelector.select(&pool, &ctx).unwrap();
    assert_eq!(selected.candidate().account.id().as_str(), "acct_a");
    assert!(selected.candidate().transfer.is_none());
    a.pool_priority = None;
    let mut b = b;
    b.pool_priority = None;
    assert_eq!(
        AccountSelector
            .select(&[a, b], &ctx)
            .unwrap()
            .candidate()
            .account
            .id()
            .as_str(),
        "acct_a"
    );
}

#[test]
fn handoff_pool_respects_quota_priority_protection_weight_scope_and_fixed_account() {
    let mut ctx = context(RotationStrategy::QuotaResetPriority);
    ctx.policy = AccountSelectionPolicy::new(
        RotationStrategy::QuotaResetPriority,
        AccountConcurrency::new(0),
    );
    let mut urgent = pool_candidate("acct_urgent", 40, true, Some(66));
    urgent.signals.quota_reset_at = Some(ctx.now + std::time::Duration::from_secs(60));
    let mut later = pool_candidate("acct_later", 0, true, Some(66));
    later.signals.quota_reset_at = Some(ctx.now + std::time::Duration::from_secs(3600));
    let new = pool_candidate("acct_new", 0, false, Some(50));
    let mut candidates = vec![urgent, later, new];
    assert_eq!(
        AccountSelector
            .select(&candidates, &ctx)
            .unwrap()
            .candidate()
            .account
            .id()
            .as_str(),
        "acct_urgent"
    );
    ctx.preferred_account = Some(candidates[2].account.id().clone());
    assert_eq!(
        AccountSelector
            .select(&candidates, &ctx)
            .unwrap()
            .candidate()
            .account
            .id()
            .as_str(),
        "acct_new"
    );
    ctx.preferred_account = None;
    candidates[2].account = candidates[2]
        .account
        .clone()
        .with_scheduling(None, AccountWeight::new(100).unwrap());
    assert_eq!(
        AccountSelector
            .select(&candidates, &ctx)
            .unwrap()
            .candidate()
            .account
            .id()
            .as_str(),
        "acct_new"
    );
    ctx.excluded_accounts
        .insert(candidates[2].account.id().clone());
    ctx.excluded_accounts
        .insert(candidates[0].account.id().clone());
    assert_eq!(
        AccountSelector
            .select(&candidates, &ctx)
            .unwrap()
            .candidate()
            .account
            .id()
            .as_str(),
        "acct_later"
    );
    ctx.excluded_accounts.clear();
    ctx.reserved_concurrency = 5;
    ctx.policy =
        AccountSelectionPolicy::new(RotationStrategy::RoundRobin, AccountConcurrency::new(0));
    ctx.round_robin_cursor = 1;
    candidates[2].account = candidates[2]
        .account
        .clone()
        .with_scheduling(None, AccountWeight::DEFAULT);
    assert_eq!(
        AccountSelector
            .select(&candidates, &ctx)
            .unwrap()
            .candidate()
            .account
            .id()
            .as_str(),
        "acct_new"
    );
}
#[test]
fn handoff_transfer_stability_and_failure_apply_to_total_not_each_donor() {
    let mut g = group();
    g.new[0].ceiling = 100;
    g.new[0].in_flight = 30;
    g.rebalance();
    g.old.iter_mut().for_each(|m| m.in_flight = m.limit);
    for i in 1..=7 {
        let now = 1000 + i * 5000;
        g.advance(
            now,
            AccountHandoffFeedback {
                successes: i * 4,
                last_success_at_ms: now,
                ..Default::default()
            },
        );
    }
    assert_eq!(g.budget, 80);
    g.advance(
        41000,
        AccountHandoffFeedback {
            successes: 30,
            failures: 1,
            last_failure_at_ms: 41000,
            ..Default::default()
        },
    );
    assert_eq!(g.budget, 50);
    assert!(g.stable_since_ms.is_none());
    g.old.iter_mut().for_each(|m| m.terminal = true);
    g.advance(46000, Default::default());
    assert!(g.finished);
}

#[test]
fn handoff_transfer_waits_for_growth_and_pending_drain_before_next_step() {
    let mut full = group();
    full.new[0].in_flight = full.new[0].ceiling;
    full.rebalance();
    assert_eq!(full.budget, 0);
    assert!(full.old.iter().all(|old| old.limit == old.baseline));
    full.advance(
        6000,
        AccountHandoffFeedback {
            failures: 1,
            ..Default::default()
        },
    );
    assert_eq!(full.budget, 0, "失败回退不能反向新增转移名额");
    let mut g = group();
    g.new[0].ceiling = 100;
    g.rebalance();
    g.old.iter_mut().for_each(|m| m.in_flight = m.limit);
    g.new[0].in_flight = 90;
    for i in 1..=9 {
        let now = 1000 + i * 5000;
        g.advance(
            now,
            AccountHandoffFeedback {
                successes: i * 4,
                last_success_at_ms: now,
                ..Default::default()
            },
        );
    }
    assert_eq!(g.budget, 50, "增长流量已经占用承接容量");
    g.new[0].in_flight = 50;
    g.old[0].in_flight = g.old[0].limit + 30;
    g.advance(
        51000,
        AccountHandoffFeedback {
            successes: 40,
            last_success_at_ms: 51000,
            ..Default::default()
        },
    );
    assert_eq!(g.budget, 50, "上一档转移仍有老号请求在途");
    g.old[0].in_flight = g.old[0].limit;
    g.advance(
        56000,
        AccountHandoffFeedback {
            successes: 44,
            last_success_at_ms: 56000,
            ..Default::default()
        },
    );
    assert_eq!(g.budget, 80);
}
#[test]
fn handoff_transfer_roles_need_maturity_and_real_depletion_not_age_alone() {
    let c = AccountTransferConfig::default();
    assert!(!c.old_eligible(3600, Some(9000), false));
    assert!(c.old_eligible(3600, Some(1700), false));
    assert!(!c.old_eligible(3600, Some(1700), true));
    assert!(!c.old_eligible(20, Some(1700), false));
    assert!(c.old_eligible(6100, None, false));
}
#[test]
fn handoff_transfer_estimate_requires_real_distinct_quota_progress() {
    let points = vec![
        TransferQuotaSample {
            id: "a".into(),
            observed: 1000,
            reset: 1000000,
            remaining: 5000,
        },
        TransferQuotaSample {
            id: "a".into(),
            observed: 61000,
            reset: 1000000,
            remaining: 4000,
        },
    ];
    assert_eq!(TransferQuotaSample::remaining_seconds(&points), Some(240));
    assert_eq!(TransferQuotaSample::remaining_seconds(&points[..1]), None);
}
