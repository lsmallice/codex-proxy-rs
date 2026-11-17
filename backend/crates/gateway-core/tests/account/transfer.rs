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
fn handoff_transfer_budget_is_shared_and_new_member_rebalances_without_reset() {
    let mut g = group();
    g.rebalance();
    assert_eq!(g.old.iter().map(|m| m.baseline - m.limit).sum::<u32>(), 50);
    assert_eq!(g.old[0].id, "acct_old1");
    assert_eq!(g.new[0].limit, 50);
    g.new.push(member("acct_new2", 0, 50, 9900));
    g.rebalance();
    assert_eq!(g.new.iter().map(|m| m.limit).collect::<Vec<_>>(), [25, 25]);
    assert_eq!(g.budget, 50);
}
#[test]
fn handoff_transfer_waterfill_respects_warmup_and_floors() {
    let mut g = group();
    g.budget = 100;
    g.new[0].ceiling = 20;
    g.new.push(member("acct_new2", 0, 100, 9900));
    g.rebalance();
    assert_eq!(g.new[0].limit, 20);
    assert_eq!(g.new[1].limit, 80);
    assert!(g.old.iter().all(|m| m.limit >= 50));
    g.new[1].available = false;
    g.rebalance();
    assert_eq!(g.new[1].limit, 0);
    assert_eq!(g.new[0].limit, 20);
}
#[test]
fn handoff_transfer_stability_and_failure_apply_to_total_not_each_donor() {
    let mut g = group();
    g.new[0].ceiling = 100;
    g.new[0].in_flight = 30;
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
