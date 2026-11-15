//! 交接放量、真实样本、失败回退、老号余量与调度隔离

use super::*;
use gateway_core::account::*;

fn handoff(total: u32) -> AccountHandoff {
    AccountHandoff::start(
        StartAccountHandoff {
            request_id: "request".into(),
            old_account_id: "acct_old".into(),
            new_account_id: "acct_new".into(),
            config: AccountHandoffConfig::default(),
        },
        ["acct_old".into(), "acct_new".into()],
        [total, 0],
        None,
        1000,
    )
    .unwrap()
}

fn observe(state: &mut AccountHandoff, now: u64, successes: u64, failures: u64, active: u32) {
    state.advance(
        AccountHandoffObservation {
            now_ms: now,
            old_available: true,
            new_available: true,
            old_finished: false,
            new_finished: false,
            old_in_flight: 50,
            new_in_flight: active,
        },
        AccountHandoffFeedback {
            successes,
            failures,
            last_success_at_ms: if successes > 0 { now } else { 0 },
            last_failure_at_ms: if failures > 0 { now } else { 0 },
        },
    );
}

#[test]
fn handoff_initial_capacity_tracks_load_but_keeps_old_floor() {
    for (load, expected) in [(40, 50), (100, 50), (120, 70), (150, 100), (200, 150)] {
        assert_eq!(handoff(load).old_limit, expected);
    }
}

#[test]
fn handoff_reduces_by_thirty_and_retains_fifty_until_exhausted() {
    let mut s = handoff(150);
    for second in (5..=35).step_by(5) {
        observe(&mut s, 1000 + second * 1000, second / 5, 0, 50);
    }
    assert_eq!(s.old_limit, 70);
    for second in (40..=65).step_by(5) {
        observe(&mut s, 1000 + second * 1000, second / 5, 0, 80);
    }
    assert_eq!(s.old_limit, 50);
    assert_eq!(s.phase, AccountHandoffPhase::Retaining);
    for second in (70..=200).step_by(5) {
        observe(&mut s, 1000 + second * 1000, second / 5, 0, 100);
    }
    assert_eq!(s.old_limit, 50);
    assert!(s.phase.active());
    s.advance(
        AccountHandoffObservation {
            now_ms: 202000,
            old_available: false,
            new_available: true,
            old_finished: true,
            new_finished: false,
            old_in_flight: 12,
            new_in_flight: 100,
        },
        AccountHandoffFeedback::default(),
    );
    assert_eq!(s.phase, AccountHandoffPhase::Completed);
}

#[test]
fn handoff_terminal_accounts_exit_paused_state_without_requiring_new_quota_or_traffic() {
    for (old_finished, new_finished, expected) in [
        (true, false, AccountHandoffPhase::Completed),
        (false, true, AccountHandoffPhase::Cancelled),
        (true, true, AccountHandoffPhase::Completed),
    ] {
        let mut s = handoff(150);
        observe(&mut s, 6000, 0, 1, 0);
        assert_eq!(s.phase, AccountHandoffPhase::Paused);
        let limit = s.old_limit;
        s.advance(
            AccountHandoffObservation {
                now_ms: 11000,
                old_available: false,
                new_available: false,
                old_finished,
                new_finished,
                old_in_flight: 3,
                new_in_flight: 2,
            },
            AccountHandoffFeedback::default(),
        );
        assert_eq!(s.phase, expected);
        assert!(!s.phase.active());
        assert_eq!(s.old_limit, limit);
        assert_eq!((s.old_in_flight, s.new_in_flight), (3, 2));
        assert_eq!(s.stable_since_ms, None);
    }
}

#[test]
fn handoff_temporary_unavailability_resumes_with_a_fresh_observation_window() {
    for old_unavailable in [true, false] {
        let mut s = handoff(150);
        observe(&mut s, 6000, 1, 0, 50);
        s.advance(
            AccountHandoffObservation {
                now_ms: 11000,
                old_available: !old_unavailable,
                new_available: old_unavailable,
                old_finished: false,
                new_finished: false,
                old_in_flight: 0,
                new_in_flight: 0,
            },
            AccountHandoffFeedback {
                successes: 10,
                last_success_at_ms: 10000,
                ..Default::default()
            },
        );
        assert_eq!(s.phase, AccountHandoffPhase::Paused);
        assert!(s.phase.active());
        assert_eq!(s.old_limit, 150);
        observe(&mut s, 16000, 11, 0, 50);
        assert_eq!(s.phase, AccountHandoffPhase::Observing);
        assert_eq!(s.stable_since_ms, Some(16000));
        for now in (21000..=41000).step_by(5000) {
            observe(&mut s, now, 20, 0, 50);
        }
        assert_eq!(s.old_limit, 150);
        observe(&mut s, 46000, 21, 0, 50);
        assert_eq!(s.old_limit, 120);
    }
}

#[test]
fn handoff_requires_real_successes_and_does_not_advance_on_idle_time() {
    let mut s = handoff(150);
    for second in (5..=300).step_by(5) {
        observe(&mut s, 1000 + second * 1000, 0, 0, 0);
    }
    assert_eq!(s.old_limit, 100);
    assert_eq!(s.stable_since_ms, None);
    for second in (305..=400).step_by(5) {
        observe(&mut s, 1000 + second * 1000, 0, 0, 50);
    }
    assert_eq!(s.old_limit, 100);
}

#[test]
fn handoff_failure_restores_prior_step_and_requires_a_new_window() {
    let mut s = handoff(150);
    for second in (5..=35).step_by(5) {
        observe(&mut s, 1000 + second * 1000, second / 5, 0, 50);
    }
    assert_eq!(s.old_limit, 70);
    observe(&mut s, 41000, 8, 1, 0);
    assert_eq!(s.old_limit, 100);
    assert_eq!(s.phase, AccountHandoffPhase::Paused);
    observe(&mut s, 46000, 9, 1, 50);
    observe(&mut s, 51000, 100, 1, 50);
    assert_eq!(s.old_limit, 100);
}

#[test]
fn handoff_first_stage_failure_can_restore_original_capacity() {
    let mut s = handoff(150);
    observe(&mut s, 6000, 0, 1, 0);
    assert_eq!(s.old_limit, 150);
}

#[test]
fn handoff_observation_gap_does_not_count_as_stable_time() {
    let mut s = handoff(150);
    observe(&mut s, 6000, 1, 0, 50);
    observe(&mut s, 66000, 20, 0, 50);
    assert_eq!(s.old_limit, 100);
    assert_eq!(s.stable_since_ms, Some(66000));
}

#[test]
fn handoff_rejects_invalid_config_and_preserves_lower_manual_ceiling() {
    let config = AccountHandoffConfig {
        minimum_old_concurrency: 0,
        ..Default::default()
    };
    assert!(config.validate().is_err());
    assert!(
        AccountHandoff::start(
            StartAccountHandoff {
                request_id: "x".into(),
                old_account_id: "acct_old".into(),
                new_account_id: "acct_new".into(),
                config: AccountHandoffConfig::default()
            },
            ["o".into(), "n".into()],
            [30, 0],
            Some(30),
            0
        )
        .is_err()
    );
}

#[test]
fn handoff_prefers_old_capacity_but_does_not_override_affinity_or_weights() {
    let mut old = candidate("acct_old", 49, None);
    old.handoff = Some(AccountHandoffCapacity {
        id: "h".into(),
        new_account_id: "acct_new".into(),
        old_limit: 50,
    });
    let new = candidate("acct_new", 100, None);
    let mut ctx = context(RotationStrategy::QuotaResetPriority);
    ctx.policy = AccountSelectionPolicy::new(
        RotationStrategy::QuotaResetPriority,
        AccountConcurrency::new(0),
    );
    let pair = [old.clone(), new.clone()];
    assert_eq!(
        AccountSelector
            .select(&pair, &ctx)
            .unwrap()
            .candidate()
            .account
            .id()
            .as_str(),
        "acct_old"
    );
    assert_eq!(ctx.concurrency_limit(&pair[0]).get(), 50);
    assert_eq!(ctx.concurrency_limit(&pair[1]).get(), 0);
    ctx.excluded_accounts
        .insert(ProviderAccountId::new("acct_new").unwrap());
    assert_eq!(ctx.concurrency_limit(&pair[0]).get(), 0);
    assert!(ctx.active_handoff(&pair[0]).is_none());
    ctx.excluded_accounts.clear();
    ctx.preferred_account = Some(ProviderAccountId::new("acct_new").unwrap());
    assert_eq!(
        AccountSelector
            .select(&pair, &ctx)
            .unwrap()
            .candidate()
            .account
            .id()
            .as_str(),
        "acct_new"
    );
    ctx.preferred_account = None;
    old.signals.in_flight = 50;
    let pair = [old, new];
    assert_eq!(
        AccountSelector
            .select(&pair, &ctx)
            .unwrap()
            .candidate()
            .account
            .id()
            .as_str(),
        "acct_new"
    );
    let mut old = pair[0].clone();
    old.signals.in_flight = 0;
    old.account = old
        .account
        .with_scheduling(None, AccountWeight::new(50).unwrap());
    let pair = [old, weighted_candidate("acct_new", 100, 0)];
    assert_eq!(
        AccountSelector
            .select(&pair, &ctx)
            .unwrap()
            .candidate()
            .account
            .id()
            .as_str(),
        "acct_new"
    );
}
