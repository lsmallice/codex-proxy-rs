//! 请求可用范围内的交接组角色识别与新增成员登记
use gateway_core::{account::*, provider_ports::*};
use std::{
    collections::BTreeSet,
    time::{SystemTime, UNIX_EPOCH},
};

pub(super) async fn prepare(
    store: &dyn AccountHandoffStore,
    ready: &[&AccountCandidate],
    scheduling: &ProviderSchedulingState,
    scope: &scope::FrozenAccountScope,
    configured: AccountSelectionPolicy,
    now: u64,
) -> Result<bool, ProviderStoreError> {
    let policy = scheduling.handoff_policy();
    let Some(config) = &policy.config.transfer_group else {
        return Ok(false);
    };
    let warm = policy
        .config
        .new_account_protection
        .as_ref()
        .ok_or_else(|| {
            ProviderStoreError::new(ProviderStoreErrorKind::InvalidData, "transfer protection")
        })?;
    let mut group_ids = BTreeSet::new();
    for c in ready {
        if let Some(a) = scope.directory().account(c.account.id()) {
            group_ids.extend(a.group_ids().iter().cloned());
        }
    }
    let legacy_busy: BTreeSet<_> = scheduling
        .handoffs()
        .iter()
        .flat_map(|(id, h)| [id.as_str(), h.new_account_id.as_str()])
        .collect();
    let age = |c: &AccountCandidate| {
        c.account
            .created_at()
            .and_then(|t| t.duration_since(UNIX_EPOCH).ok())
            .map(|d| now.saturating_sub(d.as_millis() as u64) / 1000)
    };
    let fresh_remaining = |c: &AccountCandidate| {
        c.account
            .quota()
            .observed_at()
            .filter(|t| {
                SystemTime::now()
                    .duration_since(*t)
                    .is_ok_and(|v| v.as_secs() <= 300)
            })
            .and(c.signals.quota_remaining_rank)
    };
    let samples = ready
        .iter()
        .take(200)
        .filter_map(|c| {
            Some(TransferQuotaSample {
                id: c.account.id().as_str().into(),
                observed: c
                    .account
                    .quota()
                    .observed_at()?
                    .duration_since(UNIX_EPOCH)
                    .ok()?
                    .as_millis() as u64,
                reset: c
                    .signals
                    .quota_reset_at?
                    .duration_since(UNIX_EPOCH)
                    .ok()?
                    .as_millis() as u64,
                remaining: fresh_remaining(c)?,
            })
        })
        .collect();
    let estimates: std::collections::BTreeMap<_, _> = store
        .transfer_samples(samples)
        .await?
        .into_iter()
        .filter_map(|(id, s)| TransferQuotaSample::remaining_seconds_at(&s, now).map(|v| (id, v)))
        .collect();
    for gid in group_ids {
        let members: Vec<_> = ready
            .iter()
            .copied()
            .filter(|c| {
                scope
                    .directory()
                    .account(c.account.id())
                    .is_some_and(|a| a.group_ids().contains(&gid))
            })
            .collect();
        let existing = scheduling
            .transfers()
            .iter()
            .find(|g| g.state.group_id == gid.as_str() && !g.state.finished);
        let occupied: BTreeSet<_> = scheduling
            .transfers()
            .iter()
            .flat_map(|g| g.state.participant_ids())
            .collect();
        let free = |c: &&AccountCandidate| {
            !legacy_busy.contains(c.account.id().as_str())
                && !occupied.contains(c.account.id().as_str())
        };
        let warming = |c: &AccountCandidate| {
            scheduling
                .warmups()
                .get(c.account.id())
                .is_some_and(Option::is_some)
                || (c.account.created_at().is_some_and(|t| {
                    t.duration_since(UNIX_EPOCH).is_ok_and(|v| {
                        v.as_millis() as u64
                            > policy.protection_after_ms.max(policy.enabled_after_ms)
                    })
                }) && !scheduling.warmups().contains_key(c.account.id()))
        };
        let donors: Vec<_> = members
            .iter()
            .copied()
            .filter(free)
            .filter(|c| {
                age(c).is_some_and(|age| {
                    config.old_eligible(age, fresh_remaining(c), warming(c))
                        || (!warming(c)
                            && age >= u64::from(config.minimum_mature_seconds)
                            && estimates
                                .get(c.account.id().as_str())
                                .is_some_and(|s| *s <= u64::from(config.preparation_seconds)))
                })
            })
            .collect();
        let receivers: Vec<_> = members
            .iter()
            .copied()
            .filter(free)
            .filter(|c| !scheduling.handoff_handled().contains(c.account.id()))
            .filter(|c| {
                let created = c
                    .account
                    .created_at()
                    .and_then(|t| t.duration_since(UNIX_EPOCH).ok())
                    .map(|t| t.as_millis() as u64)
                    .unwrap_or(0);
                created > policy.enabled_after_ms
                    && created <= now
                    && now - created <= 7 * 86400000
                    && fresh_remaining(c)
                        .is_some_and(|r| r > u64::from(config.remaining_percent) * 100)
                    && !donors.iter().any(|d| d.account.id() == c.account.id())
            })
            .collect();
        if existing.is_none() && (donors.is_empty() || receivers.is_empty()) {
            continue;
        }
        if existing.is_some() && donors.is_empty() && receivers.is_empty() {
            continue;
        }
        let member = |c: &AccountCandidate, donor: bool| {
            let configured = c
                .account
                .effective_concurrency(configured.max_concurrent_per_account())
                .limit()
                .map(|v| v.get());
            let ceiling = if donor {
                configured.unwrap_or(u32::MAX)
            } else {
                configured.unwrap_or(warm.release_concurrency).min(
                    scheduling
                        .warmups()
                        .get(c.account.id())
                        .copied()
                        .flatten()
                        .unwrap_or(if warming(c) {
                            warm.initial_concurrency
                        } else {
                            warm.release_concurrency
                        }),
                )
            };
            let time = age(c).map(|v| (u64::from(config.healthy_minutes) * 60).saturating_sub(v));
            let left = estimates
                .get(c.account.id().as_str())
                .copied()
                .map_or(time, |v| Some(time.map_or(v, |t| t.min(v))));
            TransferMember {
                id: c.account.id().as_str().into(),
                name: c.account.name().into(),
                baseline: c
                    .signals
                    .in_flight
                    .max(policy.config.minimum_old_concurrency)
                    .min(ceiling),
                limit: 0,
                available: true,
                terminal: false,
                in_flight: c.signals.in_flight,
                remaining_basis_points: fresh_remaining(c),
                remaining_seconds: left,
                ceiling,
            }
        };
        let mut group = existing
            .map(|g| g.state.clone())
            .unwrap_or_else(|| AccountTransferGroup {
                id: uuid::Uuid::now_v7().to_string(),
                group_id: gid.as_str().into(),
                automatic: true,
                policy_revision: policy.revision,
                revision: 0,
                config: policy.config.clone(),
                created_at_ms: now,
                checked_at_ms: now,
                old: vec![],
                new: vec![],
                budget: policy.config.initial_new_concurrency,
                previous_budget: 0,
                stable_since_ms: None,
                observed_successes: 0,
                observed_failures: 0,
                finished: false,
                message: "准备共享承接名额".into(),
            });
        for c in donors {
            if group.old.len() + group.new.len() < 64 {
                group.old.push(member(c, true));
            }
        }
        let oldest = group
            .old
            .iter()
            .filter_map(|m| {
                members
                    .iter()
                    .find(|c| c.account.id().as_str() == m.id)
                    .and_then(|c| c.account.created_at())
            })
            .max();
        for c in receivers {
            if group.old.len() + group.new.len() < 64
                && oldest.is_none_or(|t| c.account.created_at().is_some_and(|n| n > t))
            {
                group.new.push(member(c, false));
            }
        }
        if group.old.is_empty() || group.new.is_empty() {
            continue;
        }
        if existing.is_some_and(|e| e.state.old == group.old && e.state.new == group.new) {
            continue;
        }
        group.stable_since_ms = None;
        group.observed_successes = existing.map_or(0, |g| g.feedback.successes);
        group.rebalance();
        store
            .save_transfer_group(
                group,
                existing.map_or(0, |g| g.state.revision),
                existing.map_or(0, |g| g.feedback.failures),
            )
            .await?;
        // CAS 冲突后也重读共享分配，不能沿用旧快照继续无限准入
        return Ok(true);
    }
    Ok(false)
}
