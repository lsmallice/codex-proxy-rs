//! 在首次业务准入前为新导入订阅账号建立共享交接，不依赖具体导入入口

use gateway_core::{
    account::*,
    provider_ports::{
        AccountHandoffStore, ProviderSchedulingState, ProviderStoreError, ProviderStoreErrorKind,
    },
};
use std::{collections::BTreeSet, time::SystemTime};

pub(super) async fn prepare(
    store: &dyn AccountHandoffStore,
    candidates: &[AccountCandidate],
    scheduling: &ProviderSchedulingState,
    scope: Option<&scope::FrozenAccountScope>,
    configured: AccountSelectionPolicy,
    excluded: &BTreeSet<ProviderAccountId>,
) -> Result<bool, ProviderStoreError> {
    let policy = scheduling.handoff_policy();
    let Some(scope) = scope.filter(|_| policy.enabled) else {
        return Ok(false);
    };
    let now = SystemTime::now();
    let millis = |t: SystemTime| {
        t.duration_since(SystemTime::UNIX_EPOCH)
            .ok()
            .and_then(|d| u64::try_from(d.as_millis()).ok())
    };
    let Some(now_ms) = millis(now) else {
        return Ok(false);
    };
    let available = |c: &&AccountCandidate| {
        c.account.authentication_kind() == super::CODEX_AUTHENTICATION_KIND_OAUTH
            && c.account.plan_type().is_some_and(|p| {
                !p.is_empty()
                    && !p.eq_ignore_ascii_case("free")
                    && !p.eq_ignore_ascii_case("unknown")
            })
            && c.account.credential_state() == CredentialState::Ready
            && c.account.quota().access() == QuotaAccessState::Allowed
            && c.account.status_projection(now, c.signals.cooldown).status == AccountStatus::Normal
            && scope.allows(c.account.id())
            && !excluded.contains(c.account.id())
    };
    let mut ready = candidates.iter().filter(available).collect::<Vec<_>>();
    ready.sort_by_key(|c| c.account.created_at());
    let busy = scheduling
        .handoffs()
        .iter()
        .flat_map(|(id, h)| [id.as_str(), h.new_account_id.as_str()])
        .collect::<BTreeSet<_>>();
    for new in ready.iter().copied().filter(|c| {
        c.account.created_at().and_then(millis).is_some_and(|at| {
            at > policy.enabled_after_ms && at <= now_ms && now_ms - at <= 7 * 86400 * 1000
        }) && !scheduling.handoff_handled().contains(c.account.id())
            && !busy.contains(c.account.id().as_str())
    }) {
        let Some(new_groups) = scope
            .directory()
            .account(new.account.id())
            .map(|a| a.group_ids())
        else {
            continue;
        };
        let old = ready
            .iter()
            .copied()
            .filter(|c| {
                c.account
                    .created_at()
                    .is_some_and(|at| new.account.created_at().is_some_and(|new_at| at < new_at))
                    && !busy.contains(c.account.id().as_str())
                    && c.account
                        .effective_concurrency(configured.max_concurrent_per_account())
                        .limit()
                        .is_none_or(|limit| limit.get() >= policy.config.minimum_old_concurrency)
                    && scope
                        .directory()
                        .account(c.account.id())
                        .is_some_and(|a| !a.group_ids().is_disjoint(new_groups))
            })
            .max_by_key(|c| c.signals.in_flight);
        let Some(old) = old else {
            continue;
        };
        let mut state = AccountHandoff::start(
            StartAccountHandoff {
                request_id: uuid::Uuid::now_v7().to_string(),
                old_account_id: old.account.id().as_str().into(),
                new_account_id: new.account.id().as_str().into(),
                config: policy.config.clone(),
            },
            [old.account.name().into(), new.account.name().into()],
            [old.signals.in_flight, new.signals.in_flight],
            old.account
                .effective_concurrency(configured.max_concurrent_per_account())
                .limit()
                .map(|n| n.get()),
            now_ms,
        )
        .map_err(|_| {
            ProviderStoreError::new(
                ProviderStoreErrorKind::InvalidData,
                "automatic account handoff",
            )
        })?;
        state.automatic = true;
        state.automatic_policy_revision = policy.revision;
        match store.start(state).await {
            Ok(_) => return Ok(true),
            Err(error) if error.kind() == ProviderStoreErrorKind::Conflict => return Ok(true),
            Err(error) => return Err(error),
        }
    }
    Ok(false)
}
