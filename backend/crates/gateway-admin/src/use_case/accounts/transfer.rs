//! 多账号交接的管理员命令、成员校验和共享预算观测
use super::handoff::{store_error, terminal_reason};
use super::*;
use gateway_core::account::*;

fn quota_sample(id: &str, quota: &ProviderQuota, now: u64) -> Option<TransferQuotaSample> {
    let observed = u64::try_from(quota.observed_at?.timestamp_millis()).ok()?;
    let windows: Vec<_> = quota
        .windows
        .iter()
        .filter(|w| {
            w.reset_at
                .is_some_and(|reset| reset.timestamp_millis() > now as i64)
        })
        .collect();
    let remaining = windows
        .iter()
        .filter_map(|w| {
            w.used_percent
                .filter(|p| p.is_finite() && (0.0..=100.0).contains(p))
                .map(|used| ((100.0 - used) * 100.0).round() as u64)
        })
        .min()?;
    let reset = windows
        .iter()
        .filter_map(|w| w.reset_at)
        .map(|reset| reset.timestamp_millis() as u64)
        .min()?;
    let sample = TransferQuotaSample {
        id: id.into(),
        observed,
        reset,
        remaining,
    };
    sample.is_fresh(now).then_some(sample)
}

impl DefaultAccountsService {
    pub(super) async fn transfer_list(&self) -> Result<Vec<AccountTransferSnapshot>, AdminError> {
        self.handoff_store()?
            .transfer_groups()
            .await
            .map_err(store_error)
    }
    pub(super) async fn transfer_save(
        &self,
        c: StartAccountTransfer,
    ) -> Result<AccountTransferSnapshot, AdminError> {
        c.config.validate().map_err(AdminError::invalid)?;
        uuid::Uuid::parse_str(&c.id).map_err(|_| AdminError::invalid("交接组标识无效"))?;
        let previous = self
            .transfer_list()
            .await?
            .into_iter()
            .find(|g| g.state.id == c.id);
        if previous.as_ref().map_or(0, |g| g.state.revision) != c.revision {
            return Err(AdminError::conflict("交接组已变化，请刷新"));
        }
        if c.old_account_ids.is_empty()
            || c.new_account_ids.is_empty()
            || c.old_account_ids.len() + c.new_account_ids.len() > 64
        {
            return Err(AdminError::invalid("须选择 1–64 个新老账号"));
        }
        if let Some(p) = &previous
            && (p.state.group_id != c.group_id
                || p.state.config != c.config
                || p.state
                    .old
                    .iter()
                    .any(|m| !c.old_account_ids.contains(&m.id))
                || p.state
                    .new
                    .iter()
                    .any(|m| !c.new_account_ids.contains(&m.id)))
        {
            return Err(AdminError::invalid(
                "追加成员不能移除原账号、改变角色或重置参数",
            ));
        }
        let now = Utc::now().timestamp_millis() as u64;
        let mut group = previous
            .as_ref()
            .map(|p| p.state.clone())
            .unwrap_or_else(|| AccountTransferGroup {
                id: c.id.clone(),
                group_id: c.group_id.clone(),
                automatic: false,
                policy_revision: 0,
                revision: 0,
                config: c.config.clone(),
                created_at_ms: now,
                checked_at_ms: now,
                old: vec![],
                new: vec![],
                budget: c.config.initial_new_concurrency,
                previous_budget: 0,
                stable_since_ms: None,
                observed_successes: 0,
                observed_failures: 0,
                finished: false,
                message: "手动共享交接组".into(),
            });
        let protection = c
            .config
            .new_account_protection
            .as_ref()
            .ok_or_else(|| AdminError::invalid("交接组须启用新号保护"))?;
        let warm = self.handoff_store()?.warmups().await.map_err(store_error)?;
        let mut seen = std::collections::BTreeSet::new();
        for (ids, donor) in [(&c.old_account_ids, true), (&c.new_account_ids, false)] {
            for id in ids {
                if !seen.insert(id) {
                    return Err(AdminError::invalid("账号角色重复"));
                }
                if group.participant_ids().any(|p| p == id) {
                    continue;
                }
                let parsed = ProviderAccountId::new(id.clone())
                    .map_err(|_| AdminError::invalid("账号 ID 无效"))?;
                let a = self.load_account(&parsed).await?;
                if a.projection.status != AccountStatus::Normal
                    || a.account.provider_kind.as_str() != "openai"
                    || a.account.authentication_kind != "oauth"
                    || !a
                        .account
                        .groups
                        .iter()
                        .any(|g| g.enabled && g.id.as_str() == c.group_id)
                    || a.account
                        .plan_type
                        .as_deref()
                        .is_none_or(|p| p.is_empty() || p.eq_ignore_ascii_case("free"))
                {
                    return Err(AdminError::invalid("成员须为同一启用分组的正常订阅账号"));
                }
                if donor && warm.iter().any(|w| w.state.account_id == *id) {
                    return Err(AdminError::invalid("受保护新号不能作为老号"));
                }
                let used = u32::try_from(
                    a.capacity
                        .used_slots
                        .ok_or_else(|| AdminError::unavailable("实时并发不可用"))?,
                )
                .unwrap_or(u32::MAX);
                let ceiling = a
                    .capacity
                    .total_slots
                    .map(|v| u32::try_from(v).unwrap_or(u32::MAX))
                    .unwrap_or(u32::MAX)
                    .min(if donor {
                        u32::MAX
                    } else {
                        protection.initial_concurrency
                    });
                let m = TransferMember {
                    id: id.clone(),
                    name: a.account.name,
                    baseline: used.max(c.config.minimum_old_concurrency).min(ceiling),
                    limit: 0,
                    available: true,
                    terminal: false,
                    in_flight: used,
                    remaining_basis_points: None,
                    remaining_seconds: None,
                    ceiling,
                };
                if donor {
                    group.old.push(m)
                } else {
                    group.new.push(m)
                }
            }
        }
        group.validate().map_err(AdminError::invalid)?;
        if let Some(previous) = previous
            .as_ref()
            .filter(|p| p.state.old == group.old && p.state.new == group.new)
        {
            return Ok(previous.clone());
        }
        group.stable_since_ms = None;
        group.observed_successes = previous.as_ref().map_or(0, |p| p.feedback.successes);
        group.rebalance();
        if !self
            .handoff_store()?
            .save_transfer_group(
                group,
                c.revision,
                previous.as_ref().map_or(0, |p| p.feedback.failures),
            )
            .await
            .map_err(store_error)?
        {
            return Err(AdminError::conflict("成员被占用或交接组已变化"));
        }
        self.transfer_list()
            .await?
            .into_iter()
            .find(|g| g.state.id == c.id)
            .ok_or_else(|| AdminError::unavailable("交接组结果待刷新"))
    }
    pub(super) async fn transfer_cancel(
        &self,
        id: String,
        revision: u64,
    ) -> Result<(), AdminError> {
        let Some(mut item) = self
            .transfer_list()
            .await?
            .into_iter()
            .find(|g| g.state.id == id)
        else {
            return Ok(());
        };
        if item.state.revision != revision {
            return Err(AdminError::conflict("交接组已变化，请刷新"));
        }
        item.state.finished = true;
        if !self
            .handoff_store()?
            .save_transfer_group(item.state, revision, item.feedback.failures)
            .await
            .map_err(store_error)?
        {
            return Err(AdminError::conflict("交接组已变化，请刷新"));
        }
        Ok(())
    }
    pub(super) async fn transfer_reconcile(&self) -> Result<(), AdminError> {
        let Some(store) = &self.handoffs else {
            return Ok(());
        };
        let warm = store.warmups().await.map_err(store_error)?;
        for mut item in store.transfer_groups().await.map_err(store_error)? {
            let now = Utc::now().timestamp_millis() as u64;
            let rev = item.state.revision;
            let ids: Vec<_> = item.state.participant_ids().map(str::to_owned).collect();
            let mut observed = Vec::new();
            let runtime = self
                .account_runtime
                .account_runtime(&ids)
                .await
                .map_err(|e| map_store_error(e, "transfer runtime"))?;
            for (members, donor) in [(&mut item.state.old, true), (&mut item.state.new, false)] {
                for m in members {
                    if m.terminal {
                        continue;
                    }
                    let a = self
                        .accounts
                        .load_account(&m.id, runtime.clone())
                        .await
                        .map_err(|e| map_store_error(e, "transfer account"))?;
                    m.terminal = terminal_reason(a.as_ref()).is_some();
                    let Some(a) = a else {
                        m.available = false;
                        continue;
                    };
                    if !a
                        .account
                        .groups
                        .iter()
                        .any(|g| g.enabled && g.id.as_str() == item.state.group_id)
                        || a.account.authentication_kind != "oauth"
                        || a.account.provider_kind.as_str() != "openai"
                    {
                        m.terminal = true;
                    }
                    m.available = a.projection.status == AccountStatus::Normal
                        && a.account
                            .groups
                            .iter()
                            .any(|g| g.enabled && g.id.as_str() == item.state.group_id);
                    let Some(used) = a.capacity.used_slots else {
                        m.available = false;
                        continue;
                    };
                    m.in_flight = u32::try_from(used).unwrap_or(u32::MAX);
                    m.ceiling = a
                        .capacity
                        .total_slots
                        .map(|v| u32::try_from(v).unwrap_or(u32::MAX))
                        .unwrap_or(u32::MAX)
                        .min(if donor {
                            u32::MAX
                        } else {
                            warm.iter().find(|w| w.state.account_id == m.id).map_or(
                                item.state
                                    .config
                                    .new_account_protection
                                    .as_ref()
                                    .unwrap()
                                    .release_concurrency,
                                |w| w.state.limit,
                            )
                        });
                    observed.push(a);
                }
            }
            // 只读 Provider 留存额度，不发起上游刷新；缺失或陈旧观测不沿用旧预测
            let quotas = self.read_quotas(&observed, &Default::default()).await;
            let facts: BTreeMap<_, _> = observed
                .iter()
                .zip(&quotas)
                .map(|(a, q)| (a.account.id.clone(), quota_sample(&a.account.id, q, now)))
                .collect();
            let samples = store
                .transfer_samples(facts.values().flatten().cloned().collect())
                .await
                .map_err(store_error)?;
            for m in item.state.old.iter_mut().chain(&mut item.state.new) {
                let sample = facts.get(&m.id).and_then(Option::as_ref);
                m.remaining_basis_points = sample.map(|s| s.remaining);
                let time = observed
                    .iter()
                    .find(|a| a.account.id == m.id)
                    .and_then(|a| {
                        let created =
                            u64::try_from(a.account.created_at.timestamp_millis()).ok()?;
                        let age = now.checked_sub(created)? / 1000;
                        item.state
                            .config
                            .transfer_group
                            .as_ref()
                            .map(|c| (u64::from(c.healthy_minutes) * 60).saturating_sub(age))
                    });
                let estimated = sample
                    .and_then(|_| samples.get(&m.id))
                    .and_then(|s| TransferQuotaSample::remaining_seconds_at(s, now));
                m.remaining_seconds = match (time, estimated) {
                    (Some(t), Some(e)) => Some(t.min(e)),
                    (t, e) => t.or(e),
                };
            }
            item.state.advance(now, item.feedback);
            store
                .save_transfer_group(item.state, rev, item.feedback.failures)
                .await
                .map_err(store_error)?;
        }
        Ok(())
    }
}
