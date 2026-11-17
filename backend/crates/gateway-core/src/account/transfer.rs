//! 多账号交接组的角色、总转移预算及按承接能力均摊规则
use super::{AccountHandoffConfig, AccountHandoffFeedback};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct AccountTransferConfig {
    pub preparation_seconds: u32,
    pub remaining_percent: u32,
    pub healthy_minutes: u32,
    pub remaining_minutes: u32,
    pub minimum_mature_seconds: u32,
}
impl Default for AccountTransferConfig {
    fn default() -> Self {
        Self {
            preparation_seconds: 480,
            remaining_percent: 25,
            healthy_minutes: 120,
            remaining_minutes: 20,
            minimum_mature_seconds: 120,
        }
    }
}
impl AccountTransferConfig {
    pub fn validate(&self) -> Result<(), &'static str> {
        if !(30..=7200).contains(&self.preparation_seconds)
            || self.remaining_percent > 100
            || !(1..=1440).contains(&self.healthy_minutes)
            || self.remaining_minutes >= self.healthy_minutes
            || !(30..=3600).contains(&self.minimum_mature_seconds)
        {
            return Err("交接组老号判定参数无效");
        }
        Ok(())
    }
    pub fn old_eligible(&self, age_seconds: u64, remaining: Option<u64>, warming: bool) -> bool {
        !warming
            && age_seconds >= u64::from(self.minimum_mature_seconds)
            && (remaining.is_some_and(|r| r <= u64::from(self.remaining_percent) * 100)
                || age_seconds >= u64::from(self.healthy_minutes - self.remaining_minutes) * 60)
    }
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct StartAccountTransfer {
    pub id: String,
    pub revision: u64,
    pub group_id: String,
    pub old_account_ids: Vec<String>,
    pub new_account_ids: Vec<String>,
    pub config: AccountHandoffConfig,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TransferQuotaSample {
    pub id: String,
    pub observed: u64,
    pub reset: u64,
    pub remaining: u64,
}
impl TransferQuotaSample {
    pub fn is_fresh(&self, now: u64) -> bool {
        self.remaining <= 10000
            && now
                .checked_sub(self.observed)
                .is_some_and(|age| age <= 300000)
            && self.reset > now
    }
    pub fn remaining_seconds_at(points: &[Self], now: u64) -> Option<u64> {
        let last = points.last().filter(|s| s.is_fresh(now))?;
        Self::remaining_seconds(points)
            .map(|seconds| seconds.saturating_sub((now - last.observed) / 1000))
    }
    pub fn remaining_seconds(points: &[Self]) -> Option<u64> {
        let first = points.first()?;
        let last = points.last()?;
        let elapsed = last.observed.checked_sub(first.observed)?;
        let progress = first.remaining.checked_sub(last.remaining)?;
        (elapsed >= 45000 && progress >= 200 && first.reset == last.reset)
            .then(|| last.remaining.saturating_mul(elapsed).div_ceil(progress) / 1000)
    }
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TransferMember {
    pub id: String,
    pub name: String,
    pub baseline: u32,
    pub limit: u32,
    pub available: bool,
    pub terminal: bool,
    pub in_flight: u32,
    pub remaining_basis_points: Option<u64>,
    pub remaining_seconds: Option<u64>,
    pub ceiling: u32,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AccountTransferGroup {
    pub id: String,
    pub group_id: String,
    pub automatic: bool,
    pub policy_revision: u64,
    pub revision: u64,
    pub config: AccountHandoffConfig,
    pub created_at_ms: u64,
    pub checked_at_ms: u64,
    pub old: Vec<TransferMember>,
    pub new: Vec<TransferMember>,
    pub budget: u32,
    pub previous_budget: u32,
    pub stable_since_ms: Option<u64>,
    pub observed_successes: u64,
    pub observed_failures: u64,
    pub finished: bool,
    pub message: String,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AccountTransferSnapshot {
    pub state: AccountTransferGroup,
    pub feedback: AccountHandoffFeedback,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AccountTransferCapacity {
    pub group_id: String,
    pub donor: bool,
    pub limit: u32,
    pub peers: Vec<String>,
}
impl AccountTransferGroup {
    pub fn participant_ids(&self) -> impl Iterator<Item = &str> {
        self.old.iter().chain(&self.new).map(|m| m.id.as_str())
    }
    pub fn validate(&self) -> Result<(), &'static str> {
        self.config.validate()?;
        if self.old.is_empty()
            || self.new.is_empty()
            || self.old.len() + self.new.len() > 64
            || self.config.new_account_protection.is_none()
            || self
                .participant_ids()
                .collect::<std::collections::BTreeSet<_>>()
                .len()
                != self.old.len() + self.new.len()
        {
            return Err("交接组须包含不重复的新老账号并启用新号保护");
        }
        Ok(())
    }
    pub fn capacities(&self) -> BTreeMap<String, AccountTransferCapacity> {
        self.old
            .iter()
            .map(|m| (m, true, &self.new))
            .chain(self.new.iter().map(|m| (m, false, &self.old)))
            .filter(|(m, _, _)| !m.terminal)
            .map(|(m, donor, peers)| {
                (
                    m.id.clone(),
                    AccountTransferCapacity {
                        group_id: self.id.clone(),
                        donor,
                        limit: m.limit,
                        peers: peers
                            .iter()
                            .filter(|p| p.available && !p.terminal)
                            .map(|p| p.id.clone())
                            .collect(),
                    },
                )
            })
            .collect()
    }
    pub fn rebalance(&mut self) {
        let horizon = self
            .config
            .transfer_group
            .as_ref()
            .map_or(480, |c| u64::from(c.preparation_seconds));
        self.old.sort_by(|a, b| {
            a.remaining_seconds
                .filter(|s| *s <= horizon)
                .unwrap_or(u64::MAX)
                .cmp(
                    &b.remaining_seconds
                        .filter(|s| *s <= horizon)
                        .unwrap_or(u64::MAX),
                )
                .then(
                    a.remaining_basis_points
                        .unwrap_or(u64::MAX)
                        .cmp(&b.remaining_basis_points.unwrap_or(u64::MAX)),
                )
                .then(b.in_flight.cmp(&a.in_flight))
                .then(a.id.cmp(&b.id))
        });
        let capacity: u32 = self
            .new
            .iter()
            .filter(|m| m.available && !m.terminal)
            .map(|m| m.ceiling)
            .sum();
        let transferable: u32 = self
            .old
            .iter()
            .filter(|m| m.available || m.terminal)
            .map(|m| {
                if m.terminal {
                    m.baseline
                } else {
                    m.baseline
                        .saturating_sub(self.config.minimum_old_concurrency.min(m.baseline))
                }
            })
            .sum();
        let total = self
            .budget
            .min(capacity)
            .min(transferable.max(self.config.initial_new_concurrency));
        let departed = self
            .old
            .iter()
            .filter(|m| m.terminal)
            .fold(0u32, |n, m| n.saturating_add(m.baseline));
        let mut remaining = total.saturating_sub(departed);
        for old in &mut self.old {
            let taken = if old.available && !old.terminal {
                remaining.min(
                    old.baseline
                        .saturating_sub(self.config.minimum_old_concurrency.min(old.baseline)),
                )
            } else {
                0
            };
            old.limit = old.baseline.saturating_sub(taken).min(old.ceiling);
            remaining -= taken;
        }
        for new in &mut self.new {
            new.limit = 0;
        }
        // 公平水位分配，低保护上限先饱和，其余名额继续分配，单次最多 64 个账号
        remaining = total;
        while remaining > 0 {
            let available: Vec<_> = self
                .new
                .iter()
                .enumerate()
                .filter(|(_, m)| m.available && !m.terminal && m.limit < m.ceiling)
                .map(|(i, _)| i)
                .collect();
            if available.is_empty() {
                break;
            }
            let share = (remaining / available.len() as u32).max(1);
            for i in available {
                let n = remaining
                    .min(share)
                    .min(self.new[i].ceiling - self.new[i].limit);
                self.new[i].limit += n;
                remaining -= n;
                if remaining == 0 {
                    break;
                }
            }
        }
    }
    pub fn advance(&mut self, now: u64, feedback: AccountHandoffFeedback) {
        let gap = now.saturating_sub(self.checked_at_ms);
        self.checked_at_ms = now;
        if self.old.iter().all(|m| m.terminal) || self.new.iter().all(|m| m.terminal) {
            self.finished = true;
            self.message = "交接组已结束，新号独立保护继续生效".into();
            return;
        }
        if feedback.failures != self.observed_failures
            || self.new.iter().any(|m| !m.available && !m.terminal)
        {
            self.budget = self
                .previous_budget
                .max(self.config.initial_new_concurrency);
            self.stable_since_ms = None;
            self.observed_successes = feedback.successes;
            self.observed_failures = feedback.failures;
            self.message = "承接账号暂不可用，暂停转移并恢复上一档".into();
            self.rebalance();
            return;
        }
        if gap > 15000 {
            self.stable_since_ms = None;
            self.observed_successes = feedback.successes;
        }
        if self.new.iter().all(|m| m.in_flight == 0)
            && now.saturating_sub(feedback.last_success_at_ms) > 15000
        {
            self.stable_since_ms = None;
            self.observed_successes = feedback.successes;
        }
        if self.stable_since_ms.is_none()
            && (self.new.iter().any(|m| m.in_flight > 0)
                || feedback.successes > self.observed_successes)
        {
            self.stable_since_ms = Some(now);
            self.observed_successes = feedback.successes;
        }
        if self.stable_since_ms.is_some_and(|since| {
            now.saturating_sub(since) >= u64::from(self.config.stable_seconds) * 1000
                && feedback.last_success_at_ms >= since
        }) && feedback.successes.saturating_sub(self.observed_successes)
            >= u64::from(self.config.minimum_successes)
        {
            let allocated: u32 = self.new.iter().map(|m| m.limit).sum();
            self.previous_budget = allocated;
            self.budget = allocated
                .saturating_add(self.config.decrement)
                .min(self.old.iter().map(|m| m.baseline).sum());
            self.stable_since_ms = Some(now);
            self.observed_successes = feedback.successes;
        }
        self.message = "共享承接名额，保留老号并发消耗余量".into();
        self.rebalance();
    }
}
