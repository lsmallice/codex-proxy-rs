//! 新导入账号的独立并发保护与真实流量升档规则
use super::AccountHandoffFeedback;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct AccountWarmupConfig {
    pub initial_concurrency: u32,
    pub increment: u32,
    pub release_concurrency: u32,
    pub stable_seconds: u32,
    pub minimum_successes: u32,
}
impl Default for AccountWarmupConfig {
    fn default() -> Self {
        Self {
            initial_concurrency: 50,
            increment: 30,
            release_concurrency: 150,
            stable_seconds: 60,
            minimum_successes: 20,
        }
    }
}
impl AccountWarmupConfig {
    pub fn validate(&self) -> Result<(), &'static str> {
        if !(1..=10000).contains(&self.initial_concurrency)
            || !(1..=1000).contains(&self.increment)
            || !(self.initial_concurrency..=10000).contains(&self.release_concurrency)
            || !(30..=3600).contains(&self.stable_seconds)
            || !(1..=10000).contains(&self.minimum_successes)
        {
            return Err("新账号保护参数超出允许范围");
        }
        Ok(())
    }
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AccountWarmup {
    pub account_id: String,
    pub account_name: String,
    pub policy_revision: u64,
    pub config: AccountWarmupConfig,
    pub revision: u64,
    pub started_at_ms: u64,
    pub checked_at_ms: u64,
    pub stable_since_ms: Option<u64>,
    pub limit: u32,
    pub previous_limit: u32,
    pub in_flight: u32,
    pub observed_successes: u64,
    pub observed_failures: u64,
    pub finished: bool,
    pub message: String,
}
impl AccountWarmup {
    pub fn new(
        account_id: String,
        account_name: String,
        config: AccountWarmupConfig,
        revision: u64,
        now: u64,
    ) -> Self {
        Self {
            account_id,
            account_name,
            policy_revision: revision,
            limit: config.initial_concurrency,
            previous_limit: config.initial_concurrency,
            config,
            revision: 1,
            started_at_ms: now,
            checked_at_ms: now,
            stable_since_ms: None,
            in_flight: 0,
            observed_successes: 0,
            observed_failures: 0,
            finished: false,
            message: "等待新账号实际流量".into(),
        }
    }
    pub fn advance(
        &mut self,
        now: u64,
        available: bool,
        terminal: bool,
        in_flight: u32,
        configured: Option<u32>,
        feedback: AccountHandoffFeedback,
    ) {
        if self.finished {
            return;
        }
        let gap = now.saturating_sub(self.checked_at_ms);
        self.checked_at_ms = now;
        self.in_flight = in_flight;
        if terminal {
            self.finished = true;
            self.message = "账号退出可用池，结束新号保护".into();
            return;
        }
        if feedback.failures != self.observed_failures || !available {
            self.limit = self
                .previous_limit
                .min(self.limit)
                .max(self.config.initial_concurrency);
            self.previous_limit = self
                .limit
                .saturating_sub(self.config.increment)
                .max(self.config.initial_concurrency);
            self.stable_since_ms = None;
            self.observed_successes = feedback.successes;
            self.observed_failures = feedback.failures;
            self.message = "账号失败或暂不可用，保留保护并重新观察".into();
            return;
        }
        if gap > 15000 {
            self.stable_since_ms = None;
            self.observed_successes = feedback.successes;
        }
        let target = configured
            .filter(|v| *v > 0)
            .map_or(self.config.release_concurrency, |v| {
                v.min(self.config.release_concurrency)
            });
        let Some(since) = self.stable_since_ms else {
            if in_flight > 0 || feedback.successes > self.observed_successes {
                self.stable_since_ms = Some(now);
                self.observed_successes = feedback.successes;
            }
            return;
        };
        if in_flight == 0 && now.saturating_sub(feedback.last_success_at_ms) > 15000 {
            self.stable_since_ms = None;
            self.observed_successes = feedback.successes;
            return;
        }
        if now.saturating_sub(since) < u64::from(self.config.stable_seconds) * 1000
            || feedback.successes.saturating_sub(self.observed_successes)
                < u64::from(self.config.minimum_successes)
            || feedback.last_success_at_ms < since
        {
            return;
        }
        if self.limit >= target {
            self.finished = true;
            self.message = "新号保护完成，恢复账号原始并发设置".into();
        } else {
            self.previous_limit = self.limit;
            self.limit = self.limit.saturating_add(self.config.increment).min(target);
            self.stable_since_ms = Some(now);
            self.observed_successes = feedback.successes;
            self.message = "稳定流量验证通过，提高新号保护上限".into();
        }
    }
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AccountWarmupSnapshot {
    pub state: AccountWarmup,
    pub feedback: AccountHandoffFeedback,
}
