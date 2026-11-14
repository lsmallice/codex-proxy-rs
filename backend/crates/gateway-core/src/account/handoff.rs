//! 新老账号交接策略、运行状态与共享协调端口

use serde::{Deserialize, Serialize};

pub const HANDOFF_CHECK_SECONDS: u64 = 5;
pub const MAX_ACTIVE_HANDOFFS: usize = 128;
pub const HANDOFF_HISTORY_RETENTION_SECONDS: u64 = 7 * 86400;
pub const MAX_HANDOFF_HISTORY_RECORDS: usize = 10000;

#[derive(Debug, Clone, PartialEq, Eq, Default, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct AccountHandoffPolicy {
    pub enabled: bool,
    pub enabled_after_ms: u64,
    pub revision: u64,
    pub config: AccountHandoffConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct StartAccountHandoff {
    pub request_id: String,
    pub old_account_id: String,
    pub new_account_id: String,
    #[serde(default)]
    pub config: AccountHandoffConfig,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct AccountHandoffConfig {
    pub initial_new_concurrency: u32,
    pub minimum_old_concurrency: u32,
    pub decrement: u32,
    pub stable_seconds: u32,
    pub minimum_successes: u32,
}

impl Default for AccountHandoffConfig {
    fn default() -> Self {
        Self {
            initial_new_concurrency: 50,
            minimum_old_concurrency: 50,
            decrement: 30,
            stable_seconds: 30,
            minimum_successes: 3,
        }
    }
}

impl AccountHandoffConfig {
    pub fn validate(&self) -> Result<(), &'static str> {
        if !(1..=10000).contains(&self.initial_new_concurrency)
            || !(1..=10000).contains(&self.minimum_old_concurrency)
            || !(1..=1000).contains(&self.decrement)
            || !(30..=3600).contains(&self.stable_seconds)
            || !(1..=10000).contains(&self.minimum_successes)
        {
            return Err("账号交接参数超出允许范围");
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AccountHandoffPhase {
    Observing,
    Paused,
    Retaining,
    Completed,
    Cancelled,
}

impl AccountHandoffPhase {
    pub const fn active(self) -> bool {
        !matches!(self, Self::Completed | Self::Cancelled)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AccountHandoff {
    pub automatic: bool,
    pub automatic_policy_revision: u64,
    pub id: String,
    pub old_account_id: String,
    pub new_account_id: String,
    pub old_account_name: String,
    pub new_account_name: String,
    pub config: AccountHandoffConfig,
    pub phase: AccountHandoffPhase,
    pub revision: u64,
    pub created_at_ms: u64,
    #[serde(default)]
    pub finished_at_ms: Option<u64>,
    pub checked_at_ms: u64,
    pub stable_since_ms: Option<u64>,
    pub old_limit: u32,
    pub previous_old_limit: u32,
    pub initial_old_limit: u32,
    pub old_in_flight: u32,
    pub new_in_flight: u32,
    pub observed_successes: u64,
    pub observed_failures: u64,
    pub last_success_at_ms: u64,
    pub message: String,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AccountHandoffFeedback {
    pub successes: u64,
    pub failures: u64,
    pub last_success_at_ms: u64,
    pub last_failure_at_ms: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AccountHandoffSnapshot {
    pub state: AccountHandoff,
    pub feedback: AccountHandoffFeedback,
}

#[derive(Debug, Clone)]
pub struct AccountHandoffPage {
    pub items: Vec<AccountHandoffSnapshot>,
    pub total: u64,
    pub page: u32,
    pub page_size: u32,
    pub active_total: u64,
    pub occupied_account_ids: Vec<String>,
}

pub fn validate_handoff_page(page: u32, page_size: u32) -> Result<(), &'static str> {
    if page == 0 || !(1..=100).contains(&page_size) {
        return Err("交接页码须大于零，每页条数须为 1–100");
    }
    Ok(())
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AccountHandoffCapacity {
    pub id: String,
    pub new_account_id: String,
    pub old_limit: u32,
}

#[derive(Debug, Clone, Copy)]
pub struct AccountHandoffObservation {
    pub now_ms: u64,
    pub old_available: bool,
    pub new_available: bool,
    pub old_finished: bool,
    pub old_in_flight: u32,
    pub new_in_flight: u32,
}

impl AccountHandoff {
    pub fn start(
        command: StartAccountHandoff,
        names: [String; 2],
        used: [u32; 2],
        configured_limit: Option<u32>,
        now_ms: u64,
    ) -> Result<Self, &'static str> {
        command.config.validate()?;
        let floor = command.config.minimum_old_concurrency;
        if configured_limit.is_some_and(|limit| limit < floor) {
            return Err("老账号已有并发上限低于交接保留值");
        }
        let limit = used[0]
            .saturating_add(used[1])
            .saturating_sub(command.config.initial_new_concurrency)
            .max(floor);
        let limit = configured_limit.map_or(limit, |n| limit.min(n));
        let baseline = configured_limit.map_or(used[0].max(limit), |n| used[0].max(limit).min(n));
        let [old_account_name, new_account_name] = names;
        Ok(Self {
            automatic: false,
            automatic_policy_revision: 0,
            id: command.request_id,
            old_account_id: command.old_account_id,
            new_account_id: command.new_account_id,
            old_account_name,
            new_account_name,
            config: command.config,
            phase: AccountHandoffPhase::Observing,
            revision: 1,
            created_at_ms: now_ms,
            finished_at_ms: None,
            checked_at_ms: now_ms,
            stable_since_ms: None,
            old_limit: limit,
            previous_old_limit: baseline,
            initial_old_limit: baseline,
            old_in_flight: used[0],
            new_in_flight: used[1],
            observed_successes: 0,
            observed_failures: 0,
            last_success_at_ms: 0,
            message: "等待新账号实际流量".into(),
        })
    }
    /// 只改变临时承接上限，账号原始并发、额度与启停状态由原 owner 维护
    pub fn advance(
        &mut self,
        observation: AccountHandoffObservation,
        feedback: AccountHandoffFeedback,
    ) {
        if !self.phase.active() {
            return;
        }
        let gap = observation.now_ms.saturating_sub(self.checked_at_ms);
        self.checked_at_ms = observation.now_ms;
        self.old_in_flight = observation.old_in_flight;
        self.new_in_flight = observation.new_in_flight;
        self.last_success_at_ms = feedback.last_success_at_ms;
        if observation.old_finished {
            self.phase = AccountHandoffPhase::Completed;
            self.stable_since_ms = None;
            self.message = "老账号已耗尽或退出可用池，交接结束".into();
            return;
        }
        let failed = feedback.failures != self.observed_failures;
        if failed || !observation.old_available || !observation.new_available {
            self.old_limit = self.old_limit.max(self.previous_old_limit);
            self.phase = AccountHandoffPhase::Paused;
            self.stable_since_ms = None;
            self.observed_successes = feedback.successes;
            self.observed_failures = feedback.failures;
            self.message = if failed {
                "新账号上游失败，暂停交接并恢复上一档".into()
            } else {
                "账号状态未就绪，暂停交接".into()
            };
            return;
        }
        // 重启、观测中断和无流量不能把时间空白算作稳定运行
        if gap > HANDOFF_CHECK_SECONDS * 3000 {
            self.stable_since_ms = None;
            self.observed_successes = feedback.successes;
        }
        if self.old_limit <= self.config.minimum_old_concurrency {
            self.phase = AccountHandoffPhase::Retaining;
            self.message = "保留老账号并发，继续消耗剩余额度".into();
            return;
        }
        self.phase = AccountHandoffPhase::Observing;
        let Some(since) = self.stable_since_ms else {
            if observation.new_in_flight > 0 || feedback.successes > self.observed_successes {
                self.stable_since_ms = Some(observation.now_ms);
                self.observed_successes = feedback.successes;
            }
            self.message = "等待新账号实际流量".into();
            return;
        };
        if observation.new_in_flight == 0
            && observation
                .now_ms
                .saturating_sub(feedback.last_success_at_ms)
                > HANDOFF_CHECK_SECONDS * 3000
        {
            self.stable_since_ms = None;
            self.observed_successes = feedback.successes;
            self.message = "新账号缺少近期流量，重新观察".into();
            return;
        }
        self.message = "观察新账号真实请求结果".into();
        if observation.now_ms.saturating_sub(since) < u64::from(self.config.stable_seconds) * 1000
            || feedback.successes.saturating_sub(self.observed_successes)
                < u64::from(self.config.minimum_successes)
            || feedback.last_success_at_ms < since
        {
            return;
        }
        self.previous_old_limit = self.old_limit;
        self.old_limit = self
            .old_limit
            .saturating_sub(self.config.decrement)
            .max(self.config.minimum_old_concurrency);
        self.stable_since_ms = Some(observation.now_ms);
        self.observed_successes = feedback.successes;
        if self.old_limit == self.config.minimum_old_concurrency {
            self.phase = AccountHandoffPhase::Retaining;
            self.message = "保留老账号并发，继续消耗剩余额度".into();
        }
    }
}
