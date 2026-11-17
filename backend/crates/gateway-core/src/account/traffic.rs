//! 所有账号共用的瞬时发送保护配置，不改变并发和交接上限
use serde::{Deserialize, Serialize};
use std::sync::RwLock;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AccountTrafficPolicy {
    pub enabled: bool,
    pub burst: u32,
    pub requests_per_second: u32,
    pub max_wait_seconds: u32,
    pub max_waiting: u32,
}
impl Default for AccountTrafficPolicy {
    fn default() -> Self {
        Self {
            enabled: false,
            burst: 30,
            requests_per_second: 20,
            max_wait_seconds: 5,
            max_waiting: 100,
        }
    }
}
impl AccountTrafficPolicy {
    pub fn validate(self) -> Result<(), &'static str> {
        if !(1..=1000).contains(&self.burst)
            || !(1..=1000).contains(&self.requests_per_second)
            || !(1..=30).contains(&self.max_wait_seconds)
            || !(1..=1024).contains(&self.max_waiting)
        {
            return Err("账号瞬时保护参数超出允许范围");
        }
        Ok(())
    }
}
#[derive(Debug, Default)]
pub struct AccountTrafficRuntime(RwLock<AccountTrafficPolicy>);
impl AccountTrafficRuntime {
    pub fn snapshot(&self) -> AccountTrafficPolicy {
        *self
            .0
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
    pub fn publish(&self, policy: AccountTrafficPolicy) {
        *self
            .0
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = policy;
    }
}
