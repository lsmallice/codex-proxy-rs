//! 插件宿主容量和 HTTP 前置筛选的启动配置，不改变持久化功能绑定
use crate::RpcLimits;
use serde::{Deserialize, Serialize};
use std::{collections::BTreeMap, time::Duration};

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(default)]
pub struct PluginStartupConfig {
    pub rpc: PluginRpcSettings,
    pub maintenance_intervals_seconds: BTreeMap<String, u64>,
    pub http_filters: BTreeMap<String, Vec<PluginHttpRoute>>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct PluginRpcSettings {
    pub maximum_calls: usize,
    pub maximum_executing_calls: usize,
    pub maximum_queued_calls: usize,
    pub maximum_callbacks: usize,
    pub queue_timeout_ms: u64,
}
impl Default for PluginRpcSettings {
    fn default() -> Self {
        Self {
            maximum_calls: 256,
            maximum_executing_calls: 64,
            maximum_queued_calls: 64,
            maximum_callbacks: 320,
            queue_timeout_ms: 30000,
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PluginHttpRoute {
    pub method: String,
    pub path: String,
}
impl PluginHttpRoute {
    pub(crate) fn matches(&self, method: &str, path: &str) -> bool {
        self.method == method && self.path == path
    }
}
impl PluginStartupConfig {
    pub fn validate(&self) -> Result<(), &'static str> {
        let rpc = &self.rpc;
        if self.maintenance_intervals_seconds.len() > 64
            || self
                .maintenance_intervals_seconds
                .iter()
                .any(|(id, seconds)| {
                    uuid::Uuid::parse_str(id).is_err() || !(5..=3600).contains(seconds)
                })
        {
            return Err("plugins.maintenance_intervals_seconds");
        }
        if !(1..=1024).contains(&rpc.maximum_calls)
            || !(1..=rpc.maximum_calls).contains(&rpc.maximum_executing_calls)
            || rpc.maximum_queued_calls > rpc.maximum_calls
            || !(1..=4096).contains(&rpc.maximum_callbacks)
            || !(1..=60000).contains(&rpc.queue_timeout_ms)
        {
            return Err("plugins.rpc");
        }
        if self.http_filters.len() > 64
            || self.http_filters.iter().any(|(id, routes)| {
                uuid::Uuid::parse_str(id).is_err()
                    || routes.is_empty()
                    || routes.len() > 32
                    || routes.iter().any(|route| {
                        !matches!(
                            route.method.as_str(),
                            "GET" | "POST" | "PUT" | "PATCH" | "DELETE" | "HEAD" | "OPTIONS"
                        ) || !route.path.starts_with('/')
                            || route.path.len() > 1024
                            || route.path.bytes().any(|b| {
                                b.is_ascii_control()
                                    || b.is_ascii_whitespace()
                                    || matches!(b, b'?' | b'#' | b'*')
                            })
                    })
            })
        {
            return Err("plugins.http_filters");
        }
        Ok(())
    }
    #[must_use]
    pub fn rpc_limits(&self) -> RpcLimits {
        RpcLimits {
            maximum_calls: self.rpc.maximum_calls,
            maximum_executing_calls: self.rpc.maximum_executing_calls,
            maximum_queued_calls: self.rpc.maximum_queued_calls,
            maximum_callbacks: self.rpc.maximum_callbacks,
            maximum_queue_timeout: Duration::from_millis(self.rpc.queue_timeout_ms),
            ..RpcLimits::default()
        }
    }
}
