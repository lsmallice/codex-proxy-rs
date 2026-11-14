//! 独占 IPv6 出口的持久分配状态与非敏感管理投影

use gateway_core::account::OutboundProxy;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Ipv6LeaseState {
    Queued,
    Allocating,
    Unknown,
    Pending,
    Active,
    Retiring,
    Retired,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Ipv6LeaseView {
    pub request_id: String,
    pub controller_id: String,
    pub name: String,
    pub state: Ipv6LeaseState,
    pub address: Option<String>,
    pub proxy_id: Option<String>,
    pub owner_account_id: Option<String>,
}

#[derive(Debug, Clone)]
pub struct Ipv6Allocation {
    pub request_id: String,
    pub controller_id: String,
    pub name: String,
    pub account_id: Option<String>,
}

#[derive(Debug, Clone)]
pub struct Ipv6RemoteLease {
    pub id: String,
    pub address: std::net::Ipv6Addr,
    pub proxy: OutboundProxy,
}

#[derive(Debug, Clone)]
pub struct Ipv6Lease {
    pub view: Ipv6LeaseView,
    pub remote: Option<Ipv6RemoteLease>,
    pub target_account_id: Option<String>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Ipv6ImportPolicy {
    pub enabled: bool,
    pub controller_id: Option<String>,
    pub revision: u64,
}

#[derive(Debug, Clone, Serialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct Ipv6Status {
    pub configured: bool,
    pub available: bool,
    pub controller_id: Option<String>,
    pub prefix: Option<String>,
    pub issued: Option<String>,
    pub message: Option<String>,
}
