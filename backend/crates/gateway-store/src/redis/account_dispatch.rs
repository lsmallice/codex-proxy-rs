//! 账号级共享令牌桶，所有入口和实例原子消费同一发送预算
use super::{namespace, resource_fingerprint};
use crate::StoreResult;
use futures::future::BoxFuture;
use gateway_core::{
    account::{AccountTrafficPolicy, ProviderAccountId},
    provider_ports::{AccountDispatchStore, ProviderStoreError},
};
use redis::{Script, aio::ConnectionManager};
use std::time::Duration;

const START: &str = r#"
local clock=redis.call('TIME')
local now=tonumber(clock[1])*1000+math.floor(tonumber(clock[2])/1000)
local cap=tonumber(ARGV[1])*1000
local rate=tonumber(ARGV[2])
local old=redis.call('HMGET',KEYS[1],'tokens','at','rate')
local tokens=cap
if old[1] then
 local at=tonumber(old[2])
 now=math.max(now,at)
 tokens=math.min(cap,tonumber(old[1])+(now-at)*tonumber(old[3]))
end
if ARGV[3]=='peek' then return tokens>=1000 and 0 or math.ceil((1000-tokens)/rate) end
local retry=0
if tokens>=1000 then tokens=tokens-1000 else retry=math.ceil((1000-tokens)/rate) end
redis.call('HSET',KEYS[1],'tokens',tokens,'at',now,'rate',rate)
redis.call('PEXPIRE',KEYS[1],math.max(60000,math.ceil(cap/rate)+1000))
return retry
"#;

#[derive(Clone)]
pub struct RedisAccountDispatchStore {
    connection: ConnectionManager,
    namespace: String,
}
impl RedisAccountDispatchStore {
    pub fn new(connection: ConnectionManager, prefix: &str) -> StoreResult<Self> {
        Ok(Self {
            connection,
            namespace: namespace(prefix)?,
        })
    }
}
impl AccountDispatchStore for RedisAccountDispatchStore {
    fn ready<'a>(
        &'a self,
        account: &'a ProviderAccountId,
        policy: AccountTrafficPolicy,
    ) -> BoxFuture<'a, Result<bool, ProviderStoreError>> {
        Box::pin(async move {
            policy.validate().map_err(|_| {
                gateway_core::provider_ports::ProviderStoreError::new(
                    gateway_core::provider_ports::ProviderStoreErrorKind::InvalidData,
                    "account traffic policy",
                )
            })?;
            let fingerprint = resource_fingerprint("account traffic", account.as_str())
                .map_err(|e| crate::provider_unavailable("account traffic key", e))?;
            let key = format!("{}:account-traffic:{}", self.namespace, fingerprint);
            let mut connection = self.connection.clone();
            let retry: u64 = Script::new(START)
                .key(key)
                .arg(policy.burst)
                .arg(policy.requests_per_second)
                .arg("peek")
                .invoke_async(&mut connection)
                .await
                .map_err(|e| {
                    crate::provider_unavailable(
                        "read account send capacity",
                        crate::redis_unavailable("account send capacity", e),
                    )
                })?;
            Ok(retry == 0)
        })
    }
    fn try_start<'a>(
        &'a self,
        account: &'a ProviderAccountId,
        policy: AccountTrafficPolicy,
    ) -> BoxFuture<'a, Result<Duration, ProviderStoreError>> {
        Box::pin(async move {
            policy.validate().map_err(|_| {
                gateway_core::provider_ports::ProviderStoreError::new(
                    gateway_core::provider_ports::ProviderStoreErrorKind::InvalidData,
                    "account traffic policy",
                )
            })?;
            if !policy.enabled {
                return Ok(Duration::ZERO);
            }
            let fingerprint = resource_fingerprint("account traffic", account.as_str())
                .map_err(|e| crate::provider_unavailable("account traffic key", e))?;
            let key = format!("{}:account-traffic:{}", self.namespace, fingerprint);
            let mut connection = self.connection.clone();
            let retry: u64 = Script::new(START)
                .key(key)
                .arg(policy.burst)
                .arg(policy.requests_per_second)
                .arg("consume")
                .invoke_async(&mut connection)
                .await
                .map_err(|e| {
                    crate::provider_unavailable(
                        "acquire account send permit",
                        crate::redis_unavailable("account send permit", e),
                    )
                })?;
            Ok(Duration::from_millis(retry))
        })
    }
}
