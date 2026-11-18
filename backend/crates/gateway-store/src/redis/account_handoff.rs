//! 账号交接的共享运行状态、反馈计数、幂等启动与 CAS 更新

use std::collections::BTreeMap;

use futures::future::BoxFuture;
use gateway_core::{
    account::{
        AccountHandoff, AccountHandoffCapacity, AccountHandoffFeedback, AccountHandoffPage,
        AccountHandoffPolicy, AccountHandoffSnapshot, HANDOFF_HISTORY_RETENTION_SECONDS,
        MAX_ACTIVE_HANDOFFS, MAX_HANDOFF_HISTORY_RECORDS, ProviderAccountId, validate_handoff_page,
    },
    provider_ports::{AccountHandoffStore, ProviderStoreError, ProviderStoreErrorKind},
};
use redis::{AsyncCommands as _, Script, aio::ConnectionManager};

use super::namespace;
use crate::StoreResult;

const START: &str = r#"
if redis.call('HEXISTS',KEYS[1],'transfer-owner:'..ARGV[2])==1 or redis.call('HEXISTS',KEYS[1],'transfer-owner:'..ARGV[3])==1 then return '' end
if ARGV[8] == '1' then
 local policy = redis.call('HGET', KEYS[1], 'policy')
 if not policy then return '' end
 local value = cjson.decode(policy)
 if not value.enabled or value.revision ~= tonumber(ARGV[9]) then return '' end
end
local prior = redis.call('GET', KEYS[2])
if prior then return prior end
local prior = redis.call('HGET', KEYS[1], 'state:' .. ARGV[1])
if prior then return prior end
if ARGV[8] == '1' and redis.call('EXISTS', KEYS[3]) == 1 then return '' end
if redis.call('HEXISTS', KEYS[1], 'old:' .. ARGV[2]) == 1 or
   redis.call('HEXISTS', KEYS[1], 'new:' .. ARGV[2]) == 1 or
   redis.call('HEXISTS', KEYS[1], 'old:' .. ARGV[3]) == 1 or
   redis.call('HEXISTS', KEYS[1], 'new:' .. ARGV[3]) == 1 then return '' end
if tonumber(redis.call('HGET', KEYS[1], 'count') or '0') >= tonumber(ARGV[6]) then return '' end
local start_warmup = ARGV[10] ~= '' and redis.call('HEXISTS', KEYS[1], 'warmup:' .. ARGV[3]) == 0 and redis.call('EXISTS', KEYS[4]) == 0
if start_warmup and tonumber(redis.call('HGET', KEYS[1], 'warmup-count') or '0') >= 128 then return '' end
redis.call('HSET', KEYS[1], 'state:' .. ARGV[1], ARGV[4],
 'old:' .. ARGV[2], ARGV[1], 'new:' .. ARGV[3], ARGV[1],
 'capacity:' .. ARGV[2], ARGV[5], 'revision:' .. ARGV[1], '1',
 'limit:' .. ARGV[2], ARGV[7])
redis.call('HINCRBY', KEYS[1], 'count', 1)
redis.call('SET', KEYS[3], ARGV[1], 'EX', 2592000)
if start_warmup then
 redis.call('HSET', KEYS[1], 'warmup:' .. ARGV[3], ARGV[10])
 redis.call('HINCRBY', KEYS[1], 'warmup-count', 1)
end
return ARGV[4]
"#;

const UPDATE: &str = r#"
local id = ARGV[1]
if redis.call('HGET', KEYS[1], 'revision:' .. id) ~= ARGV[2] then return 0 end
if (redis.call('HGET', KEYS[1], 'failures:' .. id) or '0') ~= ARGV[3] then return 0 end
if ARGV[7] == '0' then
 local clock = redis.call('TIME')
 local now = tonumber(clock[1]) * 1000 + math.floor(tonumber(clock[2]) / 1000)
 local state = cjson.decode(ARGV[4])
 state.finishedAtMs = now
 local feedback = {
  successes = tonumber(redis.call('HGET', KEYS[1], 'successes:' .. id) or '0'),
  failures = tonumber(redis.call('HGET', KEYS[1], 'failures:' .. id) or '0'),
  lastSuccessAtMs = tonumber(redis.call('HGET', KEYS[1], 'success_at:' .. id) or '0'),
  lastFailureAtMs = tonumber(redis.call('HGET', KEYS[1], 'failure_at:' .. id) or '0')
 }
 redis.call('SET', KEYS[2], cjson.encode(state), 'EX', ARGV[10])
 redis.call('HSET', KEYS[4], id, cjson.encode({state=state, feedback=feedback}))
 redis.call('ZADD', KEYS[3], now, id)
 prune_history(KEYS[3], KEYS[4], now, tonumber(ARGV[10]), tonumber(ARGV[11]))
 redis.call('EXPIRE', KEYS[3], ARGV[10])
 redis.call('EXPIRE', KEYS[4], ARGV[10])
 redis.call('HDEL', KEYS[1], 'state:' .. id, 'revision:' .. id,
  'old:' .. ARGV[5], 'new:' .. ARGV[6], 'capacity:' .. ARGV[5], 'limit:' .. ARGV[5],
  'successes:' .. id, 'failures:' .. id, 'success_at:' .. id, 'failure_at:' .. id)
 redis.call('HINCRBY', KEYS[1], 'count', -1)
else
 redis.call('HSET', KEYS[1], 'state:' .. id, ARGV[4],
  'capacity:' .. ARGV[5], ARGV[8], 'limit:' .. ARGV[5], ARGV[9])
 redis.call('HINCRBY', KEYS[1], 'revision:' .. id, 1)
end
return 1
"#;

// 有界索引和快照同步清理，完成回执独立保留，不因历史容量裁剪失去幂等保护
const HISTORY_PRUNE: &str = r#"
local function remove_history(index, data, ids)
 for _, id in ipairs(ids) do
  redis.call('ZREM', index, id)
  redis.call('HDEL', data, id)
 end
end
local function prune_history(index, data, now, retention, maximum)
 remove_history(index, data, redis.call('ZRANGEBYSCORE', index, '-inf', now - retention * 1000))
 local excess = redis.call('ZCARD', index) - maximum
 if excess > 0 then remove_history(index, data, redis.call('ZRANGE', index, 0, excess - 1)) end
end
"#;

const HISTORY_PAGE: &str = r#"
local clock = redis.call('TIME')
local now = tonumber(clock[1]) * 1000 + math.floor(tonumber(clock[2]) / 1000)
prune_history(KEYS[1], KEYS[2], now, tonumber(ARGV[3]), tonumber(ARGV[4]))
local total = redis.call('ZCARD', KEYS[1])
local size = tonumber(ARGV[2])
local page = math.min(tonumber(ARGV[1]), math.max(1, math.ceil(total / size)))
local ids = redis.call('ZREVRANGE', KEYS[1], (page - 1) * size, page * size - 1)
local items = {}
for _, id in ipairs(ids) do
 local raw = redis.call('HGET', KEYS[2], id)
 if not raw then return redis.error_reply('handoff history snapshot missing') end
 table.insert(items, raw)
end
return {total, page, items}
"#;

const FEEDBACK: &str = r#"
local group=redis.call('HGET',KEYS[1],'transfer-new:'..ARGV[1])
if group then
 local state=cjson.decode(redis.call('HGET',KEYS[1],'transfer-group:'..group))
 local clock=redis.call('TIME')
 local now=tonumber(clock[1])*1000+math.floor(tonumber(clock[2])/1000)
 if ARGV[2]~='1' then
  redis.call('HINCRBY',KEYS[1],'transfer-failures:'..group,1)
  redis.call('HSET',KEYS[1],'transfer-failure_at:'..group,tostring(now))
 elseif tonumber(ARGV[3])>=state.createdAtMs then
  redis.call('HINCRBY',KEYS[1],'transfer-successes:'..group,1)
  redis.call('HSET',KEYS[1],'transfer-success_at:'..group,tostring(now))
 end
end
local warm = redis.call('HGET', KEYS[1], 'warmup:' .. ARGV[1])
if warm then
 local value = cjson.decode(warm)
 local clock = redis.call('TIME')
 local now = tonumber(clock[1]) * 1000 + math.floor(tonumber(clock[2]) / 1000)
 if ARGV[2] == '1' and tonumber(ARGV[3]) >= value.state.startedAtMs then
  value.feedback.successes = value.feedback.successes + 1
  value.feedback.lastSuccessAtMs = now
 elseif ARGV[2] ~= '1' then
  value.feedback.failures = value.feedback.failures + 1
  value.feedback.lastFailureAtMs = now
 end
 redis.call('HSET', KEYS[1], 'warmup:' .. ARGV[1], cjson.encode(value))
end
local id = redis.call('HGET', KEYS[1], 'new:' .. ARGV[1])
if not id then return 0 end
if ARGV[2] == '1' then
 local state = cjson.decode(redis.call('HGET', KEYS[1], 'state:' .. id))
 if tonumber(ARGV[3]) < state.createdAtMs then return 0 end
end
local clock = redis.call('TIME')
local now = (tonumber(clock[1]) * 1000) + math.floor(tonumber(clock[2]) / 1000)
if ARGV[2] == '1' then
 redis.call('HINCRBY', KEYS[1], 'successes:' .. id, 1)
 redis.call('HSET', KEYS[1], 'success_at:' .. id, tostring(now))
else
 redis.call('HINCRBY', KEYS[1], 'failures:' .. id, 1)
 redis.call('HSET', KEYS[1], 'failure_at:' .. id, tostring(now))
end
return 1
"#;

#[derive(Clone)]
pub struct RedisAccountHandoffStore {
    pub(super) connection: ConnectionManager,
    pub(super) key: String,
}

pub(super) fn handoff_key(namespace: &str) -> String {
    format!("{namespace}:scheduler:{{handoff}}:active")
}

fn invalid() -> ProviderStoreError {
    ProviderStoreError::new(ProviderStoreErrorKind::InvalidData, "account handoff")
}

fn encode(value: &impl serde::Serialize) -> Result<String, ProviderStoreError> {
    serde_json::to_string(value).map_err(|_| invalid())
}

impl RedisAccountHandoffStore {
    pub(super) fn warmup_receipt(&self, id: &str) -> Result<String, ProviderStoreError> {
        let fingerprint =
            super::resource_fingerprint("warmup account", id).map_err(|_| invalid())?;
        Ok(format!("{}:warmup-done:{fingerprint}", self.key))
    }
    pub async fn warmup_limits(
        &self,
        ids: &[ProviderAccountId],
    ) -> Result<BTreeMap<ProviderAccountId, Option<u32>>, ProviderStoreError> {
        if ids.is_empty() {
            return Ok(BTreeMap::new());
        }
        let fields: Vec<_> = ids
            .iter()
            .map(|id| format!("warmup:{}", id.as_str()))
            .collect();
        let active: Vec<Option<String>> = redis::cmd("HMGET")
            .arg(&self.key)
            .arg(fields)
            .query_async(&mut self.connection.clone())
            .await
            .map_err(|e| crate::provider_unavailable("read warmup limits", e))?;
        let keys = ids
            .iter()
            .map(|id| self.warmup_receipt(id.as_str()))
            .collect::<Result<Vec<_>, _>>()?;
        let done: Vec<Option<String>> = redis::cmd("MGET")
            .arg(keys)
            .query_async(&mut self.connection.clone())
            .await
            .map_err(|e| crate::provider_unavailable("read warmup receipts", e))?;
        let mut result = BTreeMap::new();
        for ((id, active), done) in ids.iter().zip(active).zip(done) {
            if let Some(raw) = active {
                let value: gateway_core::account::AccountWarmupSnapshot =
                    serde_json::from_str(&raw).map_err(|_| invalid())?;
                result.insert(id.clone(), Some(value.state.limit));
            } else if done.is_some() {
                result.insert(id.clone(), None);
            }
        }
        Ok(result)
    }
    pub fn new(connection: ConnectionManager, key_namespace: &str) -> StoreResult<Self> {
        Ok(Self {
            connection,
            key: handoff_key(&namespace(key_namespace)?),
        })
    }

    fn receipt_key(&self, id: &str) -> Result<String, ProviderStoreError> {
        let id = uuid::Uuid::parse_str(id).map_err(|_| invalid())?;
        Ok(format!("{}:receipt:{id}", self.key))
    }

    pub(super) fn handled_key(&self, id: &str) -> Result<String, ProviderStoreError> {
        let fingerprint =
            super::resource_fingerprint("handoff account", id).map_err(|_| invalid())?;
        Ok(format!("{}:handled:{fingerprint}", self.key))
    }

    pub async fn handled(
        &self,
        ids: &[ProviderAccountId],
    ) -> Result<std::collections::BTreeSet<ProviderAccountId>, ProviderStoreError> {
        if ids.is_empty() {
            return Ok(Default::default());
        }
        let keys = ids
            .iter()
            .map(|id| self.handled_key(id.as_str()))
            .collect::<Result<Vec<_>, _>>()?;
        let values: Vec<Option<String>> = redis::cmd("MGET")
            .arg(keys)
            .query_async(&mut self.connection.clone())
            .await
            .map_err(|e| crate::provider_unavailable("read handled handoff accounts", e))?;
        Ok(ids
            .iter()
            .cloned()
            .zip(values)
            .filter_map(|(id, v)| v.map(|_| id))
            .collect())
    }

    pub async fn capacities(
        &self,
        ids: &[ProviderAccountId],
    ) -> Result<BTreeMap<ProviderAccountId, AccountHandoffCapacity>, ProviderStoreError> {
        if ids.is_empty() {
            return Ok(BTreeMap::new());
        }
        let fields = ids
            .iter()
            .map(|id| format!("capacity:{}", id.as_str()))
            .collect::<Vec<_>>();
        let values: Vec<Option<String>> = redis::cmd("HMGET")
            .arg(&self.key)
            .arg(fields)
            .query_async(&mut self.connection.clone())
            .await
            .map_err(|e| crate::provider_unavailable("read handoff capacities", e))?;
        ids.iter()
            .cloned()
            .zip(values)
            .filter_map(|(id, value)| {
                value.map(|value| {
                    serde_json::from_str(&value)
                        .map(|v| (id, v))
                        .map_err(|_| invalid())
                })
            })
            .collect()
    }
}

impl AccountHandoffStore for RedisAccountHandoffStore {
    fn warmup_capacities(
        &self,
        ids: Vec<ProviderAccountId>,
    ) -> BoxFuture<'_, Result<BTreeMap<ProviderAccountId, Option<u32>>, ProviderStoreError>> {
        Box::pin(async move { self.warmup_limits(&ids).await })
    }
    fn transfer_samples(
        &self,
        samples: Vec<gateway_core::account::TransferQuotaSample>,
    ) -> BoxFuture<
        '_,
        Result<
            BTreeMap<String, Vec<gateway_core::account::TransferQuotaSample>>,
            ProviderStoreError,
        >,
    > {
        Box::pin(super::account_transfer::samples(self, samples))
    }
    fn transfer_groups(
        &self,
    ) -> BoxFuture<
        '_,
        Result<Vec<gateway_core::account::AccountTransferSnapshot>, ProviderStoreError>,
    > {
        Box::pin(super::account_transfer::list(self))
    }
    fn save_transfer_group(
        &self,
        state: gateway_core::account::AccountTransferGroup,
        revision: u64,
        failures: u64,
    ) -> BoxFuture<'_, Result<bool, ProviderStoreError>> {
        Box::pin(super::account_transfer::save(
            self, state, revision, failures,
        ))
    }
    fn warmups(
        &self,
    ) -> BoxFuture<'_, Result<Vec<gateway_core::account::AccountWarmupSnapshot>, ProviderStoreError>>
    {
        Box::pin(async move {
            let rows: BTreeMap<String, String> =
                self.connection
                    .clone()
                    .hgetall(&self.key)
                    .await
                    .map_err(|e| crate::provider_unavailable("read warmups", e))?;
            rows.into_iter()
                .filter(|(k, _)| k.starts_with("warmup:"))
                .map(|(_, value)| serde_json::from_str(&value).map_err(|_| invalid()))
                .collect()
        })
    }
    fn start_warmup(
        &self,
        state: gateway_core::account::AccountWarmup,
    ) -> BoxFuture<'_, Result<(), ProviderStoreError>> {
        Box::pin(async move {
            state.config.validate().map_err(|_| invalid())?;
            let value = gateway_core::account::AccountWarmupSnapshot {
                state: state.clone(),
                feedback: Default::default(),
            };
            let result: i64 = Script::new(r#"
if redis.call('HEXISTS', KEYS[1], ARGV[1]) == 1 or redis.call('EXISTS', KEYS[2]) == 1 then return 1 end
local policy = redis.call('HGET', KEYS[1], 'policy')
if not policy then return 0 end
local p = cjson.decode(policy)
if not p.enabled or p.revision ~= tonumber(ARGV[3]) or p.config.newAccountProtection == nil or p.config.newAccountProtection == cjson.null then return 0 end
if tonumber(redis.call('HGET', KEYS[1], 'warmup-count') or '0') >= 128 then return 0 end
redis.call('HSET', KEYS[1], ARGV[1], ARGV[2])
redis.call('HINCRBY', KEYS[1], 'warmup-count', 1)
return 1
"#).key(&self.key).key(self.warmup_receipt(&state.account_id)?).arg(format!("warmup:{}", state.account_id)).arg(encode(&value)?).arg(state.policy_revision).invoke_async(&mut self.connection.clone()).await.map_err(|e| crate::provider_unavailable("start warmup", e))?;
            if result != 1 {
                return Err(ProviderStoreError::new(
                    ProviderStoreErrorKind::Conflict,
                    "warmup policy or capacity changed",
                ));
            }
            Ok(())
        })
    }
    fn update_warmup(
        &self,
        mut state: gateway_core::account::AccountWarmup,
        expected_failures: u64,
    ) -> BoxFuture<'_, Result<bool, ProviderStoreError>> {
        Box::pin(async move {
            let revision = state.revision;
            state.revision = revision.checked_add(1).ok_or_else(invalid)?;
            let result: i64 = Script::new(r#"
local raw = redis.call('HGET', KEYS[1], ARGV[1])
if not raw then return 0 end
local value = cjson.decode(raw)
if value.state.revision ~= tonumber(ARGV[2]) or value.feedback.failures ~= tonumber(ARGV[3]) then return 0 end
value.state = cjson.decode(ARGV[4])
if value.state.finished then
 redis.call('HDEL', KEYS[1], ARGV[1])
 redis.call('HINCRBY', KEYS[1], 'warmup-count', -1)
 redis.call('SET', KEYS[2], '1', 'EX', 2592000)
else redis.call('HSET', KEYS[1], ARGV[1], cjson.encode(value)) end
return 1
"#).key(&self.key).key(self.warmup_receipt(&state.account_id)?).arg(format!("warmup:{}", state.account_id)).arg(revision).arg(expected_failures).arg(encode(&state)?).invoke_async(&mut self.connection.clone()).await.map_err(|e| crate::provider_unavailable("update warmup", e))?;
            Ok(result == 1)
        })
    }
    fn history(
        &self,
        page: u32,
        page_size: u32,
    ) -> BoxFuture<'_, Result<AccountHandoffPage, ProviderStoreError>> {
        Box::pin(async move {
            validate_handoff_page(page, page_size).map_err(|_| invalid())?;
            let (total, page, rows): (u64, u32, Vec<String>) =
                Script::new(&format!("{HISTORY_PRUNE}{HISTORY_PAGE}"))
                    .key(format!("{}:history-index", self.key))
                    .key(format!("{}:history-data", self.key))
                    .arg(page)
                    .arg(page_size)
                    .arg(HANDOFF_HISTORY_RETENTION_SECONDS)
                    .arg(MAX_HANDOFF_HISTORY_RECORDS)
                    .invoke_async(&mut self.connection.clone())
                    .await
                    .map_err(|e| crate::provider_unavailable("read handoff history page", e))?;
            let items = rows
                .into_iter()
                .map(|row| serde_json::from_str(&row).map_err(|_| invalid()))
                .collect::<Result<Vec<AccountHandoffSnapshot>, _>>()?;
            if items
                .iter()
                .any(|item| item.state.phase.active() || item.state.finished_at_ms.is_none())
            {
                return Err(invalid());
            }
            Ok(AccountHandoffPage {
                items,
                total,
                page,
                page_size,
                active_total: 0,
                occupied_account_ids: Vec::new(),
            })
        })
    }
    fn policy(&self) -> BoxFuture<'_, Result<AccountHandoffPolicy, ProviderStoreError>> {
        Box::pin(async move {
            let raw: Option<String> = self
                .connection
                .clone()
                .hget(&self.key, "policy")
                .await
                .map_err(|e| crate::provider_unavailable("read handoff policy", e))?;
            raw.map_or(Ok(AccountHandoffPolicy::default()), |s| {
                serde_json::from_str(&s).map_err(|_| invalid())
            })
        })
    }

    fn save_policy(
        &self,
        policy: AccountHandoffPolicy,
        expected_revision: u64,
    ) -> BoxFuture<'_, Result<bool, ProviderStoreError>> {
        Box::pin(async move {
            let updated: i64 = Script::new(
                r#"
local raw = redis.call('HGET', KEYS[1], 'policy')
local current = raw and cjson.decode(raw).revision or 0
if current ~= tonumber(ARGV[1]) then return 0 end
redis.call('HSET', KEYS[1], 'policy', ARGV[2])
return 1
"#,
            )
            .key(&self.key)
            .arg(expected_revision)
            .arg(encode(&policy)?)
            .invoke_async(&mut self.connection.clone())
            .await
            .map_err(|e| crate::provider_unavailable("save handoff policy", e))?;
            Ok(updated == 1)
        })
    }
    fn get(
        &self,
        id: String,
    ) -> BoxFuture<'_, Result<Option<AccountHandoffSnapshot>, ProviderStoreError>> {
        Box::pin(async move {
            if let Some(snapshot) = self.list().await?.into_iter().find(|s| s.state.id == id) {
                return Ok(Some(snapshot));
            }
            let raw: Option<String> = self
                .connection
                .clone()
                .get(self.receipt_key(&id)?)
                .await
                .map_err(|e| crate::provider_unavailable("read handoff receipt", e))?;
            raw.map(|s| {
                serde_json::from_str(&s)
                    .map(|state| AccountHandoffSnapshot {
                        state,
                        feedback: AccountHandoffFeedback::default(),
                    })
                    .map_err(|_| invalid())
            })
            .transpose()
        })
    }
    fn list(&self) -> BoxFuture<'_, Result<Vec<AccountHandoffSnapshot>, ProviderStoreError>> {
        Box::pin(async move {
            let rows: BTreeMap<String, String> =
                self.connection
                    .clone()
                    .hgetall(&self.key)
                    .await
                    .map_err(|e| crate::provider_unavailable("list account handoffs", e))?;
            let number = |key: String| -> Result<u64, ProviderStoreError> {
                rows.get(&key)
                    .map_or(Ok(0), |s| s.parse().map_err(|_| invalid()))
            };
            let mut result = Vec::new();
            for (key, value) in &rows {
                if let Some(id) = key.strip_prefix("state:") {
                    let state: AccountHandoff =
                        serde_json::from_str(value).map_err(|_| invalid())?;
                    result.push(AccountHandoffSnapshot {
                        state,
                        feedback: AccountHandoffFeedback {
                            successes: number(format!("successes:{id}"))?,
                            failures: number(format!("failures:{id}"))?,
                            last_success_at_ms: number(format!("success_at:{id}"))?,
                            last_failure_at_ms: number(format!("failure_at:{id}"))?,
                        },
                    });
                }
            }
            if result.len() > MAX_ACTIVE_HANDOFFS {
                return Err(invalid());
            }
            result.sort_by_key(|v| v.state.created_at_ms);
            Ok(result)
        })
    }

    fn start(
        &self,
        state: AccountHandoff,
    ) -> BoxFuture<'_, Result<AccountHandoffSnapshot, ProviderStoreError>> {
        Box::pin(async move {
            let warmup = state
                .config
                .new_account_protection
                .as_ref()
                .map(|config| {
                    encode(&gateway_core::account::AccountWarmupSnapshot {
                        state: gateway_core::account::AccountWarmup::new(
                            state.new_account_id.clone(),
                            state.new_account_name.clone(),
                            config.clone(),
                            state.automatic_policy_revision,
                            state.created_at_ms,
                        ),
                        feedback: Default::default(),
                    })
                })
                .transpose()?
                .unwrap_or_default();
            let capacity = AccountHandoffCapacity {
                id: state.id.clone(),
                new_account_id: state.new_account_id.clone(),
                old_limit: state.old_limit,
            };
            let raw: String = Script::new(START)
                .key(&self.key)
                .key(self.receipt_key(&state.id)?)
                .key(self.handled_key(&state.new_account_id)?)
                .key(self.warmup_receipt(&state.new_account_id)?)
                .arg(&state.id)
                .arg(&state.old_account_id)
                .arg(&state.new_account_id)
                .arg(encode(&state)?)
                .arg(encode(&capacity)?)
                .arg(MAX_ACTIVE_HANDOFFS)
                .arg(state.old_limit)
                .arg(u8::from(state.automatic))
                .arg(state.automatic_policy_revision)
                .arg(warmup)
                .invoke_async(&mut self.connection.clone())
                .await
                .map_err(|e| crate::provider_unavailable("start account handoff", e))?;
            if raw.is_empty() {
                return Err(ProviderStoreError::new(
                    ProviderStoreErrorKind::Conflict,
                    "account already in handoff",
                ));
            }
            let saved: AccountHandoff = serde_json::from_str(&raw).map_err(|_| invalid())?;
            if saved.old_account_id != state.old_account_id
                || saved.new_account_id != state.new_account_id
                || saved.config != state.config
            {
                return Err(ProviderStoreError::new(
                    ProviderStoreErrorKind::Conflict,
                    "handoff request id reused",
                ));
            }
            Ok(AccountHandoffSnapshot {
                state: saved,
                feedback: AccountHandoffFeedback::default(),
            })
        })
    }

    fn update(
        &self,
        mut state: AccountHandoff,
        expected_revision: u64,
        expected_failures: u64,
    ) -> BoxFuture<'_, Result<bool, ProviderStoreError>> {
        Box::pin(async move {
            state.revision = expected_revision.checked_add(1).ok_or_else(invalid)?;
            let capacity = AccountHandoffCapacity {
                id: state.id.clone(),
                new_account_id: state.new_account_id.clone(),
                old_limit: state.old_limit,
            };
            let changed: i64 = Script::new(&format!("{HISTORY_PRUNE}{UPDATE}"))
                .key(&self.key)
                .key(self.receipt_key(&state.id)?)
                .key(format!("{}:history-index", self.key))
                .key(format!("{}:history-data", self.key))
                .arg(&state.id)
                .arg(expected_revision)
                .arg(expected_failures)
                .arg(encode(&state)?)
                .arg(&state.old_account_id)
                .arg(&state.new_account_id)
                .arg(u8::from(state.phase.active()))
                .arg(encode(&capacity)?)
                .arg(state.old_limit)
                .arg(HANDOFF_HISTORY_RETENTION_SECONDS)
                .arg(MAX_HANDOFF_HISTORY_RECORDS)
                .invoke_async(&mut self.connection.clone())
                .await
                .map_err(|e| crate::provider_unavailable("update account handoff", e))?;
            Ok(changed == 1)
        })
    }

    fn feedback(
        &self,
        account_id: String,
        success: bool,
        started_at_ms: Option<u64>,
    ) -> BoxFuture<'_, Result<(), ProviderStoreError>> {
        Box::pin(async move {
            let _: i64 = Script::new(FEEDBACK)
                .key(&self.key)
                .arg(account_id)
                .arg(u8::from(success))
                .arg(started_at_ms.unwrap_or_default())
                .invoke_async(&mut self.connection.clone())
                .await
                .map_err(|e| crate::provider_unavailable("record account handoff feedback", e))?;
            Ok(())
        })
    }
}
