//! 交接组参与关系、共享名额与失败计数的原子持久化
use super::RedisAccountHandoffStore;
use gateway_core::{
    account::*,
    provider_ports::{ProviderStoreError, ProviderStoreErrorKind},
};
use redis::{AsyncCommands as _, Script};
use std::collections::BTreeMap;

pub(super) async fn list(
    store: &RedisAccountHandoffStore,
) -> Result<Vec<AccountTransferSnapshot>, ProviderStoreError> {
    let rows: BTreeMap<String, String> = store
        .connection
        .clone()
        .hgetall(&store.key)
        .await
        .map_err(|e| crate::provider_unavailable("read transfer groups", e))?;
    rows.iter()
        .filter(|(k, _)| k.starts_with("transfer-group:"))
        .map(|(k, v)| {
            let id = k.strip_prefix("transfer-group:").unwrap();
            let number = |field: &str| {
                rows.get(&format!("transfer-{field}:{id}"))
                    .map_or(Ok(0), |s| s.parse().map_err(|_| invalid()))
            };
            Ok(AccountTransferSnapshot {
                state: serde_json::from_str(v).map_err(|_| invalid())?,
                feedback: AccountHandoffFeedback {
                    successes: number("successes")?,
                    failures: number("failures")?,
                    last_success_at_ms: number("success_at")?,
                    last_failure_at_ms: number("failure_at")?,
                },
            })
        })
        .collect()
}
fn invalid() -> ProviderStoreError {
    ProviderStoreError::new(ProviderStoreErrorKind::InvalidData, "account transfer")
}
pub(super) async fn samples(
    store: &RedisAccountHandoffStore,
    samples: Vec<TransferQuotaSample>,
) -> Result<BTreeMap<String, Vec<TransferQuotaSample>>, ProviderStoreError> {
    if samples.len() > 200 {
        return Err(invalid());
    }
    let mut result = BTreeMap::new();
    // 同一 Lua 调用读取有界采样并按实际额度观测时间去重，不扫描请求日志
    let script = Script::new(
        r#"
local result={}
for i=1,#KEYS do
 local sample=cjson.decode(ARGV[i])
 local raw=redis.call('GET',KEYS[i])
 local points=raw and cjson.decode(raw) or {}
 local last=points[#points]
 if last and (last.reset~=sample.reset or sample.remaining>last.remaining or sample.observed<last.observed or sample.observed-last.observed>30000) then points={} last=nil end
 if not last or sample.observed-last.observed>=5000 then
  local trimmed={}
  for _,p in ipairs(points) do if sample.observed-p.observed<=300000 then table.insert(trimmed,p) end end
  table.insert(trimmed,sample)
  if #trimmed>61 then table.remove(trimmed,1) end
  points=trimmed
  redis.call('SET',KEYS[i],cjson.encode(points),'EX',600)
 end
 if #points>1 then result[sample.id]={points[1],points[#points]} else result[sample.id]=points end
end
return cjson.encode(result)
"#,
    );
    if samples.is_empty() {
        return Ok(result);
    }
    let mut invocation = script.prepare_invoke();
    for s in &samples {
        let fingerprint =
            super::resource_fingerprint("transfer quota", &s.id).map_err(|_| invalid())?;
        invocation.key(format!("{}:quota:{fingerprint}", store.key));
    }
    for s in &samples {
        invocation.arg(serde_json::to_string(s).map_err(|_| invalid())?);
    }
    let raw: String = invocation
        .invoke_async(&mut store.connection.clone())
        .await
        .map_err(|e| crate::provider_unavailable("observe transfer quota", e))?;
    let value: serde_json::Value = serde_json::from_str(&raw).map_err(|_| invalid())?;
    if let Some(values) = value.as_object() {
        for (k, v) in values {
            result.insert(
                k.clone(),
                serde_json::from_value(v.clone()).map_err(|_| invalid())?,
            );
        }
    }
    Ok(result)
}
pub(super) async fn save(
    store: &RedisAccountHandoffStore,
    mut state: AccountTransferGroup,
    revision: u64,
    failures: u64,
) -> Result<bool, ProviderStoreError> {
    state.validate().map_err(|_| invalid())?;
    uuid::Uuid::parse_str(&state.id).map_err(|_| invalid())?;
    for id in state.participant_ids() {
        ProviderAccountId::new(id).map_err(|_| invalid())?;
    }
    state.revision = revision.checked_add(1).ok_or_else(invalid)?;
    let raw = serde_json::to_string(&state).map_err(|_| invalid())?;
    let namespace = store
        .key
        .strip_suffix(":scheduler:{handoff}:active")
        .ok_or_else(invalid)?;
    let receiver_keys = state
        .new
        .iter()
        .map(|m| {
            super::resource_fingerprint("credential lease", &m.id)
                .map(|id| format!("{namespace}:lease:account:{{{id}}}:active"))
                .map_err(|_| invalid())
        })
        .collect::<Result<Vec<_>, _>>()?;
    let shared_limit: u64 = state.new.iter().map(|m| u64::from(m.limit)).sum();
    let mut caps = serde_json::to_value(state.capacities()).map_err(|_| invalid())?;
    for cap in caps.as_object_mut().ok_or_else(invalid)?.values_mut() {
        if cap["donor"] == false {
            cap["receiverKeys"] = serde_json::json!(receiver_keys);
            cap["sharedLimit"] = serde_json::json!(shared_limit);
        }
    }
    let caps = caps.to_string();
    let warmups: Vec<_> = state
        .new
        .iter()
        .map(|m| AccountWarmupSnapshot {
            state: AccountWarmup::new(
                m.id.clone(),
                m.name.clone(),
                state.config.new_account_protection.clone().unwrap(),
                state.policy_revision,
                state.checked_at_ms,
            ),
            feedback: Default::default(),
        })
        .collect();
    let changed:i64=Script::new(r#"
local id=ARGV[1]
local raw=redis.call('HGET',KEYS[1],'transfer-group:'..id)
local previous=raw and cjson.decode(raw) or nil
if (previous and previous.revision or 0) ~= tonumber(ARGV[2]) then return 0 end
if tonumber(redis.call('HGET',KEYS[1],'transfer-failures:'..id) or '0') ~= tonumber(ARGV[3]) then return 0 end
local state=cjson.decode(ARGV[4])
if not previous then
 local existing=redis.call('HGET',KEYS[1],'transfer-scope:'..state.groupId)
 if existing and existing~=id then return 0 end
 if redis.call('EXISTS',KEYS[2])==1 then return 0 end
 if tonumber(redis.call('HGET',KEYS[1],'transfer-count') or '0')>=128 then return 0 end
 if state.automatic then
  local policy=redis.call('HGET',KEYS[1],'policy')
  if not policy then return 0 end
  policy=cjson.decode(policy)
  if not policy.enabled or policy.revision~=state.policyRevision then return 0 end
 end
end
local ids={}
local new_warmups={}
local warms=cjson.decode(ARGV[6])
local handled_offset=2+#warms
for i,m in ipairs(state.new) do
 if state.automatic and not state.finished and redis.call('HGET',KEYS[1],'transfer-owner:'..m.id)~=id and redis.call('EXISTS',KEYS[handled_offset+i])==1 then return 0 end
end
for i,w in ipairs(warms) do
 if redis.call('HEXISTS',KEYS[1],'warmup:'..w.state.accountId)==0 and redis.call('EXISTS',KEYS[i+2])==0 then table.insert(new_warmups,w) end
end
if not state.finished and tonumber(redis.call('HGET',KEYS[1],'warmup-count') or '0')+#new_warmups>128 then return 0 end
for _,members in ipairs({state.old,state.new}) do
 for _,m in ipairs(members) do
  local owner=redis.call('HGET',KEYS[1],'transfer-owner:'..m.id)
  if (owner and owner~=id) or redis.call('HEXISTS',KEYS[1],'old:'..m.id)==1 or redis.call('HEXISTS',KEYS[1],'new:'..m.id)==1 then return 0 end
  table.insert(ids,m.id)
 end
end
if previous then
 for _,members in ipairs({previous.old,previous.new}) do
  for _,m in ipairs(members) do redis.call('HDEL',KEYS[1],'transfer-owner:'..m.id,'transfer-cap:'..m.id,'transfer-new:'..m.id) end
 end
end
for i,_ in ipairs(state.new) do redis.call('SET',KEYS[handled_offset+i],id,'EX',2592000) end
if state.finished then
 redis.call('HDEL',KEYS[1],'transfer-scope:'..state.groupId)
 redis.call('SET',KEYS[2],ARGV[4],'EX',604800)
 redis.call('HDEL',KEYS[1],'transfer-group:'..id,'transfer-successes:'..id,'transfer-failures:'..id,'transfer-success_at:'..id,'transfer-failure_at:'..id)
 if previous then redis.call('HINCRBY',KEYS[1],'transfer-count',-1) end
else
 local caps=cjson.decode(ARGV[5])
 redis.call('HSET',KEYS[1],'transfer-scope:'..state.groupId,id)
 for _,w in ipairs(new_warmups) do
  redis.call('HSET',KEYS[1],'warmup:'..w.state.accountId,cjson.encode(w))
  redis.call('HINCRBY',KEYS[1],'warmup-count',1)
 end
 for _,aid in ipairs(ids) do redis.call('HSET',KEYS[1],'transfer-owner:'..aid,id) end
 for aid,cap in pairs(caps) do redis.call('HSET',KEYS[1],'transfer-cap:'..aid,cjson.encode(cap)) end
 for _,m in ipairs(state.new) do if not m.terminal then redis.call('HSET',KEYS[1],'transfer-new:'..m.id,id) end end
 redis.call('HSET',KEYS[1],'transfer-group:'..id,ARGV[4])
 if not previous then redis.call('HINCRBY',KEYS[1],'transfer-count',1) end
end
return 1
"#).key(&store.key).key(format!("{}:transfer-receipt:{}",store.key,state.id)).key(state.new.iter().map(|m|store.warmup_receipt(&m.id)).collect::<Result<Vec<_>,_>>()?).key(state.new.iter().map(|m|store.handled_key(&m.id)).collect::<Result<Vec<_>,_>>()?).arg(&state.id).arg(revision).arg(failures).arg(raw).arg(caps).arg(serde_json::to_string(&warmups).map_err(|_|invalid())?).invoke_async(&mut store.connection.clone()).await.map_err(|e|crate::provider_unavailable("save transfer group",e))?;
    Ok(changed == 1)
}
