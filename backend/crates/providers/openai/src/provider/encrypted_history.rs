//! OpenAI 已拒绝密文的定点清理与短期摘要记忆，不扩大续接授权或保存历史正文
use super::*;
use sha2::{Digest, Sha256};
use std::collections::{HashMap, VecDeque};
use std::io::{self, Write};
use std::sync::Mutex;
use tokio::time::Instant;

const MAX_REQUEST_BYTES: usize = 8 * 1024 * 1024;
const MAX_ERROR_BYTES: usize = 64 * 1024;
const TTL: Duration = Duration::from_secs(30 * 60);
type Hash = [u8; 32];

#[derive(Default)]
struct Entries {
    scopes: HashMap<Hash, Binding>,
    expiry: BTreeSet<(Instant, Hash)>,
}
struct Binding {
    digests: VecDeque<Hash>,
    expires_at: Instant,
}
pub(super) struct Cache {
    entries: Mutex<Entries>,
    max_scopes: usize,
    max_digests: usize,
}
impl Default for Cache {
    fn default() -> Self {
        Self::new(65536, 512)
    }
}

impl Cache {
    pub(super) fn new(max_scopes: usize, max_digests: usize) -> Self {
        Self {
            entries: Mutex::new(Entries::default()),
            max_scopes: max_scopes.clamp(1, 65536),
            max_digests: max_digests.clamp(1, 512),
        }
    }
    fn remember(&self, scope: Hash, digest: Hash) {
        let Ok(mut entries) = self.entries.lock() else {
            return;
        };
        let now = Instant::now();
        // 有序到期索引避免热路径扫描全部会话；每轮清理有界，过期项从不命中
        for _ in 0..64 {
            if entries
                .expiry
                .first()
                .is_none_or(|(expires, _)| *expires > now)
            {
                break;
            }
            if let Some((_, key)) = entries.expiry.pop_first() {
                entries.scopes.remove(&key);
            }
        }
        if entries.scopes.len() >= self.max_scopes
            && !entries.scopes.contains_key(&scope)
            && let Some((_, key)) = entries.expiry.pop_first()
        {
            entries.scopes.remove(&key);
        }
        let mut binding = entries.scopes.remove(&scope).unwrap_or(Binding {
            digests: VecDeque::new(),
            expires_at: now,
        });
        entries.expiry.remove(&(binding.expires_at, scope));
        if binding.expires_at <= now {
            binding.digests.clear();
        }
        if binding.digests.len() < self.max_digests && !binding.digests.contains(&digest) {
            binding.digests.push_back(digest);
        }
        binding.expires_at = now + TTL;
        entries.expiry.insert((binding.expires_at, scope));
        entries.scopes.insert(scope, binding);
    }

    fn known(&self, scope: Hash) -> Vec<Hash> {
        let Ok(mut entries) = self.entries.lock() else {
            return Vec::new();
        };
        let Some(binding) = entries.scopes.get(&scope) else {
            return Vec::new();
        };
        if binding.expires_at <= Instant::now() {
            let expires = binding.expires_at;
            entries.scopes.remove(&scope);
            entries.expiry.remove(&(expires, scope));
            return Vec::new();
        }
        binding.digests.iter().copied().collect()
    }
}

// 限量序列化只计数，不复制最高 8 MiB 的请求，也不把正文带入共享缓存
struct SizeLimit(usize);
impl Write for SizeLimit {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.0 = self.0.saturating_add(buf.len());
        if self.0 > MAX_REQUEST_BYTES {
            return Err(io::Error::other("request exceeds recovery limit"));
        }
        Ok(buf.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

pub(super) struct Attempt {
    cache: Arc<Cache>,
    scope: Option<Hash>,
}

impl Attempt {
    pub(super) fn prepare(
        request: &CodexResponsesRequest,
        account: &ProviderAccount,
        session: Option<&ProviderSessionAffinityKey>,
        cache: Arc<Cache>,
    ) -> Option<Self> {
        if request.is_guardian()
            || !request.generate()
            || request.force_http_sse
            || request
                .body()
                .get("background")
                .is_some_and(|v| !v.is_null() && v != false)
            || request.input().len() > 2048
            || !request
                .input()
                .iter()
                .any(|item| ciphertext(item).is_some())
            || serde_json::to_writer(SizeLimit(0), request.body()).is_err()
        {
            return None;
        }
        let scope = session.map(|session| {
            let mut hash = Sha256::new();
            let revision = account.revision().get().to_string();
            for part in [
                session.expose_to_store(),
                account.id().as_str(),
                revision.as_str(),
                request
                    .body()
                    .get("model")
                    .and_then(Value::as_str)
                    .unwrap_or_default(),
                account
                    .outbound_proxy()
                    .map(|proxy| proxy.expose_url())
                    .unwrap_or_default(),
            ] {
                hash.update((part.len() as u64).to_le_bytes());
                hash.update(part.as_bytes());
            }
            hash.finalize().into()
        });
        Some(Self { cache, scope })
    }

    pub(super) fn preclean(&self, request: &mut CodexResponsesRequest) -> usize {
        let Some(scope) = self.scope else { return 0 };
        if !self_contained(request) {
            return 0;
        }
        let known = self.cache.known(scope);
        if known.is_empty() {
            return 0;
        }
        let indexes: Vec<_> = request
            .input()
            .iter()
            .enumerate()
            .filter_map(|(index, item)| {
                (repairable(item) && ciphertext(item).is_some_and(|s| known.contains(&digest(s))))
                    .then_some(index)
            })
            .collect();
        for index in &indexes {
            clean_item(request, *index);
        }
        if !indexes.is_empty() {
            clear_request_turn_state(request);
        }
        indexes.len()
    }

    /// 记录不以能否重放为前提；迟到错误仍交给正常解码器和客户端
    pub(super) fn observe(&self, request: &CodexResponsesRequest, chunk: &[u8]) -> Option<usize> {
        if chunk.len() > MAX_ERROR_BYTES {
            return None;
        }
        let text = std::str::from_utf8(chunk).ok()?;
        if !text.contains("invalid_encrypted_content") {
            return None;
        }
        let event: Value = serde_json::from_slice(chunk).ok()?;
        let error = match event.get("type").and_then(Value::as_str)? {
            "error" => event.get("error")?,
            "response.failed" => event.pointer("/response/error")?,
            _ => return None,
        };
        if error.get("code").and_then(Value::as_str) != Some("invalid_encrypted_content") {
            return None;
        }
        let items = request.input();
        let named = error.get("item_id").and_then(Value::as_str).or_else(|| {
            error
                .get("message")?
                .as_str()?
                .split_once("encrypted content for item ")?
                .1
                .split_whitespace()
                .next()
        });
        let mut matches = items.iter().enumerate().filter(|(_, item)| {
            if let Some(id) = named {
                item.get("id").and_then(Value::as_str) == Some(id)
            } else {
                ciphertext(item).is_some()
            }
        });
        let (index, item) = matches.next()?;
        if matches.next().is_some() || !repairable(item) {
            return None;
        }
        if let Some(scope) = self.scope {
            self.cache.remember(scope, digest(ciphertext(item)?));
        }
        Some(index)
    }

    pub(super) fn repaired(
        &self,
        request: &CodexResponsesRequest,
        index: usize,
    ) -> Option<CodexResponsesRequest> {
        if !self_contained(request) || !repairable(request.input().get(index)?) {
            return None;
        }
        let mut clean = request.clone();
        clean_item(&mut clean, index);
        clear_request_turn_state(&mut clean);
        Some(clean)
    }
}

fn digest(value: &str) -> Hash {
    Sha256::digest(value.as_bytes()).into()
}
fn ciphertext(item: &Value) -> Option<&str> {
    item.get("encrypted_content")
        .and_then(Value::as_str)
        .filter(|s| !s.is_empty())
}
fn repairable(item: &Value) -> bool {
    if ciphertext(item).is_none() {
        return false;
    }
    match item.get("type").and_then(Value::as_str) {
        Some("reasoning") => item
            .get("summary")
            .and_then(Value::as_array)
            .is_some_and(|parts| {
                parts.iter().any(|part| {
                    part.get("text")
                        .and_then(Value::as_str)
                        .is_some_and(|s| !s.trim().is_empty())
                })
            }),
        Some("message") | None if item.get("role").and_then(Value::as_str) == Some("assistant") => {
            match item.get("content") {
                Some(Value::String(s)) => !s.trim().is_empty(),
                Some(Value::Array(parts)) => parts.iter().any(|part| {
                    part.get("type").and_then(Value::as_str) == Some("output_text")
                        && part
                            .get("text")
                            .and_then(Value::as_str)
                            .is_some_and(|s| !s.trim().is_empty())
                }),
                _ => false,
            }
        }
        _ => false,
    }
}
fn clean_item(request: &mut CodexResponsesRequest, index: usize) {
    if let Some(item) = request
        .body_mut()
        .get_mut("input")
        .and_then(Value::as_array_mut)
        .and_then(|items| items.get_mut(index))
        .and_then(Value::as_object_mut)
    {
        for key in ["encrypted_content", "id", "status"] {
            item.remove(key);
        }
    }
}

fn self_contained(request: &CodexResponsesRequest) -> bool {
    if !transport_requirement(request).allows_connection_restart()
        || ["previous_response_id", "conversation", "continuation"]
            .iter()
            .any(|key| request.body().get(*key).is_some_and(|v| !v.is_null()))
        || request
            .body()
            .get("background")
            .is_some_and(|v| !v.is_null() && v != false)
    {
        return false;
    }
    // 只重放本体已携带工具调用及其结果的历史，不把增量工具结果变成新会话
    let mut pending = BTreeSet::new();
    for item in request.input() {
        match item.get("type").and_then(Value::as_str) {
            Some("message") | None
                if item
                    .get("role")
                    .and_then(Value::as_str)
                    .is_some_and(|role| {
                        matches!(role, "user" | "assistant" | "system" | "developer")
                    })
                    && (item
                        .get("content")
                        .is_some_and(|content| content.is_string() || content.is_array())
                        || ciphertext(item).is_some()) => {}
            Some("reasoning")
                if item.get("summary").is_some_and(Value::is_array)
                    || ciphertext(item).is_some() => {}
            Some(kind @ ("function_call" | "custom_tool_call")) => {
                let Some(id) = item.get("call_id").and_then(Value::as_str) else {
                    return false;
                };
                if id.is_empty() || !pending.insert((kind, id)) {
                    return false;
                }
            }
            Some(kind @ ("function_call_output" | "custom_tool_call_output")) => {
                let Some(id) = item.get("call_id").and_then(Value::as_str) else {
                    return false;
                };
                let call_type = if kind == "function_call_output" {
                    "function_call"
                } else {
                    "custom_tool_call"
                };
                if !pending.remove(&(call_type, id)) {
                    return false;
                }
            }
            _ => return false,
        }
    }
    pending.is_empty()
}
