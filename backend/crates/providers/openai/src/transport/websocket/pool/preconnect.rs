//! 未发送业务帧的会话备用连接，独立占有、限速、退避及原子领取

use sha2::{Digest, Sha256};
use std::{
    collections::HashMap,
    sync::{Arc, RwLock},
    time::Duration,
};
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;
use uuid::Uuid;

use super::super::{
    coordinator::connect_websocket_connection, error::CodexWebSocketExchangeError,
    model::CodexWebSocketConnection, pump::PumpLogContext,
};
use super::state::{PooledWebSocketConnection, WebSocketPoolState, close_pooled_connections};
use super::{CodexWebSocketPool, CodexWebSocketPoolKey};
use crate::{OpenAiForkRuntime, config::CodexWsPreconnect};

#[derive(Default)]
pub(super) struct PolicySource(RwLock<(CodexWsPreconnect, Option<Arc<OpenAiForkRuntime>>)>);

impl PolicySource {
    fn snapshot(&self) -> CodexWsPreconnect {
        let source = self
            .0
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        source
            .1
            .as_ref()
            .map_or(source.0, |runtime| runtime.snapshot().ws_preconnect)
    }
}

pub(super) struct Entry {
    id: Uuid,
    account: String,
    proxy: String,
    downstream: String,
    expires: Instant,
    cancellation: CancellationToken,
    connection: Option<Box<PooledWebSocketConnection>>,
}

#[derive(Default)]
pub(super) struct State {
    entries: HashMap<String, Entry>,
    blocked: HashMap<String, Instant>,
    next_start: Option<Instant>,
    connecting: usize,
}

impl State {
    fn pause(&mut self, key: &str, wait: Duration, capacity: usize) {
        let until = Instant::now()
            .checked_add(wait)
            .unwrap_or(Instant::now() + Duration::from_secs(86400 * 365));
        if let Some(current) = self.blocked.get_mut(key) {
            // 已有等待即使占满表也必须延长，并保留其他握手收到的较长 Retry-After
            *current = (*current).max(until);
        } else if self.blocked.len() < capacity {
            self.blocked.insert(key.to_owned(), until);
        }
    }

    pub(super) fn remove_downstream(&mut self, downstream: &str) -> Vec<PooledWebSocketConnection> {
        let keys = self
            .entries
            .iter()
            .filter(|(_, entry)| entry.downstream == downstream)
            .map(|(key, _)| key.clone())
            .collect::<Vec<_>>();
        self.remove(&keys)
    }

    pub(super) fn release_proxy_capacity(
        &mut self,
        proxy: &str,
    ) -> (bool, Vec<PooledWebSocketConnection>) {
        // 未使用的储备先让位，不撤销业务连接的续接上下文
        let key = self
            .entries
            .iter()
            .filter(|(_, entry)| entry.proxy == proxy)
            .min_by_key(|(_, entry)| (entry.connection.is_none(), entry.expires))
            .map(|(key, _)| key.clone());
        key.map_or_else(|| (false, Vec::new()), |key| (true, self.remove(&[key])))
    }

    pub(super) fn remove_account(&mut self, account: &str) -> Vec<PooledWebSocketConnection> {
        let keys: Vec<_> = self
            .entries
            .iter()
            .filter(|(_, entry)| entry.account == account)
            .map(|(key, _)| key.clone())
            .collect();
        self.remove(&keys)
    }

    fn remove(&mut self, keys: &[String]) -> Vec<PooledWebSocketConnection> {
        keys.iter()
            .filter_map(|key| self.entries.remove(key))
            .filter_map(|entry| {
                entry.cancellation.cancel();
                entry.connection.map(|connection| *connection)
            })
            .collect()
    }

    pub(super) fn clear(&mut self) -> Vec<PooledWebSocketConnection> {
        self.remove(&self.entries.keys().cloned().collect::<Vec<_>>())
    }
}

// 不省略会话、安装身份、Cookie、Authorization 或业务扩展头
// 随机握手键及追踪请求 ID 不参与复用，其余头必须一致
fn fingerprint(key: &CodexWebSocketPoolKey, opening: &CodexWebSocketConnection) -> String {
    let mut hash = Sha256::new();
    if let Some((owner, revision)) = &opening.preconnect_identity {
        hash.update(owner.len().to_le_bytes());
        hash.update(owner.as_bytes());
        hash.update(revision.to_le_bytes());
    }
    hash.update(key.identity_digest().as_bytes());
    hash.update(opening.endpoint().as_bytes());
    for (name, value) in opening.headers() {
        if name.eq_ignore_ascii_case("sec-websocket-key")
            || name.eq_ignore_ascii_case("x-client-request-id")
            || name.eq_ignore_ascii_case("x-request-id")
        {
            continue;
        }
        hash.update(name.len().to_le_bytes());
        hash.update(name.as_bytes());
        hash.update(value.len().to_le_bytes());
        hash.update(value.as_bytes());
    }
    hex::encode(hash.finalize())
}

impl CodexWebSocketPool {
    pub fn with_preconnect(
        self,
        initial: CodexWsPreconnect,
        runtime: Option<Arc<OpenAiForkRuntime>>,
    ) -> Self {
        *self
            .preconnect_policy
            .0
            .write()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = (initial, runtime);
        self
    }

    pub(crate) fn preconnect_enabled(&self) -> bool {
        self.config.enabled
            && self.preconnect_policy.snapshot().enabled
            && !self.shutdown.is_cancelled()
    }

    pub(crate) fn schedule_preconnect(
        &self,
        key: CodexWebSocketPoolKey,
        mut opening: CodexWebSocketConnection,
        downstream: Option<&gateway_core::lifecycle::CancellationToken>,
    ) {
        let policy = self.preconnect_policy.snapshot();
        if !policy.enabled
            || !self.config.enabled
            || self.shutdown.is_cancelled()
            || opening.preconnect_identity.is_none()
            || downstream.is_some_and(gateway_core::lifecycle::CancellationToken::is_cancelled)
        {
            return;
        }
        if let Some(downstream) = downstream {
            self.register_downstream(&key, downstream);
        }
        let scope = fingerprint(&key, &opening);
        let account = key.account_id().to_owned();
        let block_key = format!("{}:{}", account, key.proxy_capacity_key);
        let id = Uuid::new_v4();
        let cancellation = self.shutdown.child_token();
        let now = Instant::now();
        {
            let mut state = self.lock_state();
            if state.shutting_down || state.downstream_closed(&key) {
                return;
            }
            let state = &mut state.preconnect;
            state.blocked.retain(|_, until| *until > now);
            if state.entries.contains_key(&scope)
                || state.entries.len() >= policy.max_connections
                || state.blocked.contains_key(&block_key)
                || state.blocked.len() >= policy.max_connections
                || state
                    .entries
                    .values()
                    .filter(|entry| entry.account == account)
                    .count()
                    >= policy.max_connections_per_account
            {
                return;
            }
            state.entries.insert(
                scope.clone(),
                Entry {
                    id,
                    account: account.clone(),
                    proxy: key.proxy_capacity_key.clone(),
                    downstream: key.downstream_connection_id.clone(),
                    expires: now + Duration::from_secs(policy.idle_timeout_seconds),
                    cancellation: cancellation.clone(),
                    connection: None,
                },
            );
        }
        // 后台生命周期独立于触发请求的建连总期限，不保留请求正文或真实请求租约
        opening.connection_budget = None;
        let pool = self.clone();
        self.spawn_connect_task(async move {
            let outcome = tokio::select! {
                biased;
                _ = cancellation.cancelled() => None,
                _ = tokio::time::sleep(Duration::from_secs(policy.idle_timeout_seconds)) => None,
                result = pool.run_preconnect(&key, &opening, &block_key, policy) => result,
            };
            let mut close = None;
            {
                let mut state = pool.lock_state();
                let enabled = pool.preconnect_enabled();
                let owns = state
                    .preconnect
                    .entries
                    .get(&scope)
                    .is_some_and(|entry| entry.id == id);
                match outcome {
                    Some(connection) if owns && enabled && !cancellation.is_cancelled() => {
                        let entry = state
                            .preconnect
                            .entries
                            .get_mut(&scope)
                            .expect("checked reservation");
                        entry.expires =
                            Instant::now() + Duration::from_secs(policy.idle_timeout_seconds);
                        entry.connection = Some(Box::new(connection));
                        tracing::info!(
                            account_id = account,
                            event = "ready",
                            "WS standby connection"
                        );
                    }
                    other => {
                        if owns {
                            state.preconnect.entries.remove(&scope);
                        }
                        close = other;
                    }
                }
            }
            if let Some(connection) = close {
                connection.websocket.close_and_wait().await;
            }
            pool.capacity_changed.notify_waiters();
        });
    }

    async fn run_preconnect(
        &self,
        key: &CodexWebSocketPoolKey,
        opening: &CodexWebSocketConnection,
        block_key: &str,
        policy: CodexWsPreconnect,
    ) -> Option<PooledWebSocketConnection> {
        for retry in 0..=policy.max_retries {
            loop {
                if !self.preconnect_enabled() {
                    return None;
                }
                if self
                    .lock_state()
                    .preconnect
                    .blocked
                    .get(block_key)
                    .is_some_and(|until| *until > Instant::now())
                {
                    return None;
                }
                if let Some((permit, proxy, active)) =
                    self.try_preconnect_start(opening, self.preconnect_policy.snapshot())
                {
                    // 计数守卫覆盖取消与超时，不能依赖 await 后的显式递减
                    let result = connect_websocket_connection(
                        opening,
                        PumpLogContext::new(
                            Some(key.account_id().to_owned()),
                            Some(key.conversation_id_hash()),
                        ),
                        true,
                        proxy,
                    )
                    .await;
                    drop(permit);
                    match result {
                        Ok((connection, _)) => return Some(connection),
                        Err(error) => {
                            let wait = retry_delay(&error, policy, retry);
                            let status = match error.classified() {
                                CodexWebSocketExchangeError::Upstream(error) => error.status_code,
                                _ => 0,
                            };
                            tracing::info!(
                                account_id = key.account_id(),
                                upstream_status = status,
                                retry,
                                retryable = wait.is_some(),
                                event = "opening_failed",
                                "WS standby connection"
                            );
                            drop(active);
                            // 同账号出口共享等待，新的补建任务不能跳过 Retry-After
                            if let Some(wait) = wait {
                                self.lock_state().preconnect.pause(
                                    block_key,
                                    wait,
                                    policy.max_connections,
                                );
                            }
                            if retry < policy.max_retries
                                && let Some(wait) = wait
                                && wait <= Duration::from_secs(policy.idle_timeout_seconds)
                            {
                                tokio::time::sleep(wait).await;
                                break;
                            }
                            let wait = wait
                                .unwrap_or_default()
                                .max(Duration::from_secs(policy.failure_backoff_seconds));
                            self.lock_state().preconnect.pause(
                                block_key,
                                wait,
                                policy.max_connections,
                            );
                            return None;
                        }
                    }
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        }
        None
    }

    fn try_preconnect_start(
        &self,
        opening: &CodexWebSocketConnection,
        policy: CodexWsPreconnect,
    ) -> Option<(
        tokio::sync::OwnedSemaphorePermit,
        Option<super::ProxyConnectionPermit>,
        ActiveOpening,
    )> {
        // 为真实请求留下建连名额，后台任务不在这里排队持有 permit
        if self.connect_semaphore.available_permits() <= 1 {
            return None;
        }
        let permit = self.connect_semaphore.clone().try_acquire_owned().ok()?;
        let proxy = self
            .try_preconnect_proxy_capacity(opening.outbound_proxy.as_ref())
            .ok()?;
        let mut state = self.lock_state();
        let state = &mut state.preconnect;
        if state.connecting >= policy.max_connecting
            || state.next_start.is_some_and(|at| at > Instant::now())
        {
            return None;
        }
        state.connecting += 1;
        state.next_start = Some(
            Instant::now()
                + Duration::from_millis(1000_u64.div_ceil(u64::from(policy.starts_per_second))),
        );
        Some((permit, proxy, ActiveOpening(self.clone())))
    }

    pub(super) fn take_preconnected(
        &self,
        state: &mut WebSocketPoolState,
        key: &CodexWebSocketPoolKey,
        opening: &CodexWebSocketConnection,
    ) -> Option<Box<PooledWebSocketConnection>> {
        if !self.preconnect_enabled() {
            return None;
        }
        let scope = fingerprint(key, opening);
        let ready = state.preconnect.entries.get(&scope).is_some_and(|entry| {
            entry.expires > Instant::now()
                && entry.connection.as_ref().is_some_and(|connection| {
                    !connection.websocket.is_closed()
                        && connection.created_at.elapsed() < self.config.max_age
                })
        });
        if !ready {
            return None;
        }
        let entry = state.preconnect.entries.remove(&scope)?;
        entry.cancellation.cancel();
        tracing::info!(
            account_id = key.account_id(),
            event = "hit",
            "WS standby connection"
        );
        entry.connection
    }

    pub(super) async fn maintain_preconnections(&self) {
        let policy = self.preconnect_policy.snapshot();
        let close = {
            let mut state = self.lock_state();
            let state = &mut state.preconnect;
            state.blocked.retain(|_, until| *until > Instant::now());
            let keys: Vec<_> = state
                .entries
                .iter()
                .filter(|(_, entry)| {
                    !policy.enabled
                        || entry.expires <= Instant::now()
                        || entry.connection.as_ref().is_some_and(|connection| {
                            connection.websocket.is_closed()
                                || connection.created_at.elapsed() >= self.config.max_age
                        })
                })
                .map(|(key, _)| key.clone())
                .collect();
            let mut close = state.remove(&keys);
            let mut entries: Vec<_> = state
                .entries
                .iter()
                .map(|(key, entry)| (key.clone(), entry.account.clone(), entry.expires))
                .collect();
            entries.sort_by_key(|(_, _, expires)| std::cmp::Reverse(*expires));
            let mut per_account = HashMap::<String, usize>::new();
            let mut retained = 0;
            let excess: Vec<_> = entries
                .into_iter()
                .filter_map(|(key, account, _)| {
                    let count = per_account.entry(account).or_default();
                    if retained >= policy.max_connections
                        || *count >= policy.max_connections_per_account
                    {
                        Some(key)
                    } else {
                        *count += 1;
                        retained += 1;
                        None
                    }
                })
                .collect();
            close.extend(state.remove(&excess));
            if !keys.is_empty() || !excess.is_empty() {
                tracing::info!(
                    removed = keys.len() + excess.len(),
                    retained,
                    event = "reclaimed",
                    "WS standby connection"
                );
            }
            close
        };
        close_pooled_connections(close).await;
    }
}

struct ActiveOpening(CodexWebSocketPool);
impl Drop for ActiveOpening {
    fn drop(&mut self) {
        self.0.lock_state().preconnect.connecting -= 1;
    }
}

fn retry_delay(
    error: &CodexWebSocketExchangeError,
    policy: CodexWsPreconnect,
    retry: u32,
) -> Option<Duration> {
    let suggested = match error.classified() {
        CodexWebSocketExchangeError::Upstream(error)
            if crate::transport::diagnostics::empty_opening_rejection(
                error.status_code,
                &error.body,
                &error.diagnostics,
                error.send_phase,
            ) || matches!(error.status_code, 429 | 500 | 502 | 503 | 504) =>
        {
            error.retry_after_seconds
        }
        CodexWebSocketExchangeError::ConnectTimeout { .. } => None,
        CodexWebSocketExchangeError::Connect {
            source: tungstenite::Error::Io(error),
            ..
        } if matches!(
            error.kind(),
            std::io::ErrorKind::ConnectionReset
                | std::io::ErrorKind::ConnectionAborted
                | std::io::ErrorKind::TimedOut
                | std::io::ErrorKind::ConnectionRefused
        ) =>
        {
            None
        }
        _ => return None,
    };
    let jitter = u64::from(Uuid::new_v4().as_bytes()[0]);
    Some(
        Duration::from_millis(policy.retry_delay_ms.saturating_mul(1 << retry.min(5)) + jitter)
            .max(Duration::from_secs(suggested.unwrap_or(0))),
    )
}
