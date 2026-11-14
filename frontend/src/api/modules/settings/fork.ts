import type { RequestOptions } from '../../request'
import request from '../../request'

export interface ForkHttpRoute { method: string, path: string }
export interface ForkSettingsValues {
  openai: {
    request_compatibility: { lite_context: boolean, store_false: boolean, json_object: boolean, min_output_tokens_16: boolean, tool_structure: boolean }
    model_unsupported_cooldown_seconds: number
    ws_opening_recovery: { enabled: boolean, max_retries: number, retry_delay_ms: number, http_fallback: boolean }
    ws_preconnect: { enabled: boolean, max_connections: number, max_connections_per_account: number, max_connecting: number, starts_per_second: number, idle_timeout_seconds: number, max_retries: number, retry_delay_ms: number, failure_backoff_seconds: number }
    ws_response_recovery: { enabled: boolean, full_history: boolean, encrypted_history: boolean, encrypted_history_max_sessions: number, encrypted_history_max_digests_per_session: number, max_retries: number, window_ms: number, max_prefix_bytes: number, close_codes: number[], disconnects: boolean }
  }
  plugins: {
    maintenance_intervals_seconds: Record<string, number>
    rpc: { maximum_calls: number, maximum_executing_calls: number, maximum_queued_calls: number, maximum_callbacks: number, queue_timeout_ms: number }
    http_filters: Record<string, ForkHttpRoute[]>
  }
}
export interface ForkSettingsSnapshot { revision: string, active: ForkSettingsValues, saved: ForkSettingsValues, restartRequired: boolean }
export function getForkSettings(options: RequestOptions = {}) {
  return request<ForkSettingsSnapshot>({
    url: '/api/admin/settings/fork',
    method: 'GET',
    ...options,
  })
}
export function saveForkSettings(revision: string, values: ForkSettingsValues, options: RequestOptions = {}) {
  return request<ForkSettingsSnapshot>({
    url: '/api/admin/settings/fork',
    method: 'POST',
    data: { revision, values },
    ...options,
  })
}
