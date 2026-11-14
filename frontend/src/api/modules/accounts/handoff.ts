import type { RequestOptions } from '../../request'
import request from '../../request'

export interface AccountHandoffConfig {
  initialNewConcurrency: number
  minimumOldConcurrency: number
  decrement: number
  stableSeconds: number
  minimumSuccesses: number
}
export interface AccountHandoffPolicy {
  enabled: boolean
  enabledAfterMs: number
  revision: number
  config: AccountHandoffConfig
}
export function getAccountHandoffPolicy(options: RequestOptions = {}) {
  return request<AccountHandoffPolicy>({
    url: '/api/admin/accounts/handoffs/policy',
    method: 'GET',
    ...options,
  })
}
export function setAccountHandoffPolicy(data: Pick<AccountHandoffPolicy, 'enabled' | 'revision' | 'config'>) {
  return request<AccountHandoffPolicy>({
    url: '/api/admin/accounts/handoffs/policy',
    method: 'POST',
    data,
  })
}
export interface AccountHandoff {
  id: string
  oldAccountId: string
  newAccountId: string
  oldAccountName: string
  newAccountName: string
  config: AccountHandoffConfig
  phase: 'observing' | 'paused' | 'retaining' | 'completed' | 'cancelled'
  revision: number
  oldLimit: number
  oldInFlight: number
  newInFlight: number
  message: string
  createdAtMs: number
  finishedAtMs: number | null
}
export interface AccountHandoffSnapshot {
  state: AccountHandoff
  feedback: { successes: number, failures: number }
}
export interface AccountHandoffRecord extends AccountHandoffSnapshot {
  createdAtDisplay: string | null
  finishedAtDisplay: string | null
}
export interface AccountHandoffRecords {
  items: AccountHandoffRecord[]
  page: { page: number, pageSize: number, total: number, totalPages: number }
  activeTotal: number
  occupiedAccountIds: string[]
  historyRetentionDays: number
  historyMaximumRecords: number
}
export function getAccountHandoffRecords(params: { view: 'active' | 'history', page: number, pageSize: number }, options: RequestOptions = {}) {
  return request<AccountHandoffRecords>({
    url: '/api/admin/accounts/handoffs/records',
    method: 'GET',
    params,
    ...options,
  })
}
export interface StartAccountHandoff {
  requestId: string
  oldAccountId: string
  newAccountId: string
  config: AccountHandoffConfig
}
export function getAccountHandoffs(options: RequestOptions = {}) {
  return request<AccountHandoffSnapshot[]>({
    url: '/api/admin/accounts/handoffs',
    method: 'GET',
    ...options,
  })
}
export function startAccountHandoff(data: StartAccountHandoff) {
  return request<AccountHandoffSnapshot>({
    url: '/api/admin/accounts/handoffs/start',
    method: 'POST',
    data,
  })
}
export function cancelAccountHandoff(data: { id: string, revision: number }) {
  return request<Record<string, never>>({
    url: '/api/admin/accounts/handoffs/cancel',
    method: 'POST',
    data,
  })
}
