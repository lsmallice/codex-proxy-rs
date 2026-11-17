import type { RequestOptions } from '../../request'
import request from '../../request'

export interface AccountHandoffConfig {
  transferGroup?: AccountTransferConfig | null
  newAccountProtection?: AccountWarmupConfig | null
  initialNewConcurrency: number
  minimumOldConcurrency: number
  decrement: number
  stableSeconds: number
  minimumSuccesses: number
}
export interface AccountTransferConfig { remainingPercent: number, healthyMinutes: number, remainingMinutes: number, minimumMatureSeconds: number, preparationSeconds: number }
export interface TransferMember { id: string, name: string, baseline: number, limit: number, inFlight: number, remainingSeconds: number | null, remainingBasisPoints: number | null, terminal: boolean }
export interface AccountTransferSnapshot { state: { id: string, groupId: string, revision: number, config: AccountHandoffConfig, old: TransferMember[], new: TransferMember[], budget: number, message: string }, feedback: { successes: number, failures: number } }
export interface StartAccountTransfer { id: string, revision: number, groupId: string, oldAccountIds: string[], newAccountIds: string[], config: AccountHandoffConfig }
export function getAccountTransferGroups(options: RequestOptions = {}) {
  return request<AccountTransferSnapshot[]>({ url: '/api/admin/accounts/handoffs/groups', method: 'GET', ...options })
}
export function saveAccountTransferGroup(data: StartAccountTransfer) {
  return request<AccountTransferSnapshot>({ url: '/api/admin/accounts/handoffs/groups', method: 'POST', data })
}
export function cancelAccountTransferGroup(id: string, revision: number) {
  return request<Record<string, never>>({
    url: '/api/admin/accounts/handoffs/groups/cancel',
    method: 'POST',
    data: { id, revision },
  })
}
export interface AccountWarmupConfig {
  initialConcurrency: number
  increment: number
  releaseConcurrency: number
  stableSeconds: number
  minimumSuccesses: number
}
export interface AccountWarmupSnapshot {
  state: { accountId: string, accountName: string, revision: number, limit: number, inFlight: number, message: string }
  feedback: { successes: number, failures: number }
}
export function getAccountWarmups(options: RequestOptions = {}) {
  return request<AccountWarmupSnapshot[]>({ url: '/api/admin/accounts/warmups', method: 'GET', ...options })
}
export function cancelAccountWarmup(id: string, revision: number) {
  return request<Record<string, never>>({
    url: '/api/admin/accounts/warmups/cancel',
    method: 'POST',
    data: { id, revision },
  })
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
