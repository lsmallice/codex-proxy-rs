import type { RequestOptions } from '../request'
import type { RequestLocation, RequestLocationDetection } from '../types/location'
import type { AccountUpstream } from './accounts'
import type { AccountGroupRef } from './groups'
import request from '../request'

export interface DetectedProxyLocation {
  location: RequestLocation
  exitIpv4: string | null
  exitIpv6: string | null
  detectedAt: string
}

export interface OutboundProxyTest {
  location: RequestLocationDetection
  success: boolean
  latencyMs: number
  exitIp: string | null
  exitIpv4: string | null
  exitIpv6: string | null
  message: string
}

export interface OutboundProxyRecord {
  managedIpv6: ManagedIpv6Lease | null
  maxWebsocketConnections: number
  enabled: boolean
  autoLocation: boolean
  detectedLocation: DetectedProxyLocation | null
  location: RequestLocation | null
  id: string
  name: string
  endpoint: string
  hasAuthentication: boolean
  revision: number
  accountCount: number
  lastTestAt: string | null
  lastTestAtDisplay: string | null
  lastTest: OutboundProxyTest | null
  createdAt: string
  createdAtDisplay: string
  updatedAt: string
  updatedAtDisplay: string
}

export interface ManagedIpv6Lease {
  requestId: string
  controllerId: string
  name: string
  state: 'queued' | 'allocating' | 'unknown' | 'pending' | 'active' | 'retiring' | 'retired'
  address: string | null
  proxyId: string | null
  ownerAccountId: string | null
}

export interface ManagedIpv6Status {
  configured: boolean
  available: boolean
  controllerId: string | null
  prefix: string | null
  issued: string | null
  message: string | null
}

export interface ManagedIpv6Policy {
  enabled: boolean
  controllerId: string | null
  revision: number
}

interface ProxyPage {
  items: OutboundProxyRecord[]
  page: { page: number, pageSize: number, total: number, totalPages: number }
}

export interface OutboundProxyAccount {
  upstream: AccountUpstream | null
  id: string
  name: string
  email: string | null
  provider: string
  authenticationKind: string
  planType: string | null
  planTypeDisplay: string | null
  groups: AccountGroupRef[]
  enabled: boolean
}

interface ProxyAccountPage {
  items: OutboundProxyAccount[]
  page: ProxyPage['page']
}

interface ProxyListParams {
  page: number
  pageSize: number
  search?: string
}

interface ProxyAccountListParams extends ProxyListParams {
  proxyId: string
}

interface ProxyAccountRemoveParam {
  proxyId: string
  accountId: string
}

interface ProxyCreateParam {
  name: string
  proxyUrl: string
  maxWebsocketConnections?: number
  autoLocation?: boolean
  location?: RequestLocation | null
}

interface ProxyIdParam {
  id: string
  revision: number
}

interface ProxyUpdateParam extends ProxyIdParam {
  name: string
  enabled?: boolean
  maxWebsocketConnections?: number
  proxyUrl?: string
  autoLocation?: boolean
  location?: RequestLocation | null
}

interface ProxyTestParam extends ProxyIdParam {
  detectLocation?: boolean
}

interface ProxyProbeParam {
  useDeploymentProxy?: boolean
  proxyUrl: string
  detectLocation?: boolean
}

interface ProxyMutationResponse {
  configRevision: number
}

export function getProxyAccounts(data: ProxyAccountListParams, options: RequestOptions = {}) {
  return request<ProxyAccountPage>({
    url: '/api/admin/proxies/accounts',
    method: 'GET',
    params: data,
    ...options,
  })
}

export function removeProxyAccount(data: ProxyAccountRemoveParam) {
  return request<ProxyMutationResponse>({
    url: '/api/admin/proxies/accounts/remove',
    method: 'POST',
    data,
  })
}

export type ProxyBatchResult<T>
  = | { status: 'succeeded', data: T }
    | { status: 'failed', httpStatus: number, code: number, message: string }
    | { status: 'skipped' }

interface ProxyMutation extends ProxyMutationResponse {
  record: OutboundProxyRecord
}

export function getProxies(data: ProxyListParams, options: RequestOptions = {}) {
  return request<ProxyPage>({
    url: '/api/admin/proxies',
    method: 'GET',
    params: data,
    ...options,
  })
}

export function createProxy(data: ProxyCreateParam[], options: RequestOptions = {}) {
  return request<ProxyBatchResult<ProxyMutation>[]>({
    url: '/api/admin/proxies/create',
    method: 'POST',
    data,
    timeout: 25000 * Math.max(1, data.length),
    ...options,
  })
}

export function updateProxy(data: ProxyUpdateParam) {
  return request<ProxyMutation>({
    url: '/api/admin/proxies/update',
    method: 'POST',
    data,
    timeout: 25000,
  })
}

export function deleteProxy(data: ProxyIdParam) {
  return request<ProxyMutationResponse>({
    url: '/api/admin/proxies/delete',
    method: 'POST',
    data,
  })
}

export function testProxy(data: ProxyTestParam[], options: RequestOptions = {}) {
  return request<ProxyBatchResult<OutboundProxyRecord>[]>({
    url: '/api/admin/proxies/test',
    method: 'POST',
    data,
    timeout: 25000 * Math.max(1, Math.ceil(data.length / 2)),
    ...options,
  })
}

export function probeProxy(data: ProxyProbeParam, options: RequestOptions = {}) {
  return request<OutboundProxyTest>({
    url: '/api/admin/proxies/probe',
    method: 'POST',
    data,
    timeout: 25000,
    ...options,
  })
}

export function getManagedIpv6Status(options: RequestOptions = {}) {
  return request<ManagedIpv6Status>({ url: '/api/admin/proxies/ipv6/status', method: 'GET', ...options })
}

export function getManagedIpv6Leases(options: RequestOptions = {}) {
  return request<ManagedIpv6Lease[]>({ url: '/api/admin/proxies/ipv6/leases', method: 'GET', ...options })
}

export function allocateManagedIpv6(data: { requestId: string, name: string, accountId?: string | null }) {
  return request<ManagedIpv6Lease>({
    url: '/api/admin/proxies/ipv6/allocate',
    method: 'POST',
    data,
    timeout: 30000,
  })
}

export function getManagedIpv6Policy(options: RequestOptions = {}) {
  return request<ManagedIpv6Policy>({
    url: '/api/admin/proxies/ipv6/policy',
    method: 'GET',
    ...options,
  })
}

export function setManagedIpv6Policy(data: { enabled: boolean, revision: number }) {
  return request<ManagedIpv6Policy>({
    url: '/api/admin/proxies/ipv6/policy',
    method: 'POST',
    data,
  })
}

export function retireManagedIpv6(data: { requestId: string }) {
  return request<ManagedIpv6Lease>({
    url: '/api/admin/proxies/ipv6/retire',
    method: 'POST',
    data,
    timeout: 30000,
  })
}
