import type { Ref } from 'vue'
import type { Account, AccountHandoff, AccountHandoffConfig, AccountHandoffPolicy, StartAccountHandoff } from '@/api'
import { computed, onScopeDispose, shallowRef, watch } from 'vue'
import { cancelAccountHandoff, getAccountHandoffPolicy, getAccountHandoffRecords, getAccounts, setAccountHandoffPolicy, startAccountHandoff } from '@/api'
import { ApiError } from '@/api/request'
import { useAsyncAction } from '@/composables/useAsyncAction'
import { usePagedQuery } from '@/composables/usePagedQuery'
import { useRequestState } from '@/composables/useRequestState'

export function useAccountHandoff(open: Ref<boolean>) {
  const query = useRequestState()
  const { loading: busy, run } = useAsyncAction()
  const accounts = shallowRef<Account[]>([])
  const policy = shallowRef<AccountHandoffPolicy | null>(null)
  const view = shallowRef<'active' | 'history'>('active')
  const occupied = shallowRef<string[]>([])
  const activeTotal = shallowRef(0)
  const historyRetentionDays = shallowRef(7)
  const historyMaximumRecords = shallowRef(10000)
  const pending = shallowRef<StartAccountHandoff | null>(null)
  const records = usePagedQuery({
    initialPageSize: 20,
    load: (page, options) => getAccountHandoffRecords({ ...page, view: view.value }, options),
    onSuccess: (result) => {
      occupied.value = result.occupiedAccountIds
      activeTotal.value = result.activeTotal
      historyRetentionDays.value = result.historyRetentionDays
      historyMaximumRecords.value = result.historyMaximumRecords
      if (pending.value && result.items.some(v => v.state.id === pending.value?.requestId))
        pending.value = null
    },
  })
  const pagination = computed(() => ({ currentPage: records.page.value, pageSize: records.pageSize.value, total: records.total.value }))
  let timer: ReturnType<typeof setTimeout> | undefined
  function scheduleRefresh() {
    clearTimeout(timer)
    if (open.value)
      timer = setTimeout(() => void refresh(), 5000)
  }
  async function refresh(catalog = false) {
    clearTimeout(timer)
    const generation = query.start()
    try {
      const options = { signal: query.signal, silent: true }
      await records.execute({ silent: !catalog })
      const currentPolicy = await getAccountHandoffPolicy(options)
      if (!query.isCurrent(generation))
        return
      policy.value = currentPolicy
      if (catalog) {
        const available: Account[] = []
        for (let page = 1; page <= 20; page++) {
          const result = await getAccounts({
            provider: 'openai',
            status: 'normal',
            page,
            pageSize: 200,
          }, options)
          if (!query.isCurrent(generation))
            return
          available.push(...result.items.filter(a => a.authenticationKind === 'oauth'))
          if (page >= result.page.totalPages)
            break
        }
        accounts.value = available
      }
    }
    catch (error) {
      query.fail(generation, error)
    }
    finally {
      query.finish(generation)
      if (open.value && query.isCurrent(generation)) {
        scheduleRefresh()
      }
    }
  }
  async function changePage(page: number) {
    clearTimeout(timer)
    query.invalidate()
    await records.execute({ page })
    scheduleRefresh()
  }
  async function changePageSize(pageSize: number) {
    clearTimeout(timer)
    query.invalidate()
    await records.execute({ page: 1, pageSize })
    scheduleRefresh()
  }
  watch(view, async () => {
    clearTimeout(timer)
    query.invalidate()
    records.invalidate()
    if (open.value) {
      await records.reloadFromStart()
      scheduleRefresh()
    }
  })
  async function start(command: Omit<StartAccountHandoff, 'requestId'>) {
    await run(async () => {
      pending.value ??= {
        ...command,
        requestId: crypto.randomUUID(),
      }
      try {
        await startAccountHandoff(pending.value)
      }
      catch (error) {
        if (error instanceof ApiError && error.status >= 400 && error.status < 500)
          pending.value = null
        throw error
      }
      pending.value = null
      await refresh(true)
    })
  }
  async function cancel(item: AccountHandoff) {
    return await run(async () => {
      await cancelAccountHandoff({ id: item.id, revision: item.revision })
      await refresh(true)
      return true
    })
  }
  async function configure(enabled: boolean, config: AccountHandoffConfig) {
    if (!policy.value)
      return
    await run(async () => {
      try {
        policy.value = await setAccountHandoffPolicy({ enabled, config, revision: policy.value!.revision })
      }
      catch (error) {
        if (error instanceof ApiError && error.status === 409)
          await refresh()
        throw error
      }
    })
  }
  watch(open, (value) => {
    clearTimeout(timer)
    query.invalidate()
    records.invalidate()
    if (value)
      void refresh(true)
  })
  onScopeDispose(() => clearTimeout(timer))
  return {
    accounts,
    items: records.items,
    view,
    activeTotal,
    occupied,
    historyRetentionDays,
    historyMaximumRecords,
    pagination,
    changePage,
    changePageSize,
    pending,
    policy,
    busy,
    loading: computed(() => query.loading.value || records.loading.value),
    error: computed(() => query.error.value || records.error.value),
    refresh,
    start,
    cancel,
    configure,
  }
}
