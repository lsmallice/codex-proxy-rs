<script setup lang="ts">
import type { ManagedIpv6Lease, ManagedIpv6Policy, ManagedIpv6Status } from '@/api'
import { ZButton, ZConfirmDialog, ZDialog, ZFormItem, ZIconButton, ZInput, ZSwitch, ZTag } from '@codex-proxy/ui'
import { Network, RefreshCw, Trash2 } from '@lucide/vue'
import { computed, onScopeDispose, shallowRef, watch } from 'vue'
import { useI18n } from 'vue-i18n'
import { allocateManagedIpv6, getManagedIpv6Leases, getManagedIpv6Policy, getManagedIpv6Status, retireManagedIpv6, setManagedIpv6Policy } from '@/api'
import { useAsyncAction } from '@/composables/useAsyncAction'
import { useRequestState } from '@/composables/useRequestState'

const emit = defineEmits<{ changed: [] }>()
const open = defineModel<boolean>({ required: true })
const { t } = useI18n({ useScope: 'global' })
const stateLabels = computed(() => ({
  queued: t('proxies.ipv6.states.queued'),
  allocating: t('proxies.ipv6.states.allocating'),
  unknown: t('proxies.ipv6.states.unknown'),
  pending: t('proxies.ipv6.states.pending'),
  active: t('proxies.ipv6.states.active'),
  retiring: t('proxies.ipv6.states.retiring'),
  retired: t('proxies.ipv6.states.retired'),
}))
const status = shallowRef<ManagedIpv6Status | null>(null)
const policy = shallowRef<ManagedIpv6Policy | null>(null)
const leases = shallowRef<ManagedIpv6Lease[]>([])
const name = shallowRef('')
const requestId = shallowRef<string | null>(null)
const submittedName = shallowRef('')
const retiring = shallowRef<ManagedIpv6Lease | null>(null)
const confirmRetire = shallowRef(false)
const query = useRequestState()
const { loading, error } = query
const { loading: busy, run } = useAsyncAction()
let poll: ReturnType<typeof setTimeout> | undefined

async function refresh() {
  const generation = query.start()
  try {
    const options = { signal: query.signal, silent: true }
    const results = await Promise.allSettled([
      getManagedIpv6Status(options),
      getManagedIpv6Policy(options),
      getManagedIpv6Leases(options),
    ])
    if (!query.isCurrent(generation))
      return
    const [statusResult, policyResult, leasesResult] = results
    if (statusResult.status === 'fulfilled')
      status.value = statusResult.value
    else
      status.value = null
    if (policyResult.status === 'fulfilled')
      policy.value = policyResult.value
    if (leasesResult.status === 'fulfilled') {
      leases.value = leasesResult.value
      if (requestId.value && leases.value.some(lease => lease.requestId === requestId.value)) {
        requestId.value = null
        name.value = ''
      }
    }
    for (const result of results) {
      if (result.status === 'rejected')
        query.fail(generation, result.reason)
    }
  }
  finally {
    query.finish(generation)
    if (open.value && query.isCurrent(generation)) {
      clearTimeout(poll)
      poll = setTimeout(() => void refresh(), 5000)
    }
  }
}

async function allocate() {
  if (!name.value.trim() || !status.value?.available)
    return
  await run(async () => {
    if (!requestId.value) {
      requestId.value = crypto.randomUUID()
      submittedName.value = name.value.trim()
    }
    await allocateManagedIpv6({ requestId: requestId.value, name: submittedName.value })
    requestId.value = null
    name.value = ''
    emit('changed')
    await refresh()
  })
}

async function changePolicy(enabled: boolean) {
  if (!policy.value)
    return
  await run(async () => {
    policy.value = await setManagedIpv6Policy({ enabled, revision: policy.value!.revision })
    await refresh()
  })
}

async function retire() {
  const lease = retiring.value
  if (!lease)
    return
  await run(async () => {
    await retireManagedIpv6({ requestId: lease.requestId })
    confirmRetire.value = false
    emit('changed')
    await refresh()
  })
}

watch(open, (value) => {
  clearTimeout(poll)
  query.invalidate()
  if (value)
    void refresh()
})
onScopeDispose(() => clearTimeout(poll))
</script>

<template>
  <ZDialog v-model="open" :title="t('proxies.ipv6.title')" width="52rem" :show-close="!busy" :close-on-click-modal="!busy" :close-on-press-escape="!busy">
    <div class="flex min-w-0 flex-col gap-4">
      <div class="flex flex-wrap items-center justify-between gap-2">
        <div class="min-w-0">
          <span class="break-all font-mono text-z-sm">{{ status?.prefix ?? t('proxies.ipv6.unconfigured') }}</span>
          <ZTag class="ml-2" :type="status?.available ? 'success' : 'warning'">
            {{ status?.available ? t('proxies.ipv6.available') : t('proxies.ipv6.unavailable') }}
          </ZTag>
        </div>
        <ZIconButton :aria-label="t('proxies.ipv6.refresh')" :loading="loading" :disabled="busy" @click="refresh">
          <RefreshCw class="size-4" />
        </ZIconButton>
      </div>
      <div class="flex items-center justify-between gap-4">
        <span class="text-z-sm">{{ t('proxies.ipv6.autoBind') }}</span>
        <ZSwitch :model-value="policy?.enabled ?? false" :disabled="busy || !policy || (!policy.enabled && !status?.available)" :aria-label="t('proxies.ipv6.autoBind')" @update:model-value="changePolicy" />
      </div>
      <p class="m-0 text-z-xs text-z-text-secondary">
        {{ t('proxies.ipv6.autoHelp') }}
      </p>
      <div class="flex flex-col items-stretch gap-2 sm:flex-row sm:items-end">
        <ZFormItem :label="t('proxies.ipv6.name')" class="min-w-0 flex-1">
          <ZInput v-model="name" :disabled="busy || !!requestId" :maxlength="100" :aria-label="t('proxies.ipv6.name')" :placeholder="t('proxies.ipv6.namePlaceholder')" />
        </ZFormItem>
        <ZButton type="primary" class="mb-1" :loading="busy" :disabled="!name.trim() || !status?.available" @click="allocate">
          <Network class="size-4" />
          {{ requestId ? t('proxies.ipv6.checkRequest') : t('proxies.ipv6.allocate') }}
        </ZButton>
      </div>
      <p v-if="error" role="alert" class="m-0 text-z-sm text-z-error">
        {{ error }}
      </p>
      <div class="max-h-80 overflow-y-auto">
        <div v-for="lease in leases" :key="lease.requestId" class="flex min-w-0 items-center justify-between gap-3 py-3">
          <div class="min-w-0 flex-1">
            <div class="truncate text-z-sm" :title="lease.name">
              {{ lease.name }}
            </div>
            <div class="break-all font-mono text-z-xs text-z-text-secondary">
              {{ lease.address ?? '-' }}
            </div>
            <div v-if="lease.ownerAccountId" class="truncate text-z-xs text-z-text-secondary">
              {{ lease.ownerAccountId }}
            </div>
          </div>
          <ZTag class="shrink-0" :type="lease.state === 'active' ? 'success' : 'default'">
            {{ stateLabels[lease.state] }}
          </ZTag>
          <ZIconButton :disabled="busy || lease.state === 'retired' || lease.state === 'retiring'" :aria-label="t('proxies.ipv6.retire')" @click="retiring = lease; confirmRetire = true">
            <Trash2 class="size-4 text-z-error" />
          </ZIconButton>
        </div>
        <p v-if="!leases.length" class="m-0 py-4 text-z-sm text-z-text-secondary">
          {{ t('proxies.ipv6.empty') }}
        </p>
      </div>
    </div>
  </ZDialog>
  <ZConfirmDialog v-model="confirmRetire" :title="t('proxies.ipv6.retire')" :confirm-text="t('proxies.ipv6.retire')" :loading="busy" destructive @confirm="retire">
    <p class="m-0 break-all">
      {{ retiring?.address ?? retiring?.name }}
    </p>
    <p class="mb-0 mt-3 text-z-sm text-z-text-secondary">
      {{ t('proxies.ipv6.retireHelp') }}
    </p>
  </ZConfirmDialog>
</template>
