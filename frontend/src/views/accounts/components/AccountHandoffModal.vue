<script setup lang="ts">
import type { AccountHandoff } from '@/api'
import { ZButton, ZConfirmDialog, ZDialog, ZFormItem, ZIconButton, ZInput, ZPagination, ZSegmented, ZSelect, ZSwitch, ZTag } from '@codex-proxy/ui'
import { ArrowRightLeft, RefreshCw, Save, X } from '@lucide/vue'
import { computed, ref, shallowRef, watch } from 'vue'
import { useI18n } from 'vue-i18n'
import { useAccountHandoff } from '../composables/useAccountHandoff'

const open = defineModel<boolean>({ required: true })
const { t } = useI18n({ useScope: 'global' })
const { accounts, items, pending, policy, busy, loading, error, refresh, start, cancel, configure, view, activeTotal, occupied, historyRetentionDays, historyMaximumRecords, pagination, changePage, changePageSize } = useAccountHandoff(open)
const oldId = ref('')
const newId = ref('')
const initial = ref('50')
const minimum = ref('50')
const decrement = ref('30')
const stable = ref('30')
const successes = ref('3')
const cancelling = shallowRef<AccountHandoff | null>(null)
const confirm = ref(false)
const phases = computed(() => ({
  observing: t('accounts.handoff.phases.observing'),
  paused: t('accounts.handoff.phases.paused'),
  retaining: t('accounts.handoff.phases.retaining'),
  completed: t('accounts.handoff.phases.completed'),
  cancelled: t('accounts.handoff.phases.cancelled'),
}))
let loadedRevision: number | null = null
watch(policy, (value) => {
  if (!value || loadedRevision !== null)
    return
  loadedRevision = value.revision
  initial.value = String(value.config.initialNewConcurrency)
  minimum.value = String(value.config.minimumOldConcurrency)
  decrement.value = String(value.config.decrement)
  stable.value = String(value.config.stableSeconds)
  successes.value = String(value.config.minimumSuccesses)
})
watch(open, (value) => {
  if (!value)
    loadedRevision = null
})
const viewOptions = computed(() => [
  {
    value: 'active',
    label: t('accounts.handoff.activeTab'),
  },
  {
    value: 'history',
    label: t('accounts.handoff.historyTab'),
  },
])
const options = computed(() => accounts.value.map(a => ({
  value: a.id,
  label: a.name || a.email || a.id,
  disabled: occupied.value.includes(a.id),
})))
const config = computed(() => ({
  initialNewConcurrency: Number(initial.value),
  minimumOldConcurrency: Number(minimum.value),
  decrement: Number(decrement.value),
  stableSeconds: Number(stable.value),
  minimumSuccesses: Number(successes.value),
}))
const valid = computed(() => oldId.value && newId.value && oldId.value !== newId.value
  && Object.values(config.value).every(n => Number.isInteger(n) && n > 0) && config.value.stableSeconds >= 30)
async function submit() {
  if (valid.value || pending.value)
    await start({ oldAccountId: oldId.value, newAccountId: newId.value, config: config.value })
}
async function stop() {
  if (cancelling.value) {
    if (await cancel(cancelling.value))
      confirm.value = false
  }
}
</script>

<template>
  <ZDialog v-model="open" :title="t('accounts.handoff.title')" width="56rem" :show-close="!busy" :close-on-click-modal="!busy" :close-on-press-escape="!busy">
    <div class="flex min-w-0 flex-col gap-4">
      <div class="flex items-center justify-between gap-2">
        <span class="text-z-sm text-z-text-secondary">{{ t('accounts.handoff.active', { count: activeTotal }) }}</span>
        <ZIconButton :aria-label="t('accounts.handoff.refresh')" :loading="loading" :disabled="busy" @click="refresh(true)">
          <RefreshCw class="size-4" />
        </ZIconButton>
      </div>
      <div class="flex flex-wrap items-center justify-between gap-3">
        <span class="text-z-sm">{{ t('accounts.handoff.automatic') }}</span>
        <div class="flex items-center gap-2">
          <ZSwitch :model-value="policy?.enabled ?? false" :disabled="busy || !policy" :aria-label="t('accounts.handoff.automatic')" @update:model-value="value => configure(value, config)" />
          <ZIconButton :aria-label="t('accounts.handoff.saveDefaults')" :disabled="busy || !policy" @click="configure(policy?.enabled ?? false, config)">
            <Save class="size-4" />
          </ZIconButton>
        </div>
      </div>
      <div class="grid min-w-0 gap-3 sm:grid-cols-2">
        <ZFormItem :label="t('accounts.handoff.old')">
          <ZSelect v-model="oldId" :options="options.filter(o => o.value !== newId)" filterable :disabled="busy || !!pending" :aria-label="t('accounts.handoff.old')" />
        </ZFormItem>
        <ZFormItem :label="t('accounts.handoff.new')">
          <ZSelect v-model="newId" :options="options.filter(o => o.value !== oldId)" filterable :disabled="busy || !!pending" :aria-label="t('accounts.handoff.new')" />
        </ZFormItem>
      </div>
      <div class="grid min-w-0 gap-3 sm:grid-cols-3">
        <ZFormItem :label="t('accounts.handoff.initial')">
          <ZInput v-model="initial" type="number" min="1" max="10000" :disabled="busy || !!pending" :aria-label="t('accounts.handoff.initial')" />
        </ZFormItem>
        <ZFormItem :label="t('accounts.handoff.minimum')">
          <ZInput v-model="minimum" type="number" min="1" max="10000" :disabled="busy || !!pending" :aria-label="t('accounts.handoff.minimum')" />
        </ZFormItem>
        <ZFormItem :label="t('accounts.handoff.decrement')">
          <ZInput v-model="decrement" type="number" min="1" max="1000" :disabled="busy || !!pending" :aria-label="t('accounts.handoff.decrement')" />
        </ZFormItem>
        <ZFormItem :label="t('accounts.handoff.stable')">
          <ZInput v-model="stable" type="number" min="30" max="3600" :disabled="busy || !!pending" :aria-label="t('accounts.handoff.stable')" />
        </ZFormItem>
        <ZFormItem :label="t('accounts.handoff.successes')">
          <ZInput v-model="successes" type="number" min="1" max="10000" :disabled="busy || !!pending" :aria-label="t('accounts.handoff.successes')" />
        </ZFormItem>
        <div class="flex items-end justify-end pb-1">
          <ZButton type="primary" :disabled="!valid && !pending" :loading="busy" @click="submit">
            <ArrowRightLeft class="size-4" />{{ pending ? t('accounts.handoff.retry') : t('accounts.handoff.start') }}
          </ZButton>
        </div>
      </div>
      <p v-if="error" role="alert" class="m-0 text-z-sm text-z-error">
        {{ error }}
      </p>
      <div class="flex flex-wrap items-center justify-between gap-2">
        <ZSegmented v-model="view" :options="viewOptions" :aria-label="t('accounts.handoff.recordsView')" />
        <span v-if="view === 'history'" class="text-z-xs text-z-text-secondary">{{ t('accounts.handoff.retention', { days: historyRetentionDays, count: historyMaximumRecords }) }}</span>
      </div>
      <div class="max-h-80 min-h-24 overflow-auto" :aria-busy="loading">
        <div v-for="item in items" :key="item.state.id" class="flex min-w-0 items-start justify-between gap-3 border-b border-z-border py-3">
          <div class="min-w-0 flex-1">
            <div class="break-all text-z-sm">
              {{ item.state.oldAccountName }} → {{ item.state.newAccountName }}
            </div>
            <div class="mt-1 text-z-xs text-z-text-secondary">
              {{ view === 'history' ? t('accounts.handoff.finalLimit', { limit: item.state.oldLimit }) : t('accounts.handoff.capacity', { old: item.state.oldInFlight, limit: item.state.oldLimit, fresh: item.state.newInFlight }) }}
            </div>
            <div v-if="view === 'history'" class="mt-1 break-words text-z-xs text-z-text-secondary">
              {{ t('accounts.handoff.finishedAt', { time: item.finishedAtDisplay ?? '-' }) }}
            </div>
            <div class="mt-1 text-z-xs text-z-text-secondary">
              {{ view === 'history' ? t('accounts.handoff.endReason', { reason: item.state.message }) : item.state.message }}
            </div>
          </div>
          <ZTag class="shrink-0" :type="item.state.phase === 'paused' ? 'warning' : 'default'">
            {{ phases[item.state.phase] }}
          </ZTag>
          <ZIconButton v-if="view === 'active'" :aria-label="t('accounts.handoff.cancel')" :disabled="busy" @click="cancelling = item.state; confirm = true">
            <X class="size-4" />
          </ZIconButton>
        </div>
        <p v-if="!items.length" class="m-0 py-4 text-z-sm text-z-text-secondary">
          {{ view === 'history' ? t('accounts.handoff.emptyHistory') : t('accounts.handoff.empty') }}
        </p>
      </div>
      <ZPagination v-bind="pagination" :disabled="loading" @current-change="changePage" @size-change="changePageSize" />
    </div>
  </ZDialog>
  <ZConfirmDialog v-model="confirm" :title="t('accounts.handoff.cancel')" :confirm-text="t('accounts.handoff.cancel')" :loading="busy" @confirm="stop">
    {{ t('accounts.handoff.cancelWarning') }}
  </ZConfirmDialog>
</template>
