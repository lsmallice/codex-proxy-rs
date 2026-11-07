<script setup lang="ts">
import { ZButton, ZCheckbox, ZFormItem, ZIconButton, ZInput, ZSegmented, ZSelect, ZSwitch, ZTag } from '@codex-proxy/ui'
import { Plus, RefreshCw, Save, Trash2, Undo2 } from '@lucide/vue'
import { computed, ref, watch } from 'vue'
import { useI18n } from 'vue-i18n'
import { useForkSettings } from '../../composables/useForkSettings'
import ForkNumber from './ForkNumber.vue'

const props = defineProps<{ active: boolean }>()
const { t } = useI18n({ useScope: 'global' })
const { state, plugins, saving, viewingActive, hasChanges, displayed, loading, error, load, save, reset } = useForkSettings()
const busy = computed(() => loading.value || saving.value)
const disabled = computed(() => busy.value || viewingActive.value)
const view = computed({
  get: () => viewingActive.value ? 'active' : 'saved',
  set: (value: string) => { viewingActive.value = value === 'active' },
})
const selectedPlugin = ref<string>()
const pluginOptions = computed(() => plugins.value.filter(p => !displayed.value?.plugins.http_filters[p.id]).map(p => ({ label: p.name, value: p.id })))
const methods = [
  'GET',
  'POST',
  'PUT',
  'PATCH',
  'DELETE',
  'HEAD',
  'OPTIONS',
].map(value => ({ label: value, value }))
const codes = [1000, 1001, 1011, 1012, 1013]
const compatibilityFields = computed(() => [
  {
    key: 'lite_context',
    label: t('forkSettings.liteContext'),
  },
  {
    key: 'store_false',
    label: t('forkSettings.storeFalse'),
  },
  {
    key: 'json_object',
    label: t('forkSettings.jsonObject'),
  },
  {
    key: 'min_output_tokens_16',
    label: t('forkSettings.minOutputTokens'),
  },
  {
    key: 'tool_structure',
    label: t('forkSettings.toolStructure'),
  },
] as const)
const rpcFields = computed(() => [
  {
    key: 'maximum_calls',
    label: t('forkSettings.totalCalls'),
    min: 1,
    max: 1024,
  },
  {
    key: 'maximum_executing_calls',
    label: t('forkSettings.executingCalls'),
    min: 1,
    max: 1024,
  },
  {
    key: 'maximum_queued_calls',
    label: t('forkSettings.queuedCalls'),
    min: 0,
    max: 1024,
  },
  {
    key: 'maximum_callbacks',
    label: t('forkSettings.callbacks'),
    min: 1,
    max: 4096,
  },
  {
    key: 'queue_timeout_ms',
    label: t('forkSettings.queueTimeout'),
    min: 1,
    max: 60000,
  },
] as const)
function setCode(code: number, enabled: boolean) {
  if (!displayed.value || viewingActive.value)
    return
  const current = displayed.value.openai.ws_response_recovery
  current.close_codes = enabled
    ? [
        ...new Set([...current.close_codes, code]),
      ].sort()
    : current.close_codes.filter(c => c !== code)
}
function addFilter() {
  if (!selectedPlugin.value || !displayed.value || viewingActive.value)
    return
  displayed.value.plugins.http_filters[selectedPlugin.value] = [
    { method: 'POST', path: '/v1/responses' },
    { method: 'POST', path: '/responses' },
  ]
  selectedPlugin.value = undefined
}
function removeRoute(id: string, index: number) {
  if (!displayed.value || viewingActive.value)
    return
  const routes = displayed.value.plugins.http_filters[id]
  if (!routes)
    return
  routes.splice(index, 1)
  if (!routes.length)
    delete displayed.value.plugins.http_filters[id]
}
watch(() => props.active, (active) => {
  if (active && !state.value && !busy.value)
    void load()
}, { immediate: true })
defineExpose({ hasChanges })
</script>

<template>
  <div class="min-w-0">
    <div class="flex flex-wrap items-center justify-between gap-3">
      <div class="flex flex-wrap items-center gap-3">
        <ZSegmented v-model="view" :options="[{ label: t('forkSettings.editSaved'), value: 'saved' }, { label: t('forkSettings.currentActive'), value: 'active' }]" :aria-label="t('forkSettings.configView')" />
        <ZTag v-if="state" :type="state.restartRequired ? 'warning' : 'success'">
          {{ state.restartRequired ? t('forkSettings.restartPending') : t('forkSettings.inEffect') }}
        </ZTag>
      </div>
      <div class="flex items-center gap-2">
        <ZIconButton :aria-label="t('forkSettings.refresh')" :title="t('forkSettings.refresh')" :loading="loading" :disabled="busy || hasChanges" @click="load">
          <RefreshCw class="size-4" />
        </ZIconButton>
        <ZIconButton :aria-label="t('forkSettings.undo')" :title="t('forkSettings.undo')" :disabled="busy || !hasChanges" @click="reset">
          <Undo2 class="size-4" />
        </ZIconButton>
        <ZButton type="primary" :loading="saving" :disabled="busy || !hasChanges || viewingActive" @click="save">
          <template #icon>
            <Save class="size-4" />
          </template>{{ t('forkSettings.save') }}
        </ZButton>
      </div>
    </div>
    <p class="mt-3 text-z-sm text-z-text-secondary">
      {{ t('forkSettings.restartNotice') }}
    </p>
    <p v-if="error" role="alert" class="mt-4 text-z-error">
      {{ error }}
    </p>
    <p v-if="loading && !state" role="status" class="py-12 text-center text-z-text-secondary">
      {{ t('forkSettings.loading') }}
    </p>
    <ZButton v-else-if="!state" class="mt-4" @click="load">
      {{ t('forkSettings.retry') }}
    </ZButton>
    <fieldset v-if="displayed" :disabled="disabled" :aria-label="t('forkSettings.title')" class="m-0 mt-5 grid min-w-0 gap-7 border-0 p-0">
      <section>
        <h2 class="text-z-base font-semibold">
          {{ t('forkSettings.requestCompatibility') }}
          <ZTag type="success" class="ml-2">
            {{ t('forkSettings.hotApply') }}
          </ZTag>
        </h2>
        <div class="mt-4 grid max-w-6xl gap-4.5 sm:grid-cols-2 xl:grid-cols-3">
          <ZFormItem v-for="field in compatibilityFields" :key="field.key" :label="field.label">
            <ZSwitch v-model="displayed.openai.request_compatibility[field.key]" :disabled="disabled" :label="t('forkSettings.enable')" />
          </ZFormItem>
        </div>
        <p class="mt-3 text-z-sm text-z-warning">
          {{ t('forkSettings.compatibilityRisk') }}
        </p>
      </section>
      <section>
        <h2 class="text-z-base font-semibold">
          {{ t('forkSettings.cooldown') }}
          <ZTag type="success" class="ml-2">
            {{ t('forkSettings.hotApply') }}
          </ZTag>
        </h2>
        <div class="mt-4 grid max-w-6xl gap-4.5 sm:grid-cols-2 xl:grid-cols-3">
          <ForkNumber v-model="displayed.openai.model_unsupported_cooldown_seconds" :disabled="disabled" :label="t('forkSettings.cooldownSeconds')" :min="0" :max="86400" :unit="t('forkSettings.seconds')" />
        </div>
        <p class="mt-3 text-z-sm text-z-text-secondary">
          {{ t('forkSettings.zeroCooldown') }}
        </p>
      </section>
      <section>
        <h2 class="text-z-base font-semibold">
          {{ t('forkSettings.opening') }}
          <ZTag type="success" class="ml-2">
            {{ t('forkSettings.hotApply') }}
          </ZTag>
        </h2>
        <div class="mt-4 grid max-w-6xl gap-4.5 sm:grid-cols-2 xl:grid-cols-3">
          <ZFormItem :label="t('forkSettings.openingEnabled')">
            <ZSwitch v-model="displayed.openai.ws_opening_recovery.enabled" :disabled="disabled" :label="t('forkSettings.enable')" />
          </ZFormItem>
          <ForkNumber v-model="displayed.openai.ws_opening_recovery.max_retries" :disabled="disabled" :label="t('forkSettings.extraRetries')" :min="0" :max="5" :unit="t('forkSettings.times')" />
          <ForkNumber v-model="displayed.openai.ws_opening_recovery.retry_delay_ms" :disabled="disabled" :label="t('forkSettings.retryDelay')" :min="50" :max="5000" :unit="t('forkSettings.milliseconds')" />
          <ZFormItem :label="t('forkSettings.httpFallback')">
            <ZSwitch v-model="displayed.openai.ws_opening_recovery.http_fallback" :disabled="disabled" :label="t('forkSettings.enable')" />
          </ZFormItem>
        </div>
      </section>
      <section>
        <h2 class="text-z-base font-semibold">
          {{ t('forkSettings.responseRecovery') }}
          <ZTag type="success" class="ml-2">
            {{ t('forkSettings.hotApply') }}
          </ZTag>
        </h2>
        <div class="mt-4 grid max-w-6xl gap-4.5 sm:grid-cols-2 xl:grid-cols-3">
          <ZFormItem :label="t('forkSettings.responseEnabled')">
            <ZSwitch v-model="displayed.openai.ws_response_recovery.enabled" :disabled="disabled" :label="t('forkSettings.enable')" />
          </ZFormItem>
          <ForkNumber v-model="displayed.openai.ws_response_recovery.max_retries" :disabled="disabled" :label="t('forkSettings.extraRetries')" :min="0" :max="5" :unit="t('forkSettings.times')" />
          <ForkNumber v-model="displayed.openai.ws_response_recovery.window_ms" :disabled="disabled" :label="t('forkSettings.window')" :min="1" :max="10000" :unit="t('forkSettings.milliseconds')" />
          <ForkNumber v-model="displayed.openai.ws_response_recovery.max_prefix_bytes" :disabled="disabled" :label="t('forkSettings.prefixBytes')" :min="1024" :max="262144" unit="B" />
          <ZFormItem :label="t('forkSettings.disconnects')">
            <ZSwitch v-model="displayed.openai.ws_response_recovery.disconnects" :disabled="disabled" :label="t('forkSettings.enable')" />
          </ZFormItem>
          <ZFormItem :label="t('forkSettings.fullHistory')">
            <ZSwitch v-model="displayed.openai.ws_response_recovery.full_history" :disabled="disabled" :label="t('forkSettings.enable')" />
          </ZFormItem>
          <ZFormItem :label="t('forkSettings.encryptedHistory')">
            <ZSwitch v-model="displayed.openai.ws_response_recovery.encrypted_history" :disabled="disabled" :label="t('forkSettings.enable')" />
          </ZFormItem>
          <ForkNumber v-model="displayed.openai.ws_response_recovery.encrypted_history_max_sessions" :disabled="disabled" :label="t('forkSettings.encryptedSessions')" :min="1" :max="65536" />
          <ForkNumber v-model="displayed.openai.ws_response_recovery.encrypted_history_max_digests_per_session" :disabled="disabled" :label="t('forkSettings.encryptedDigests')" :min="1" :max="512" />
        </div>
        <ZFormItem class="mt-4" :label="t('forkSettings.closeCodes')">
          <div class="flex flex-wrap gap-4">
            <ZCheckbox v-for="code in codes" :key="code" :disabled="disabled" :model-value="displayed.openai.ws_response_recovery.close_codes.includes(code)" :label="String(code)" @update:model-value="setCode(code, $event)" />
          </div>
        </ZFormItem>
        <p class="mt-3 text-z-sm text-z-warning">
          {{ t('forkSettings.replayRisk') }}
          {{ t('forkSettings.encryptedHistoryRisk') }}
          {{ t('forkSettings.cacheRestart') }}
        </p>
      </section>
      <section>
        <h2 class="text-z-base font-semibold">
          {{ t('forkSettings.capacity') }}
          <ZTag type="warning" class="ml-2">
            {{ t('forkSettings.restartApply') }}
          </ZTag>
        </h2>
        <div class="mt-4 grid max-w-6xl gap-4.5 sm:grid-cols-2 xl:grid-cols-3">
          <ForkNumber v-for="field in rpcFields" :key="field.key" v-model="displayed.plugins.rpc[field.key]" :disabled="disabled" :label="field.label" :min="field.min" :max="field.max" :unit="field.key === 'queue_timeout_ms' ? t('forkSettings.milliseconds') : undefined" />
        </div>
        <p class="mt-3 text-z-sm text-z-text-secondary">
          {{ t('forkSettings.sdkNotice') }}
        </p>
      </section>
      <section>
        <h2 class="text-z-base font-semibold">
          {{ t('forkSettings.filters') }}
          <ZTag type="warning" class="ml-2">
            {{ t('forkSettings.restartApply') }}
          </ZTag>
        </h2>
        <p class="mt-2 text-z-sm text-z-text-secondary">
          {{ t('forkSettings.filterNotice') }}
        </p>
        <div class="mt-4 flex flex-wrap items-end gap-3">
          <ZFormItem :label="t('forkSettings.plugin')" class="min-w-0 flex-1 sm:max-w-96">
            <ZSelect v-model="selectedPlugin" :disabled="disabled" :options="pluginOptions" :placeholder="t('forkSettings.selectPlugin')" :aria-label="t('forkSettings.plugin')" filterable />
          </ZFormItem>
          <ZButton :disabled="!selectedPlugin || viewingActive" @click="addFilter">
            <template #icon>
              <Plus class="size-4" />
            </template>{{ t('forkSettings.addFilter') }}
          </ZButton>
        </div>
        <p v-if="!Object.keys(displayed.plugins.http_filters).length" class="py-6 text-z-sm text-z-text-secondary">
          {{ t('forkSettings.noFilters') }}
        </p>
        <div v-for="(routes, id) in displayed.plugins.http_filters" :key="id" class="mt-5 min-w-0">
          <div class="flex flex-wrap items-center justify-between gap-2">
            <h3 class="break-all text-z-sm font-medium">
              {{ plugins.find(p => p.id === id)?.name ?? id }}
            </h3><ZIconButton :aria-label="t('forkSettings.addRoute')" :title="t('forkSettings.addRoute')" :disabled="viewingActive || routes.length >= 32" @click="routes.push({ method: 'POST', path: '/responses' })">
              <Plus class="size-4" />
            </ZIconButton>
          </div>
          <div v-for="(route, index) in routes" :key="index" class="mt-3 grid min-w-0 grid-cols-[7rem_minmax(0,1fr)_2rem] gap-2">
            <ZSelect v-model="route.method" :disabled="disabled" :options="methods" :aria-label="t('forkSettings.method')" />
            <ZInput v-model="route.path" :disabled="disabled" :aria-label="t('forkSettings.path')" placeholder="/v1/responses" />
            <ZIconButton :aria-label="t('forkSettings.removeRoute')" :title="t('forkSettings.removeRoute')" :disabled="viewingActive" @click="removeRoute(id, index)">
              <Trash2 class="size-4" />
            </ZIconButton>
          </div>
        </div>
      </section>
    </fieldset>
  </div>
</template>
