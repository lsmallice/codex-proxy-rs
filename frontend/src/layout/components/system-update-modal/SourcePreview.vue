<script setup lang="ts">
import { ZButton, ZFormItem, ZMarkdown, ZScrollbar, ZSegmented, ZTag } from '@codex-proxy/ui'
import { ExternalLink, RefreshCw } from '@lucide/vue'
import { computed } from 'vue'
import { useI18n } from 'vue-i18n'
import { useSourcePreview } from './useSourcePreview'

const { t } = useI18n({ useScope: 'global' })
const { source, channel, data, loading, error, load } = useSourcePreview()
const sources = computed(() => [
  {
    label: t('systemUpdate.sources.fork'),
    value: 'fork',
  },
  {
    label: t('systemUpdate.sources.official'),
    value: 'official',
  },
])
const channels = ['stable', 'rc', 'beta', 'alpha'].map(value => ({ label: {
  stable: 'Stable',
  rc: 'RC',
  beta: 'Beta',
  alpha: 'Alpha',
}[value]!, value }))
const labels = computed<Record<string, string>>(() => ({
  source: t('systemUpdate.sources.checkSource'),
  build: t('systemUpdate.sources.checkBuild'),
  version: t('systemUpdate.sources.checkVersion'),
  assets: t('systemUpdate.sources.checkAssets'),
  database: t('systemUpdate.sources.checkDatabase'),
  plugins: t('systemUpdate.sources.checkPlugins'),
  deployment: t('systemUpdate.sources.checkDeployment'),
  switch: t('systemUpdate.sources.checkSwitch'),
}))
const statuses = computed(() => ({
  passed: t('systemUpdate.sources.passed'),
  blocked: t('systemUpdate.sources.blocked'),
  pending: t('systemUpdate.sources.pending'),
}))
</script>

<template>
  <div class="grid min-w-0 gap-4" :aria-busy="loading">
    <div class="flex flex-wrap items-end justify-between gap-3">
      <ZFormItem :label="t('systemUpdate.sources.label')">
        <ZSegmented v-model="source" :options="sources" :aria-label="t('systemUpdate.sources.label')" />
      </ZFormItem>
      <ZFormItem :label="t('systemUpdate.sources.stage')">
        <ZSegmented v-model="channel" :options="channels" :aria-label="t('systemUpdate.sources.stage')" />
      </ZFormItem>
      <ZButton :loading="loading" @click="load">
        <template #icon>
          <RefreshCw class="size-4" />
        </template>
        {{ t('systemUpdate.sources.inspect') }}
      </ZButton>
    </div>
    <p class="m-0 text-z-sm text-z-text-secondary">
      {{ t('systemUpdate.sources.readOnly') }}
    </p>
    <p v-if="source === 'official'" class="m-0 text-z-sm text-z-warning">
      {{ t('systemUpdate.sources.officialWarning') }}
    </p>
    <p v-if="error" role="alert" class="m-0 text-z-sm text-z-error">
      {{ error }}
    </p>
    <p v-if="loading && !data" role="status" class="m-0 py-5 text-center text-z-sm text-z-text-secondary">
      {{ t('systemUpdate.sources.loading') }}
    </p>
    <template v-if="data">
      <dl class="m-0 grid min-w-0 gap-x-5 gap-y-3 sm:grid-cols-2">
        <div class="min-w-0">
          <dt class="text-z-xs text-z-text-secondary">
            {{ t('systemUpdate.sources.configured') }}
          </dt><dd class="m-0 mt-1 break-all font-mono text-z-sm">
            {{ data.configuredRepository ?? '-' }}
          </dd>
        </div>
        <div class="min-w-0">
          <dt class="text-z-xs text-z-text-secondary">
            {{ t('systemUpdate.sources.target') }}
          </dt><dd class="m-0 mt-1 break-all font-mono text-z-sm">
            {{ data.repository }}
          </dd>
        </div>
        <div class="min-w-0">
          <dt class="text-z-xs text-z-text-secondary">
            {{ t('systemUpdate.currentVersion') }}
          </dt><dd class="m-0 mt-1 break-all font-mono text-z-sm">
            v{{ data.currentVersion }}
          </dd>
        </div>
        <div class="min-w-0">
          <dt class="text-z-xs text-z-text-secondary">
            {{ t('systemUpdate.latestVersion') }}
          </dt><dd class="m-0 mt-1 flex flex-wrap items-center gap-2 font-mono text-z-sm">
            <span>{{ data.latestVersion ? `v${data.latestVersion}` : t('systemUpdate.sources.noRelease') }}</span><a v-if="data.releaseUrl" :href="data.releaseUrl" target="_blank" rel="noopener noreferrer" :aria-label="t('systemUpdate.releasePage')" :title="t('systemUpdate.releasePage')" class="text-z-link"><ExternalLink class="size-4" /></a>
          </dd>
        </div>
      </dl>
      <ul class="m-0 grid list-none gap-4 p-0" :aria-label="t('systemUpdate.sources.results')">
        <li v-for="check in data.checks" :key="check.id" class="flex min-w-0 items-start gap-3">
          <ZTag class="shrink-0" :type="check.status === 'passed' ? 'success' : check.status === 'blocked' ? 'danger' : 'warning'">
            {{ statuses[check.status] }}
          </ZTag>
          <div class="grid min-w-0 gap-1">
            <strong class="text-z-sm font-medium">{{ labels[check.id] ?? check.id }}</strong><span class="break-words text-z-sm text-z-text-secondary">{{ check.message }}</span>
          </div>
        </li>
      </ul>
      <details v-if="data.notes" class="text-z-sm">
        <summary class="cursor-pointer text-z-text-secondary">
          {{ t('systemUpdate.releaseNotes') }}
        </summary>
        <ZScrollbar max-height="160px" class="mt-3">
          <ZMarkdown :source="data.notes" />
        </ZScrollbar>
      </details>
    </template>
  </div>
</template>
