<script setup lang="ts">
import type { Account } from '@/api'

import { ZButton } from '@codex-proxy/ui'
import { Grid2X2, Slash } from '@lucide/vue'
import { computed } from 'vue'
import { useI18n } from 'vue-i18n'

const props = defineProps<{
  capacity: Account['capacity']
}>()

defineEmits<{ showDetails: [] }>()

const { t } = useI18n({ useScope: 'global' })

const limit = computed(() => props.capacity.handoffLimit ?? props.capacity.totalSlots)
const handoff = computed(() => props.capacity.handoffLimit != null)
const full = computed(() => props.capacity.usedSlots !== null
  && limit.value !== null
  && props.capacity.usedSlots >= limit.value)
const label = computed(() => t('accounts.capacity.concurrentRequestsLimit', { value: props.capacity.usedSlots ?? t('common.values.unknown'), value2: limit.value ?? t('common.values.unlimited') }))
const handoffDescription = computed(() => handoff.value
  ? t('accounts.capacity.handoffDescription', { limit: limit.value, configured: props.capacity.totalSlots ?? t('common.values.unlimited') })
  : undefined)
</script>

<template>
  <ZButton
    variant="text" size="small" :type="full ? 'warning' : 'default'"
    :aria-label="t('accounts.capacity.viewConcurrencyDetails', { label })"
    :title="handoffDescription"
    class="relative top-0.75 -m-0.5 h-auto! px-0.5! py-0.5! whitespace-nowrap font-mono text-[9px]! font-normal! tabular-nums hover:bg-z-bg-text-hover!"
    @click.stop="$emit('showDetails')"
  >
    <span aria-hidden="true" class="inline-flex items-center gap-1">
      <Grid2X2 class="size-2.5 shrink-0" :stroke-width="1.8" />
      <!-- 数字按字面高度居中，分隔线使用图标以避免字形基线偏移 -->
      <span class="inline-flex items-center gap-0.5">
        <span class="[text-box:trim-both_cap_alphabetic]">{{ capacity.usedSlots ?? '—' }}</span>
        <Slash class="size-1.75 shrink-0 -rotate-20" />
        <span class="[text-box:trim-both_cap_alphabetic]">{{ limit ?? '∞' }}</span>
      </span>
      <span v-if="handoff">{{ t('accounts.capacity.handoff') }}</span>
    </span>
  </ZButton>
</template>
