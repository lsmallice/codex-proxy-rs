<script setup lang="ts">
import { ZFormItem, ZInputNumber } from '@codex-proxy/ui'

defineProps<{ label: string, min: number, max: number, unit?: string, disabled?: boolean }>()
const model = defineModel<number>({ required: true })
</script>

<template>
  <ZFormItem :label="label">
    <ZInputNumber v-model="model" :aria-label="label" :min="min" :max="max" :step="1" :unit="unit" :disabled="disabled" class="w-full" />
  </ZFormItem>
</template>
