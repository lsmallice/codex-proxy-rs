<script setup lang="ts">
import type { Account } from '@/api'

import { ZDialog } from '@codex-proxy/ui'
import { computed, toRef } from 'vue'
import { useI18n } from 'vue-i18n'
import { useAccountConcurrency } from '../composables/useAccountConcurrency'
import AccountConcurrencyTable from './AccountConcurrencyTable.vue'

const props = defineProps<{ account: Account | null }>()

const { t } = useI18n({ useScope: 'global' })

const open = defineModel<boolean>({ required: true })
const { data, entries, loading, error } = useAccountConcurrency(toRef(props, 'account'), open)
const description = computed(() => {
  const capacity = props.account?.capacity
  return capacity?.handoffLimit != null
    ? t('accounts.capacity.handoffDescription', { limit: capacity.handoffLimit, configured: capacity.totalSlots ?? t('common.values.unlimited') })
    : t('accounts.concurrency.description')
})
</script>

<template>
  <ZDialog v-model="open" :title="t('accounts.concurrency.title')" :description="description" width="72rem">
    <AccountConcurrencyTable
      :entries="entries"
      :loading="loading && !data" :aria-busy="loading"
      :empty-text="error ? t('accounts.concurrency.unavailable') : t('accounts.concurrency.empty')"
    />
  </ZDialog>
</template>
