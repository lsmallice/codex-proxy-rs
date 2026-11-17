<script setup lang="ts">
import type { Account, AccountHandoffConfig, AccountTransferSnapshot } from '@/api'
import { ZButton, ZConfirmDialog, ZFormItem, ZIconButton, ZSelect } from '@codex-proxy/ui'
import { Plus, RefreshCw, X } from '@lucide/vue'
import { computed, onMounted, onScopeDispose, ref } from 'vue'
import { useI18n } from 'vue-i18n'
import { cancelAccountTransferGroup, getAccountTransferGroups, saveAccountTransferGroup } from '@/api'
import { useAsyncAction } from '@/composables/useAsyncAction'

const props = defineProps<{ accounts: Account[], config: AccountHandoffConfig, occupied: string[] }>()
const { t } = useI18n({ useScope: 'global' })
const groups = ref<AccountTransferSnapshot[]>([])
const oldIds = ref<string[]>([])
const newIds = ref<string[]>([])
const selected = ref<string | undefined>()
const cancelling = ref<AccountTransferSnapshot | null>(null)
const confirm = ref(false)
const { loading: busy, run } = useAsyncAction()
const error = ref('')
let alive = true
let timer: ReturnType<typeof setTimeout> | undefined
async function refresh() {
  clearTimeout(timer)
  try {
    const data = await getAccountTransferGroups({ silent: true })
    if (alive) {
      groups.value = data
      error.value = ''
    }
  }
  catch (e) {
    if (alive) {
      error.value = e instanceof Error
        ? e.message
        : String(e)
    }
  }
  finally {
    if (alive)
      timer = setTimeout(() => void refresh(), 5000)
  }
}
const editing = computed(() => groups.value.find(g => g.state.id === selected.value))
const options = computed(() => props.accounts.map(a => ({ value: a.id, label: a.name || a.id, disabled: props.occupied.includes(a.id) || groups.value.some(g => g.state.id !== selected.value && [
  ...g.state.old,
  ...g.state.new,
].some(m => m.id === a.id)) })))
const selectedAccounts = computed(() => props.accounts.filter(a => [
  ...oldIds.value,
  ...newIds.value,
].includes(a.id)))
const groupId = computed(() => editing.value?.state.groupId ?? selectedAccounts.value[0]?.groups.find(g => g.enabled && selectedAccounts.value.every(a => a.groups.some(x => x.enabled && x.id === g.id)))?.id)
async function save() {
  if (!groupId.value)
    return
  await run(async () => {
    const prior = selected.value
      ? (await getAccountTransferGroups()).find(g => g.state.id === selected.value)
      : undefined
    if (selected.value && !prior) {
      clearTimeout(timer)
      await refresh()
      return
    }
    await saveAccountTransferGroup({
      id: prior?.state.id ?? crypto.randomUUID(),
      revision: prior?.state.revision ?? 0,
      groupId: groupId.value!,
      oldAccountIds: [
        ...new Set([
          ...(prior?.state.old.map(m => m.id) ?? []),
          ...oldIds.value,
        ]),
      ],
      newAccountIds: [
        ...new Set([
          ...(prior?.state.new.map(m => m.id) ?? []),
          ...newIds.value,
        ]),
      ],
      config: prior?.state.config ?? props.config,
    })
    oldIds.value = []
    newIds.value = []
    selected.value = undefined
    clearTimeout(timer)
    await refresh()
  })
}
async function cancel() {
  if (!cancelling.value)
    return
  await run(async () => {
    await cancelAccountTransferGroup(cancelling.value!.state.id, cancelling.value!.state.revision)
    confirm.value = false
    clearTimeout(timer)
    await refresh()
  })
}
onMounted(() => void refresh())
onScopeDispose(() => {
  alive = false
  clearTimeout(timer)
})
</script>

<template>
  <section class="flex min-w-0 flex-col gap-3 border-t border-z-border pt-3">
    <p v-if="error" role="alert" class="text-z-sm text-z-error">
      {{ error }}
    </p>
    <div class="flex items-center justify-between">
      <h3 class="text-z-sm">
        {{ t('accounts.handoff.groupTitle') }}
      </h3><ZIconButton :aria-label="t('accounts.handoff.refresh')" :disabled="busy" @click="refresh()">
        <RefreshCw class="size-4" />
      </ZIconButton>
    </div>
    <ZFormItem :label="t('accounts.handoff.appendGroup')">
      <ZSelect v-model="selected" :options="groups.map(g => ({ value: g.state.id, label: `${g.state.old.length} → ${g.state.new.length} · ${g.state.id.slice(0, 8)}` }))" clearable :placeholder="t('accounts.handoff.newGroup')" />
    </ZFormItem>
    <div class="grid gap-3 sm:grid-cols-2">
      <ZFormItem :label="t('accounts.handoff.old')">
        <ZSelect v-model="oldIds" :options="options.filter(o => !newIds.includes(o.value))" multiple filterable :disabled="busy" />
      </ZFormItem>
      <ZFormItem :label="t('accounts.handoff.new')">
        <ZSelect v-model="newIds" :options="options.filter(o => !oldIds.includes(o.value))" multiple filterable :disabled="busy" />
      </ZFormItem>
    </div>
    <ZButton :disabled="busy || !config.newAccountProtection || !groupId || (!editing && (!oldIds.length || !newIds.length))" @click="save">
      <Plus class="size-4" />{{ t('accounts.handoff.saveGroup') }}
    </ZButton>
    <div v-for="g in groups" :key="g.state.id" class="border-b border-z-border py-2 text-z-sm">
      <div class="flex justify-between gap-2">
        <span>{{ t('accounts.handoff.sharedBudget', { value: g.state.budget }) }}</span><ZIconButton :aria-label="t('accounts.handoff.cancel')" @click="cancelling = g; confirm = true">
          <X class="size-4" />
        </ZIconButton>
      </div>
      <div v-for="m in g.state.old" :key="m.id" class="break-all">
        {{ t('accounts.handoff.old') }} · {{ m.name }} · {{ m.inFlight }} / {{ m.limit }}
      </div>
      <div v-for="m in g.state.new" :key="m.id" class="break-all">
        {{ t('accounts.handoff.new') }} · {{ m.name }} · {{ m.inFlight }} / {{ m.limit }}
      </div>
      <p class="m-0 text-z-xs text-z-text-secondary">
        {{ g.state.message }}
      </p>
    </div>
    <ZConfirmDialog v-model="confirm" :title="t('accounts.handoff.cancel')" :loading="busy" @confirm="cancel">
      {{ t('accounts.handoff.cancelGroupWarning') }}
    </ZConfirmDialog>
  </section>
</template>
