import type { ForkSettingsSnapshot, ForkSettingsValues } from '@/api'
import { isEqual } from 'es-toolkit'
import { computed, onScopeDispose, ref, shallowRef, toRaw } from 'vue'
import { getForkSettings, getPluginInstances, saveForkSettings } from '@/api'
import { useRequestState } from '@/composables/useRequestState'
import { t } from '@/i18n'
import { notification } from '@/utils/notification'

export function useForkSettings() {
  const state = shallowRef<ForkSettingsSnapshot | null>(null)
  const draft = ref<ForkSettingsValues | null>(null)
  const plugins = shallowRef<{ id: string, name: string }[]>([])
  const saving = shallowRef(false)
  const viewingActive = ref(false)
  const query = useRequestState()
  let disposed = false
  const hasChanges = computed(() => !!draft.value && !!state.value && !isEqual(draft.value, state.value.saved))
  const displayed = computed(() => viewingActive.value ? state.value?.active : draft.value)
  function accept(value: ForkSettingsSnapshot) {
    state.value = value
    draft.value = structuredClone(value.saved)
  }
  async function load() {
    if (query.loading.value || saving.value)
      return
    const id = query.start()
    try {
      const value = await getForkSettings({ signal: query.signal, silent: true })
      if (query.isCurrent(id))
        accept(value)
    }
    catch (error) { query.fail(id, error) }
    finally { query.finish(id) }
    if (!query.isCurrent(id) || query.error.value)
      return
    try {
      const items = await getPluginInstances({ signal: query.signal })
      if (query.isCurrent(id))
        plugins.value = items.map(p => ({ id: p.id, name: p.name }))
    }
    catch { /* 实例名称加载不阻塞配置编辑，错误由请求层提示 */ }
  }
  function reset() {
    if (state.value)
      draft.value = structuredClone(state.value.saved)
  }
  async function save() {
    if (!draft.value || !state.value || saving.value || !hasChanges.value)
      return
    saving.value = true
    try {
      const value = await saveForkSettings(state.value.revision, structuredClone(toRaw(draft.value)))
      if (disposed)
        return
      accept(value)
      notification.success({
        message: value.restartRequired
          ? t('forkSettings.savedPending')
          : t('forkSettings.savedActive'),
      })
    }
    catch {}
    finally {
      saving.value = false
    }
  }
  onScopeDispose(() => {
    disposed = true
  })
  return {
    state,
    draft,
    plugins,
    saving,
    viewingActive,
    hasChanges,
    displayed,
    loading: query.loading,
    error: query.error,
    load,
    save,
    reset,
  }
}
