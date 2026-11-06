import type { SystemReleaseSource, SystemSourcePreview, SystemUpdateChannel } from '@/api'
import { ref, shallowRef, watch } from 'vue'
import { getSystemSourcePreview } from '@/api'
import { useRequestState } from '@/composables/useRequestState'

export function useSourcePreview() {
  const source = ref<SystemReleaseSource>('official')
  const channel = ref<Exclude<SystemUpdateChannel, 'exp'>>('stable')
  const data = shallowRef<SystemSourcePreview | null>(null)
  const query = useRequestState()
  async function load() {
    const id = query.start()
    // 同条件刷新保留布局，换来源或通道时不能沿用旧结果
    if (data.value?.source !== source.value || data.value?.channel !== channel.value)
      data.value = null
    try {
      const result = await getSystemSourcePreview(source.value, channel.value, { signal: query.signal, silent: true })
      if (query.isCurrent(id))
        data.value = result
    }
    catch (error) {
      if (query.isCurrent(id))
        data.value = null
      query.fail(id, error)
    }
    finally { query.finish(id) }
  }
  watch([source, channel], load, { immediate: true })
  return {
    source,
    channel,
    data,
    loading: query.loading,
    error: query.error,
    load,
  }
}
