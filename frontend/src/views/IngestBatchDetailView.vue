<template>
  <section class="page ingest-detail-page" data-testid="ingest-detail">
    <header class="page-head">
      <div>
        <p class="breadcrumb"><RouterLink to="/ingest_batch">入湖批次</RouterLink> / 入湖详情</p>
        <h2>入湖批次详情</h2>
        <p class="page-desc">核对逻辑批次、每一次重传尝试、来源凭据、按条接受范围与实际确认的快照。</p>
      </div>
      <div class="page-actions">
        <button class="btn" data-testid="refresh-detail" type="button" @click="reload">刷新</button>
        <RouterLink class="btn ghost" to="/ingest_batch">返回列表</RouterLink>
      </div>
    </header>

    <p v-if="errorMessage" class="error-text" data-testid="detail-error">{{ errorMessage }}</p>
    <p v-if="loading" class="status-legend">正在载入入湖详情…</p>

    <template v-if="detail">
      <section class="detail-grid" data-testid="logical-summary">
        <article class="info-card">
          <span>逻辑批次</span>
          <strong data-testid="logical-batch-id">{{ detail.logical_batch_id || detail.batch_id || batchId }}</strong>
        </article>
        <article class="info-card">
          <span>当前状态</span>
          <strong data-testid="batch-status">{{ batchStatus }}</strong>
        </article>
        <article class="info-card">
          <span>来源系统 / 凭据</span>
          <strong data-testid="batch-source">{{ batchSource }}<em v-if="sourceCredential"> / {{ sourceCredential }}</em></strong>
        </article>
        <article class="info-card">
          <span>采集窗口</span>
          <strong>{{ batch?.window_start || '—' }} ~ {{ batch?.window_end || '—' }}</strong>
        </article>
        <article class="info-card">
          <span>规范摘要（字段顺序无关）</span>
          <strong class="mono" data-testid="canonical-checksum">{{ batch?.canonical_checksum || '—' }}</strong>
        </article>
        <article class="info-card">
          <span>原始摘要（旧算法）</span>
          <strong class="mono" data-testid="legacy-checksum">{{ batch?.legacy_checksum || batch?.checksum || '—' }}</strong>
        </article>
      </section>

      <section class="detail-panel" data-testid="snapshot-panel">
        <div class="panel-head">
          <h3>实际确认的快照</h3>
          <button
            v-if="detail.logical_batch_id && detail.confirmed_snapshot_id == null"
            class="btn primary"
            data-testid="confirm-batch"
            type="button"
            :disabled="confirming"
            @click="confirmBatch"
          >
            {{ confirming ? '确认中…' : '确认逻辑入湖' }}
          </button>
        </div>
        <div v-if="detail.confirmed_snapshot_id" data-testid="confirmed-snapshot">
          <p>快照编号：<strong>{{ detail.confirmed_snapshot_id }}</strong></p>
          <p>状态：{{ detail.confirmed_snapshot?.status || 'available' }}；新增行数：{{ detail.confirmed_snapshot?.added_rows ?? '—' }}</p>
        </div>
        <p v-else class="muted" data-testid="no-confirmed-snapshot">快照尚未确认；已接收的原始数据和尝试记录仍保留。</p>
        <p v-if="actionMessage" class="status-legend" data-testid="action-message">{{ actionMessage }}</p>
      </section>

      <section class="detail-panel" data-testid="attempts-panel">
        <div class="panel-head">
          <h3>每一次尝试（{{ attempts.length }}）</h3>
          <span class="muted">重传不会新增逻辑确认，只追加可审计尝试</span>
        </div>
        <table class="data-table attempts-table" data-testid="attempts-table">
          <thead>
            <tr>
              <th>次数</th>
              <th>尝试编号</th>
              <th>时间</th>
              <th>状态 / 回执</th>
              <th>原始摘要</th>
              <th>规范摘要</th>
              <th>快照</th>
              <th>原始输入与按条范围</th>
            </tr>
          </thead>
          <tbody>
            <tr v-for="attempt in attempts" :key="attempt.attempt_id" :data-testid="`attempt-row-${attempt.sequence}`">
              <td>{{ attempt.sequence }}</td>
              <td class="mono">{{ attempt.attempt_id }}</td>
              <td>{{ attempt.received_at || formatTime(attempt.created_at) }}</td>
              <td>
                <span>{{ attempt.status }}</span>
                <small v-if="attempt.simulated_lost_receipt" class="warn">回执丢失</small>
              </td>
              <td class="mono small">{{ attempt.legacy_checksum || attempt.checksum }}</td>
              <td class="mono small">{{ attempt.canonical_checksum }}</td>
              <td class="mono small">{{ attempt.snapshot_id || '—' }}</td>
              <td>
                <details>
                  <summary :data-testid="'payload-toggle-' + attempt.sequence">查看原始载荷 / 接手范围</summary>
                  <pre class="payload-pre" data-testid="attempt-payload">{{ JSON.stringify(attempt.raw_input || attempt.original_input || [], null, 2) }}</pre>
                  <ul v-if="attempt.item_results?.length" class="scope-list">
                    <li v-for="item in attempt.item_results" :key="`${attempt.attempt_id}-${item.index}`">
                      第 {{ item.index + 1 }} 条：{{ item.status }}；
                      本次包范围 {{ formatRange(item.accepted_range || item) }}；
                      原接手范围 {{ formatRange(item.owner_scope || item.handoff_range) }}；
                      来源 {{ item.source_system }}
                    </li>
                  </ul>
                </details>
              </td>
            </tr>
            <tr v-if="!attempts.length">
              <td colspan="8" class="empty-state">该资源条目还没有场景化接收尝试；示例登记数据仅用于列表展示。</td>
            </tr>
          </tbody>
        </table>
      </section>

      <section class="detail-panel" data-testid="lake-rows-panel">
        <h3>后续入湖读取（{{ lakeRows.length }} 份逻辑记录）</h3>
        <table class="data-table" data-testid="lake-rows-table">
          <thead>
            <tr><th>来源</th><th>来源凭据</th><th>行内容</th><th>接受范围</th><th>确认快照</th></tr>
          </thead>
          <tbody>
            <tr v-for="row in lakeRows" :key="row.row_uid" data-testid="lake-row">
              <td data-label="source">{{ row.source_system }}</td>
              <td>{{ row.source_credential || '—' }}</td>
              <td><pre class="payload-pre inline">{{ JSON.stringify(row.values) }}</pre></td>
              <td>{{ formatRange(row.accepted_scope) }}</td>
              <td class="mono small">{{ detail.confirmed_snapshot_id || row.snapshot_id || '—' }}</td>
            </tr>
            <tr v-if="!lakeRows.length"><td colspan="5" class="empty-state">快照确认后，这里只显示去重后的真实湖表记录。</td></tr>
          </tbody>
        </table>
      </section>

      <section class="detail-panel" data-testid="verification-panel">
        <h3>摘要核验与单独对账结果</h3>
        <div class="verify-form">
          <label>
            <span>旧摘要</span>
            <input v-model="checksumInput" data-testid="verify-checksum" class="mono" />
          </label>
          <label class="verify-payload">
            <span>待核对载荷（JSON 数组；可只改字段顺序）</span>
            <textarea v-model="payloadInput" data-testid="verify-payload" rows="5"></textarea>
          </label>
          <button class="btn primary" type="button" data-testid="run-verification" :disabled="verifying" @click="runVerification">
            {{ verifying ? '核验中…' : '核验摘要' }}
          </button>
        </div>
        <pre v-if="verificationResult" class="verification-result" data-testid="verification-result">{{ JSON.stringify(verificationResult, null, 2) }}</pre>
      </section>
    </template>
  </section>
</template>

<script setup lang="ts">
import { computed, onMounted, ref, watch } from 'vue'
import { useRoute } from 'vue-router'

import { request } from '@/api/client'

const route = useRoute()
const batchId = computed(() => String(
  route.params.batchId
  || route.query.batchId
  || route.query.id
  || '',
))
const detail = ref<Record<string, any> | null>(null)
const loading = ref(false)
const confirming = ref(false)
const verifying = ref(false)
const errorMessage = ref('')
const actionMessage = ref('')
const verificationResult = ref<Record<string, any> | null>(null)
const checksumInput = ref('')
const payloadInput = ref('')

const batch = computed<Record<string, any>>(() => detail.value?.batch || {})
const attempts = computed<Record<string, any>[]>(() => detail.value?.attempts || [])
const lakeRows = computed<Record<string, any>[]>(() => detail.value?.lake_rows || [])
const batchStatus = computed(() => batch.value.status || detail.value?.resource_entry?.status || '—')
const batchSource = computed(() => batch.value.source_system || detail.value?.resource_entry?.来源系统 || '—')
const sourceCredential = computed(() => attempts.value.find((a) => a.source_credential)?.source_credential || '')

function formatTime(value: unknown): string {
  if (typeof value !== 'number') return '—'
  return new Date(value * 1000).toLocaleString()
}

function formatRange(range: any): string {
  if (!range) return '—'
  if (typeof range.label === 'string') return range.label
  if (Number.isInteger(range.start) && Number.isInteger(range.end)) return `[${range.start}:${range.end}]`
  if (Array.isArray(range.indexes)) return `#${range.indexes.map((i: number) => i + 1).join(',#')}`
  return JSON.stringify(range)
}

async function reload() {
  if (!batchId.value) return
  loading.value = true
  errorMessage.value = ''
  try {
    const response = await request(`/api/ingest_batch/detail/${encodeURIComponent(batchId.value)}`)
    if (!response.ok) throw new Error(`详情接口返回 ${response.status}`)
    detail.value = await response.json()
    checksumInput.value = batch.value.legacy_checksum || batch.value.checksum || ''
    payloadInput.value = JSON.stringify(detail.value?.rows || batch.value.raw_rows || [], null, 2)
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '入湖详情读取失败'
  } finally {
    loading.value = false
  }
}

async function confirmBatch() {
  confirming.value = true
  actionMessage.value = ''
  try {
    const response = await request(
      `/api/ingest_batch/${encodeURIComponent(String(detail.value?.logical_batch_id || batchId.value))}/confirm`,
      { method: 'POST', body: JSON.stringify({}) },
    )
    const result = await response.json()
    if (!response.ok || result.success === false) throw new Error(result.error || result.message || '确认失败')
    actionMessage.value = result.idempotent ? '该逻辑批次此前已确认，未重复生成快照。' : '逻辑入湖已确认。'
    await reload()
  } catch (error) {
    actionMessage.value = error instanceof Error ? error.message : '确认失败'
  } finally {
    confirming.value = false
  }
}

async function runVerification() {
  verifying.value = true
  errorMessage.value = ''
  try {
    let payload: unknown
    try {
      payload = JSON.parse(payloadInput.value)
    } catch {
      payload = payloadInput.value
    }
    const response = await request('/api/ingest_batch/verify', {
      method: 'POST',
      body: JSON.stringify({
        batch_id: detail.value?.logical_batch_id || batchId.value,
        checksum: checksumInput.value,
        payload,
      }),
    })
    verificationResult.value = await response.json()
  } catch (error) {
    errorMessage.value = error instanceof Error ? error.message : '核验请求失败'
  } finally {
    verifying.value = false
  }
}

watch(batchId, reload)
onMounted(reload)
</script>

<style scoped>
.breadcrumb { font-size: 12px; color: #64748b; margin: 0 0 6px; }
.breadcrumb a { color: #1f6feb; text-decoration: none; }
.detail-grid { display: grid; grid-template-columns: repeat(3, minmax(0, 1fr)); gap: 12px; margin: 12px 0; }
.info-card, .detail-panel { background: #fff; border: 1px solid #d8dee6; border-radius: 8px; padding: 14px; margin-bottom: 14px; }
.info-card span { display: block; color: #64748b; font-size: 12px; margin-bottom: 6px; }
.info-card strong { font-size: 14px; word-break: break-all; }
.info-card em { color: #475569; font-style: normal; }
.detail-panel h3 { margin: 0 0 12px; font-size: 16px; }
.panel-head { display: flex; justify-content: space-between; align-items: center; gap: 12px; margin-bottom: 10px; }
.panel-head h3 { margin: 0; }
.mono { font-family: ui-monospace, SFMono-Regular, Menlo, monospace; word-break: break-all; }
.small { font-size: 12px; max-width: 180px; }
.muted { color: #64748b; font-size: 13px; }
.warn { color: #b42318; margin-left: 6px; }
.payload-pre { max-width: 420px; max-height: 180px; overflow: auto; background: #0f172a; color: #e2e8f0; padding: 8px; border-radius: 6px; font-size: 12px; white-space: pre-wrap; word-break: break-all; margin: 6px 0; }
.payload-pre.inline { background: #f8fafc; color: #334155; max-height: 72px; max-width: 320px; margin: 0; }
.scope-list { margin: 6px 0 0; padding-left: 18px; font-size: 12px; color: #475569; }
.verify-form { display: grid; gap: 10px; }
.verify-form label span { display: block; font-size: 12px; color: #64748b; margin-bottom: 4px; }
.verify-form input, .verify-form textarea { width: 100%; border: 1px solid #d8dee6; border-radius: 6px; padding: 8px; font-size: 13px; }
.verify-payload textarea { font-family: ui-monospace, SFMono-Regular, Menlo, monospace; }
.verification-result { margin-top: 12px; background: #f8fafc; border: 1px solid #e2e8f0; border-radius: 6px; padding: 10px; font-size: 12px; overflow: auto; }
@media (max-width: 1100px) { .detail-grid { grid-template-columns: repeat(2, minmax(0, 1fr)); } }
</style>
