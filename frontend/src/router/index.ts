import { createRouter, createWebHistory } from 'vue-router'

import Overview from '@/views/Overview.vue'
import WorkbenchView from '@/views/WorkbenchView.vue'
import IngestBatchDetailView from '@/views/IngestBatchDetailView.vue'

const router = createRouter({
  history: createWebHistory(),
  routes: [
    { path: '/', name: 'overview', component: Overview },
    {
      path: '/ingest_batch/detail',
      name: 'ingest-batch-detail-query',
      component: IngestBatchDetailView,
      props: true,
    },
    {
      path: '/ingest_batch/detail/:batchId',
      name: 'ingest-batch-detail',
      component: IngestBatchDetailView,
      props: true,
    },
    {
      path: '/ingest_batch/:batchId',
      name: 'ingest-batch-direct-detail',
      component: IngestBatchDetailView,
      props: true,
    },
    {
      path: '/ingest-batch/:batchId',
      name: 'ingest-singular-batch-direct-detail',
      component: IngestBatchDetailView,
      props: true,
    },
    {
      path: '/ingest-batches/:batchId',
      name: 'ingest-batch-detail-alias',
      component: IngestBatchDetailView,
      props: true,
    },
    { path: '/:moduleKey', name: 'module', component: WorkbenchView, props: true },
  ],
})

export default router
