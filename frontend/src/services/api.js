import axios from 'axios'

const BASE_URL = import.meta.env.VITE_API_URL || 'http://localhost:8000'

const api = axios.create({
  baseURL: BASE_URL,
  timeout: 60_000,
})

// Unwrap { status, data } envelope
const unwrap = (res) => res.data.data

// Global error normalisation
api.interceptors.response.use(
  (res) => res,
  (err) => {
    const detail =
      err.response?.data?.detail ||
      err.response?.data?.message ||
      err.message ||
      'Request failed'
    return Promise.reject(new Error(detail))
  }
)

// ── Datasets ──────────────────────────────────────────────────────────────────

export const datasetsAPI = {
  upload(file, onUploadProgress) {
    const form = new FormData()
    form.append('file', file)
    return api.post('/upload', form, { onUploadProgress }).then(unwrap)
  },

  uploadNewVersion(id, file, onUploadProgress) {
    const form = new FormData()
    form.append('file', file)
    return api.post(`/dataset/${id}/upload-version`, form, { onUploadProgress }).then(unwrap)
  },

  getAll() {
    return api.get('/datasets').then(unwrap)
  },

  getById(id) {
    return api.get(`/dataset/${id}`).then(unwrap)
  },

  getProfile(id, version = null) {
    const params = version != null ? { version } : {}
    return api.get(`/dataset/${id}/profile`, { params }).then(unwrap)
  },

  getQuality(id, version = null, method = 'all') {
    const params = { method, ...(version != null ? { version } : {}) }
    return api.get(`/dataset/${id}/quality`, { params }).then(unwrap)
  },

  getAnomalies(id, version = null, method = 'all') {
    const params = { method, ...(version != null ? { version } : {}) }
    return api.get(`/dataset/${id}/anomalies`, { params }).then(unwrap)
  },

  getRepairs(id, version = null) {
    const params = version != null ? { version } : {}
    return api.get(`/dataset/${id}/repairs`, { params }).then(unwrap)
  },

  clean(id, request) {
    return api.post(`/dataset/${id}/clean`, request).then(unwrap)
  },

  getDrift(id, versionA, versionB) {
    return api
      .get(`/dataset/${id}/drift`, { params: { version_a: versionA, version_b: versionB } })
      .then(unwrap)
  },

  getChangeSummary(id, versionA, versionB) {
    return api
      .get(`/dataset/${id}/change-summary`, { params: { version_a: versionA, version_b: versionB } })
      .then(unwrap)
  },

  getSecurityScan(id, version = null) {
    const params = version != null ? { version } : {}
    return api.get(`/dataset/${id}/security-scan`, { params }).then(unwrap)
  },

  getDelimiterCheck(id, version = null) {
    const params = version != null ? { version } : {}
    return api.get(`/dataset/${id}/delimiter-check`, { params }).then(unwrap)
  },

  getPii(id, version = null) {
    const params = version != null ? { version } : {}
    return api.get(`/dataset/${id}/pii`, { params }).then(unwrap)
  },

  getPrediction(id, slaThreshold = 70) {
    return api.get(`/dataset/${id}/prediction`, { params: { sla_threshold: slaThreshold } }).then(unwrap)
  },

  getReport(id, version = null, method = 'all') {
    const params = { method, ...(version != null ? { version } : {}) }
    return api.get(`/dataset/${id}/report`, { params }).then(unwrap)
  },

  deleteDataset(id) {
    return api.delete(`/dataset/${id}`).then(unwrap)
  },

  getVersions(id) {
    return api.get(`/dataset/${id}/versions`).then(unwrap)
  },

  getVersion(id, versionNum) {
    return api.get(`/dataset/${id}/versions/${versionNum}`).then(unwrap)
  },

  restoreVersion(id, versionNum) {
    return api.post(`/dataset/${id}/versions/${versionNum}/restore`).then(unwrap)
  },

  diffVersions(id, versionA, versionB) {
    return api
      .get(`/dataset/${id}/versions/diff`, { params: { version_a: versionA, version_b: versionB } })
      .then(unwrap)
  },

  deleteVersion(id, versionNum) {
    return api.delete(`/datasets/${id}/versions/${versionNum}`).then(unwrap)
  },
}

// ── Alerts ────────────────────────────────────────────────────────────────────

export const alertsAPI = {
  getAll(datasetId = null) {
    const params = datasetId ? { dataset_id: datasetId } : {}
    return api.get('/alerts', { params }).then(unwrap)
  },

  markRead(alertId) {
    return api.patch(`/alerts/${alertId}/read`).then(unwrap)
  },
}

export default api
