import { useEffect, useRef, useState, useCallback } from 'react'
import { useParams, useSearchParams, Link, useNavigate } from 'react-router-dom'
import { ChevronRight, Upload, Trash2, FileBarChart2, RefreshCw, Info } from 'lucide-react'
import toast from 'react-hot-toast'
import { datasetsAPI } from '../../services/api'
import { PageSpinner } from '../../components/LoadingSpinner'
import ErrorAlert from '../../components/ErrorAlert'
import { FileTypeBadge } from '../../components/Badge'
import ConfirmDialog from '../../components/ConfirmDialog'
import OverviewTab    from './OverviewTab'
import QualityTab     from './QualityTab'
import ProfileTab     from './ProfileTab'
import AnomaliesTab   from './AnomaliesTab'
import CleaningTab    from './CleaningTab'
import DriftTab       from './DriftTab'
import DatasetAlertsTab from './AlertsTab'
import VersionHistoryTab from './VersionHistoryTab'
import PIITab from './PIITab'
import PredictionTab from './PredictionTab'
import ReportTab from './ReportTab'

const TABS = [
  { id: 'overview',   label: 'Overview',       description: 'Displays dataset structure, size, and version history.' },
  { id: 'report',     label: '📋 Report',       description: 'Provides an executive summary of data quality, risks, and recommendations.' },
  { id: 'quality',    label: 'Quality',         description: 'Shows overall data quality score based on completeness, validity, consistency, uniqueness, security, and garbage detection.' },
  { id: 'profile',    label: 'Profile',         description: 'Displays column-level statistics including missing values, unique counts, and sample data.' },
  { id: 'anomalies',  label: '⭐ Anomalies',    description: 'Identifies unusual or invalid values using rule-based and statistical methods.' },
  { id: 'cleaning',   label: '⭐ Cleaning',     description: 'Apply data cleaning operations (e.g., fixing missing values, standardizing formats) and create a new dataset version.' },
  { id: 'changes',    label: '🔄 Changes',      description: 'Compares dataset versions and highlights differences in structure and data.' },
  { id: 'pii',        label: '🔒 PII',          description: 'Detects sensitive information such as emails, phone numbers, and personal identifiers.' },
  { id: 'prediction', label: '📈 Prediction',   description: 'Forecasts future data quality trends and SLA breach risks.' },
  { id: 'alerts',     label: 'Alerts',          description: 'Shows triggered alerts for anomalies, quality drops, or schema changes.' },
  { id: 'versions',   label: 'Versions',        description: 'Manage dataset versions, including restore, delete, and download.' },
]

export default function DatasetDetail() {
  const { id } = useParams()
  const navigate = useNavigate()
  const [searchParams, setSearchParams] = useSearchParams()
  const activeTab = searchParams.get('tab') || 'overview'

  const [dataset,       setDataset]       = useState(null)
  const [loading,       setLoading]       = useState(true)
  const [error,         setError]         = useState(null)
  const [refreshKey,    setRefreshKey]    = useState(0)
  const [syncing,       setSyncing]       = useState(false)
  const [uploading,     setUploading]     = useState(false)
  const [showDelete,    setShowDelete]    = useState(false)
  const [deleting,      setDeleting]      = useState(false)
  const [showInfo,      setShowInfo]      = useState(true)
  const fileInputRef = useRef(null)

  const loadDataset = async () => {
    setLoading(true); setError(null)
    try   { setDataset(await datasetsAPI.getById(id)) }
    catch (e) { setError(e.message) }
    finally   { setLoading(false) }
  }

  useEffect(() => { loadDataset() }, [id, refreshKey])

  const triggerRefresh = useCallback(() => {
    setRefreshKey(k => k + 1)
  }, [])

  const handleSync = async () => {
    setSyncing(true)
    try {
      await loadDataset()
      setRefreshKey(k => k + 1)
      toast.success('All tabs synced!')
    } catch (err) {
      toast.error('Sync failed: ' + err.message)
    } finally {
      setSyncing(false)
    }
  }

  const setTab = (tab) => setSearchParams({ tab })

  const handleVersionUpload = async (e) => {
    const file = e.target.files?.[0]
    if (!file) return
    e.target.value = ''
    setUploading(true)
    try {
      await datasetsAPI.uploadNewVersion(id, file)
      toast.success('New version uploaded!')
      setRefreshKey(k => k + 1)
    } catch (err) {
      toast.error(err.message)
    } finally {
      setUploading(false)
    }
  }

  const handleDeleteConfirm = async () => {
    setDeleting(true)
    try {
      await datasetsAPI.deleteDataset(id)
      toast.success(`"${dataset.name}" deleted.`)
      navigate('/datasets')
    } catch (err) {
      toast.error(err.message)
      setDeleting(false)
      setShowDelete(false)
    }
  }

  if (loading) return <PageSpinner text="Loading dataset…" />
  if (error)   return (
    <div className="p-8">
      <ErrorAlert message={error} onRetry={loadDataset} />
    </div>
  )

  return (
    <div className="flex flex-col h-full animate-fade-in">

      {/* Delete confirmation */}
      <ConfirmDialog
        open={showDelete}
        danger
        title="Delete Dataset"
        message={`Permanently delete "${dataset.name}" and all ${dataset.versions?.length ?? 1} version(s)? This cannot be undone.`}
        confirmLabel="Delete"
        loading={deleting}
        onConfirm={handleDeleteConfirm}
        onCancel={() => setShowDelete(false)}
      />

      {/* ── Header ── */}
      <div className="bg-white border-b border-slate-200 px-8 py-5">
        {/* Breadcrumb */}
        <nav className="flex items-center gap-1.5 text-xs text-slate-400 mb-3">
          <Link to="/datasets" className="hover:text-slate-600">Datasets</Link>
          <ChevronRight size={12} />
          <span className="text-slate-600 font-medium">{dataset.name}</span>
        </nav>

        <div className="flex items-start justify-between gap-4">
          <div>
            <div className="flex items-center gap-3">
              <h1 className="text-xl font-bold text-slate-900">{dataset.name}</h1>
              <FileTypeBadge type={dataset.file_type} />
              <span className="text-xs font-medium bg-slate-100 text-slate-600 px-2 py-0.5 rounded-full">
                v{dataset.current_version}
              </span>
            </div>
            <p className="text-sm text-slate-400 mt-1">
              {dataset.row_count?.toLocaleString()} rows × {dataset.col_count} columns
              &nbsp;·&nbsp;{dataset.original_filename}
              &nbsp;·&nbsp;{dataset.versions?.length} version{dataset.versions?.length !== 1 ? 's' : ''}
            </p>
          </div>

          {/* Upload new version + Delete */}
          <div className="flex items-center gap-2">
            <input
              ref={fileInputRef}
              type="file"
              accept=".csv,.json"
              className="hidden"
              onChange={handleVersionUpload}
            />
            <button
              onClick={handleSync}
              disabled={syncing}
              className="btn-secondary"
              title="Refresh / Sync All Tabs"
            >
              <RefreshCw size={14} className={syncing ? 'animate-spin' : ''} />
              {syncing ? 'Syncing…' : 'Sync'}
            </button>
            <button
              onClick={() => setSearchParams({ tab: 'report' })}
              className="btn-secondary"
              title="Generate Report"
            >
              <FileBarChart2 size={14} /> Report
            </button>
            <button
              onClick={() => fileInputRef.current?.click()}
              disabled={uploading}
              className="btn-secondary"
            >
              {uploading ? (
                <span className="w-4 h-4 border-2 border-slate-300 border-t-slate-600 rounded-full animate-spin" />
              ) : (
                <Upload size={14} />
              )}
              {uploading ? 'Uploading…' : 'Upload New Version'}
            </button>
            <button
              onClick={() => setShowDelete(true)}
              className="p-2 rounded-lg text-slate-400 hover:text-red-600 hover:bg-red-50 transition-colors"
              title="Delete dataset"
            >
              <Trash2 size={16} />
            </button>
            <button
              onClick={() => setShowInfo(v => !v)}
              className={`p-2 rounded-lg transition-colors ${
                showInfo
                  ? 'text-blue-600 bg-blue-50 hover:bg-blue-100'
                  : 'text-slate-400 hover:text-slate-600 hover:bg-slate-50'
              }`}
              title={showInfo ? 'Hide feature descriptions' : 'Show feature descriptions'}
            >
              <Info size={16} />
            </button>
          </div>
        </div>

        {/* Tab bar */}
        <div className="flex gap-0.5 mt-5 -mb-5 overflow-x-auto">
          {TABS.map(t => (
            <button
              key={t.id}
              onClick={() => setTab(t.id)}
              className={`px-4 pb-3 pt-1 text-sm font-medium border-b-2 transition-colors whitespace-nowrap ${
                activeTab === t.id
                  ? 'border-blue-500 text-blue-600'
                  : 'border-transparent text-slate-500 hover:text-slate-700 hover:border-slate-300'
              }`}
            >
              {t.label}
            </button>
          ))}
        </div>
      </div>

      {/* ── Feature description banner ── */}
      {showInfo && (
        <div className="bg-blue-50 border-b border-blue-100 px-8 py-2.5 flex items-center gap-2 text-sm text-blue-700">
          <Info size={14} className="shrink-0 text-blue-500" />
          <span>{TABS.find(t => t.id === activeTab)?.description}</span>
        </div>
      )}

      {/* ── Tab content ── */}
      <div className="flex-1 overflow-y-auto p-8">
        {activeTab === 'overview'    && <OverviewTab  dataset={dataset} refreshKey={refreshKey} />}
        {activeTab === 'report'      && <ReportTab datasetId={id} refreshKey={refreshKey} />}
        {activeTab === 'quality'     && <QualityTab   datasetId={id} dataset={dataset} refreshKey={refreshKey} />}
        {activeTab === 'profile'     && <ProfileTab   datasetId={id} dataset={dataset} refreshKey={refreshKey} />}
        {activeTab === 'anomalies'   && <AnomaliesTab datasetId={id} dataset={dataset} refreshKey={refreshKey} />}
        {activeTab === 'cleaning'    && (
          <CleaningTab
            datasetId={id}
            dataset={dataset}
            onCleanSuccess={triggerRefresh}
          />
        )}
        {activeTab === 'changes'     && <DriftTab   datasetId={id} dataset={dataset} refreshKey={refreshKey} />}
        {activeTab === 'pii'         && <PIITab datasetId={id} refreshKey={refreshKey} />}
        {activeTab === 'prediction'  && <PredictionTab datasetId={id} refreshKey={refreshKey} />}
        {activeTab === 'alerts'      && <DatasetAlertsTab datasetId={id} dataset={dataset} refreshKey={refreshKey} />}
        {activeTab === 'versions'    && <VersionHistoryTab datasetId={id} dataset={dataset} onDatasetChanged={triggerRefresh} />}
      </div>
    </div>
  )
}
