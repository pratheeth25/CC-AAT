import { useEffect, useState } from 'react'
import { datasetsAPI } from '../../services/api'
import { PageSpinner } from '../../components/LoadingSpinner'
import ErrorAlert from '../../components/ErrorAlert'
import { Clock, GitBranch, RotateCcw, Check, FileText, Trash2, Download } from 'lucide-react'
import toast from 'react-hot-toast'
import ConfirmDialog from '../../components/ConfirmDialog'

export default function VersionHistoryTab({ datasetId, dataset, onDatasetChanged }) {
  const [versions, setVersions] = useState([])
  const [loading,  setLoading]  = useState(true)
  const [error,    setError]    = useState(null)
  const [restoring, setRestoring] = useState(null)
  const [deletingVersion, setDeletingVersion] = useState(null)
  const [showDeleteConfirm, setShowDeleteConfirm] = useState(null)

  const load = async () => {
    setLoading(true); setError(null)
    try {
      const data = await datasetsAPI.getVersions(datasetId)
      setVersions(data.versions || [])
    } catch (e) {
      setError(e.message)
    } finally {
      setLoading(false)
    }
  }

  useEffect(() => { load() }, [datasetId])

  const handleRestore = async (versionNum) => {
    setRestoring(versionNum)
    try {
      await datasetsAPI.restoreVersion(datasetId, versionNum)
      toast.success(`Restored to version ${versionNum}`)
      await load()
      onDatasetChanged?.()
    } catch (e) {
      toast.error(`Failed to restore: ${e.message}`)
    } finally {
      setRestoring(null)
    }
  }

  const handleDeleteVersion = async (versionNum) => {
    setDeletingVersion(versionNum)
    try {
      const result = await datasetsAPI.deleteVersion(datasetId, versionNum)
      toast.success(result.message || `Version ${versionNum} deleted`)
      setShowDeleteConfirm(null)
      await load()
      onDatasetChanged?.()
    } catch (e) {
      toast.error(`Failed to delete: ${e.message}`)
    } finally {
      setDeletingVersion(null)
    }
  }

  if (loading) return <PageSpinner text="Loading version history…" />
  if (error)   return <ErrorAlert message={error} onRetry={load} />

  if (versions.length === 0) {
    return (
      <div className="card p-12 text-center">
        <GitBranch className="w-12 h-12 text-slate-300 mx-auto mb-4" />
        <h3 className="text-lg font-semibold text-slate-600">No Version History</h3>
        <p className="text-sm text-slate-400 mt-2">Version history will appear after cleaning operations.</p>
      </div>
    )
  }

  return (
    <div className="space-y-4 animate-fade-in max-w-3xl">
      {/* Delete version confirmation */}
      <ConfirmDialog
        open={showDeleteConfirm != null}
        danger
        title="Delete Version"
        message={`Permanently delete version ${showDeleteConfirm}? This cannot be undone.`}
        confirmLabel="Delete Version"
        loading={deletingVersion === showDeleteConfirm}
        onConfirm={() => handleDeleteVersion(showDeleteConfirm)}
        onCancel={() => setShowDeleteConfirm(null)}
      />

      <div className="flex items-center gap-2 mb-2">
        <GitBranch className="w-5 h-5 text-slate-500" />
        <h2 className="text-sm font-semibold text-slate-700">
          {versions.length} Version{versions.length !== 1 ? 's' : ''}
        </h2>
      </div>

      {/* Timeline */}
      <div className="relative">
        {/* Vertical line */}
        <div className="absolute left-5 top-0 bottom-0 w-0.5 bg-slate-200" />

        <div className="space-y-4">
          {versions.map((v, idx) => {
            const isLatest = v.is_latest
            const date = v.created_at ? new Date(v.created_at).toLocaleString() : 'Unknown'
            return (
              <div key={v.version_number} className="relative pl-12">
                {/* Timeline dot */}
                <div className={`absolute left-3.5 top-4 w-3.5 h-3.5 rounded-full border-2 ${isLatest ? 'bg-blue-500 border-blue-500' : 'bg-white border-slate-300'}`} />

                <div className={`card p-4 ${isLatest ? 'border-blue-300 border-2' : ''}`}>
                  <div className="flex items-start justify-between">
                    <div>
                      <div className="flex items-center gap-2">
                        <span className="text-sm font-bold text-slate-800">
                          Version {v.version_number}
                        </span>
                        {isLatest && (
                          <span className="text-[10px] font-semibold bg-blue-100 text-blue-700 px-2 py-0.5 rounded-full">
                            CURRENT
                          </span>
                        )}
                        <span className="text-[10px] font-medium bg-slate-100 text-slate-500 px-2 py-0.5 rounded-full capitalize">
                          {v.source}
                        </span>
                      </div>
                      <div className="flex items-center gap-3 mt-1.5 text-xs text-slate-400">
                        <span className="flex items-center gap-1">
                          <Clock className="w-3 h-3" /> {date}
                        </span>
                        <span>{v.row_count} rows × {v.col_count} cols</span>
                      </div>
                      {v.checksum && (
                        <p className="text-[10px] text-slate-300 mt-1 font-mono">
                          SHA-256: {v.checksum.substring(0, 16)}…
                        </p>
                      )}
                      {v.changes_applied && v.changes_applied.length > 0 && (
                        <div className="mt-2 flex flex-wrap gap-1">
                          {v.changes_applied.map((c, ci) => (
                            <span key={ci} className="text-[10px] bg-emerald-50 text-emerald-600 px-1.5 py-0.5 rounded">
                              {c}
                            </span>
                          ))}
                        </div>
                      )}
                    </div>

                    <div className="flex items-center gap-2">
                        <a
                          href={`${import.meta.env.VITE_API_URL || 'http://localhost:8000'}/dataset/${datasetId}/versions/${v.version_number}/download`}
                          download
                          className="flex items-center gap-1.5 text-xs font-medium text-slate-500 hover:text-slate-800 hover:bg-slate-50 px-3 py-1.5 rounded-lg transition-colors"
                          title="Download this version"
                        >
                          <Download className="w-3.5 h-3.5" />
                          Download
                        </a>
                        {!isLatest && (
                          <>
                            <button
                              onClick={() => handleRestore(v.version_number)}
                              disabled={restoring === v.version_number}
                              className="flex items-center gap-1.5 text-xs font-medium text-blue-600 hover:text-blue-800 hover:bg-blue-50 px-3 py-1.5 rounded-lg transition-colors disabled:opacity-50"
                            >
                              <RotateCcw className={`w-3.5 h-3.5 ${restoring === v.version_number ? 'animate-spin' : ''}`} />
                              Restore
                            </button>
                            {versions.length > 1 && (
                              <button
                                onClick={() => setShowDeleteConfirm(v.version_number)}
                                disabled={deletingVersion === v.version_number}
                                className="flex items-center gap-1.5 text-xs font-medium text-red-500 hover:text-red-700 hover:bg-red-50 px-3 py-1.5 rounded-lg transition-colors disabled:opacity-50"
                              >
                                <Trash2 className="w-3.5 h-3.5" />
                                Delete
                              </button>
                            )}
                          </>
                        )}
                      </div>
                  </div>
                </div>
              </div>
            )
          })}
        </div>
      </div>
    </div>
  )
}
