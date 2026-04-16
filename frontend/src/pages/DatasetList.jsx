import { useEffect, useState } from 'react'
import { Link } from 'react-router-dom'
import { Search, ChevronRight, Database, RefreshCw, Trash2, FileBarChart2 } from 'lucide-react'
import toast from 'react-hot-toast'
import { datasetsAPI } from '../services/api'
import { FileTypeBadge } from '../components/Badge'
import { PageSpinner } from '../components/LoadingSpinner'
import ErrorAlert from '../components/ErrorAlert'
import EmptyState from '../components/EmptyState'
import ConfirmDialog from '../components/ConfirmDialog'

function formatDate(str) {
  return new Date(str).toLocaleDateString('en-IN', {
    year: 'numeric', month: 'short', day: 'numeric', timeZone: 'Asia/Kolkata',
  })
}

function formatSize(bytes) {
  if (!bytes) return '—'
  if (bytes < 1024) return `${bytes} B`
  if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(1)} KB`
  return `${(bytes / (1024 * 1024)).toFixed(1)} MB`
}

export default function DatasetList() {
  const [datasets,      setDatasets]      = useState([])
  const [loading,       setLoading]       = useState(true)
  const [error,         setError]         = useState(null)
  const [query,         setQuery]         = useState('')
  const [deleteTarget,  setDeleteTarget]  = useState(null)   // dataset to delete
  const [deleting,      setDeleting]      = useState(false)

  const load = async () => {
    setLoading(true); setError(null)
    try   { setDatasets(await datasetsAPI.getAll()) }
    catch (e) { setError(e.message) }
    finally   { setLoading(false) }
  }

  useEffect(() => { load() }, [])

  const handleDeleteConfirm = async () => {
    if (!deleteTarget) return
    setDeleting(true)
    try {
      await datasetsAPI.deleteDataset(deleteTarget._id)
      toast.success(`"${deleteTarget.name}" deleted.`)
      setDatasets(prev => prev.filter(d => d._id !== deleteTarget._id))
      setDeleteTarget(null)
    } catch (e) {
      toast.error(e.message)
    } finally {
      setDeleting(false)
    }
  }

  const filtered = datasets.filter(d =>
    d.name.toLowerCase().includes(query.toLowerCase()) ||
    d.original_filename.toLowerCase().includes(query.toLowerCase())
  )

  return (
    <div className="p-8 space-y-6 animate-fade-in">

      {/* Delete confirmation */}
      <ConfirmDialog
        open={!!deleteTarget}
        danger
        title="Delete Dataset"
        message={`Permanently delete "${deleteTarget?.name}" and all its versions? This cannot be undone.`}
        confirmLabel="Delete"
        loading={deleting}
        onConfirm={handleDeleteConfirm}
        onCancel={() => setDeleteTarget(null)}
      />

      {/* Header */}
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-2xl font-bold text-slate-900">Datasets</h1>
          <p className="text-sm text-slate-500 mt-1">
            {datasets.length} dataset{datasets.length !== 1 ? 's' : ''} uploaded
          </p>
        </div>
        <div className="flex items-center gap-3">
          <button onClick={load} className="btn-secondary">
            <RefreshCw size={14} /> Refresh
          </button>
          <Link to="/upload" className="btn-primary">
            + Upload Dataset
          </Link>
        </div>
      </div>

      {/* Search */}
      <div className="relative max-w-sm">
        <Search size={15} className="absolute left-3 top-1/2 -translate-y-1/2 text-slate-400" />
        <input
          className="input-base pl-9"
          placeholder="Search datasets…"
          value={query}
          onChange={e => setQuery(e.target.value)}
        />
      </div>

      {/* Content */}
      {loading ? <PageSpinner text="Loading datasets…" /> :
       error   ? <ErrorAlert message={error} onRetry={load} /> :
       filtered.length === 0 ? (
         <EmptyState
           title={query ? 'No results' : 'No datasets yet'}
           description={query ? `No datasets match "${query}"` : 'Upload a CSV or JSON file to get started.'}
           action={!query && <Link to="/upload" className="btn-primary">Upload Dataset</Link>}
         />
       ) : (
        <div className="card overflow-hidden">
          <table className="table-base">
            <thead>
              <tr>
                <th>Name</th>
                <th>File</th>
                <th>Type</th>
                <th>Size</th>
                <th>Rows</th>
                <th>Cols</th>
                <th>Versions</th>
                <th>Uploaded</th>
                <th />
              </tr>
            </thead>
            <tbody>
              {filtered.map(ds => (
                <tr key={ds._id} className="group">
                  <td>
                    <div className="flex items-center gap-2">
                      <div className="w-7 h-7 rounded-lg bg-blue-100 flex items-center justify-center flex-shrink-0">
                        <Database size={13} className="text-blue-500" />
                      </div>
                      <span className="font-medium text-slate-800 truncate max-w-[160px]">{ds.name}</span>
                    </div>
                  </td>
                  <td className="text-slate-500 text-xs max-w-[140px] truncate" title={ds.original_filename}>
                    {ds.original_filename}
                  </td>
                  <td><FileTypeBadge type={ds.file_type} /></td>
                  <td className="text-slate-500 text-xs">{formatSize(ds.size_bytes)}</td>
                  <td className="text-slate-600">{ds.row_count?.toLocaleString()}</td>
                  <td className="text-slate-600">{ds.col_count}</td>
                  <td>
                    <span className="inline-flex items-center px-2 py-0.5 rounded-full text-xs font-medium bg-slate-100 text-slate-600">
                      {ds.versions?.length ?? 1} ver{ds.versions?.length !== 1 ? 's' : ''}
                    </span>
                  </td>
                  <td className="text-slate-500 text-xs">{formatDate(ds.uploaded_at)}</td>
                  <td>
                    <div className="flex items-center gap-1.5">
                      <Link
                        to={`/dataset/${ds._id}?tab=report`}
                        className="p-1.5 rounded-lg text-slate-400 hover:text-violet-600 hover:bg-violet-50 transition-colors"
                        title="View Report"
                      >
                        <FileBarChart2 size={15} />
                      </Link>
                      <Link
                        to={`/dataset/${ds._id}`}
                        className="btn-secondary text-xs"
                      >
                        View <ChevronRight size={13} />
                      </Link>
                      <button
                        onClick={() => setDeleteTarget(ds)}
                        className="p-1.5 rounded-lg text-slate-400 hover:text-red-600 hover:bg-red-50 transition-colors"
                        title="Delete dataset"
                      >
                        <Trash2 size={15} />
                      </button>
                    </div>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </div>
  )
}

