import { useEffect, useState } from 'react'
import { Link } from 'react-router-dom'
import { RefreshCw, BellOff, ExternalLink } from 'lucide-react'
import toast from 'react-hot-toast'
import { alertsAPI } from '../services/api'
import { SeverityBadge, AlertTypeBadge } from '../components/Badge'
import { PageSpinner } from '../components/LoadingSpinner'
import ErrorAlert from '../components/ErrorAlert'
import EmptyState from '../components/EmptyState'

function formatTime(isoStr) {
  if (!isoStr) return '—'
  const d = new Date(isoStr)
  const diffMs = Date.now() - d.getTime()
  const diffMins = Math.floor(diffMs / 60_000)
  const diffHours = Math.floor(diffMs / 3_600_000)
  const diffDays = Math.floor(diffMs / 86_400_000)
  if (diffMins < 1)   return 'just now'
  if (diffMins < 60)  return `${diffMins}m ago`
  if (diffHours < 24) return `${diffHours}h ago`
  if (diffDays < 7)   return `${diffDays}d ago`
  return new Intl.DateTimeFormat('en-IN', {
    timeZone: 'Asia/Kolkata',
    day: 'numeric', month: 'short', year: 'numeric',
    hour: '2-digit', minute: '2-digit', hour12: true,
  }).format(d)
}

function toIST(isoStr) {
  if (!isoStr) return ''
  return new Intl.DateTimeFormat('en-IN', {
    timeZone: 'Asia/Kolkata',
    day: 'numeric', month: 'short', year: 'numeric',
    hour: '2-digit', minute: '2-digit', second: '2-digit', hour12: true,
  }).format(new Date(isoStr))
}

const SEVERITY_ORDER = { critical: 0, high: 1, medium: 2, low: 3 }

export default function AlertsPage() {
  const [alerts,   setAlerts]   = useState([])
  const [loading,  setLoading]  = useState(true)
  const [error,    setError]    = useState(null)
  const [filter,   setFilter]   = useState('all')

  const load = async () => {
    setLoading(true); setError(null)
    try   { setAlerts((await alertsAPI.getAll()) ?? []) }
    catch (e) { setError(e.message) }
    finally   { setLoading(false) }
  }

  useEffect(() => { load() }, [])

  const markRead = async (id) => {
    try {
      await alertsAPI.markRead(id)
      setAlerts(prev => prev.map(a => a._id === id ? { ...a, is_read: true } : a))
      toast.success('Marked as read')
    } catch (e) { toast.error(e.message) }
  }

  const filtered = [...(alerts ?? [])]
    .filter(a => filter === 'all' || a.severity === filter)
    .sort((a, b) => (SEVERITY_ORDER[a.severity] ?? 99) - (SEVERITY_ORDER[b.severity] ?? 99) || new Date(b.triggered_at) - new Date(a.triggered_at))

  const counts = { critical: 0, high: 0, medium: 0, low: 0 }
  alerts.forEach(a => { if (counts[a.severity] !== undefined) counts[a.severity]++ })

  return (
    <div className="p-8 space-y-6 animate-fade-in">

      {/* Header */}
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-2xl font-bold text-slate-900">Alerts</h1>
          <p className="text-sm text-slate-500 mt-1">
            {alerts.filter(a => !a.is_read).length} unread · {alerts.length} total
          </p>
        </div>
        <button onClick={load} className="btn-secondary">
          <RefreshCw size={14} /> Refresh
        </button>
      </div>

      {/* Severity filter pills */}
      <div className="flex flex-wrap gap-2">
        {[['all', 'All', alerts.length], ['critical', 'Critical', counts.critical], ['high', 'High', counts.high], ['medium', 'Medium', counts.medium], ['low', 'Low', counts.low]].map(([val, label, count]) => (
          <button
            key={val}
            onClick={() => setFilter(val)}
            className={`px-4 py-1.5 rounded-full text-sm font-medium transition-colors border ${
              filter === val
                ? 'bg-slate-900 text-white border-slate-900'
                : 'bg-white text-slate-600 border-slate-200 hover:border-slate-400'
            }`}
          >
            {label}
            <span className={`ml-1.5 text-xs px-1.5 py-0.5 rounded-full ${filter === val ? 'bg-white/20' : 'bg-slate-100'}`}>
              {count}
            </span>
          </button>
        ))}
      </div>

      {/* Content */}
      {loading ? <PageSpinner text="Loading alerts…" /> :
       error   ? <ErrorAlert message={error} onRetry={load} /> :
       filtered.length === 0 ? (
         <EmptyState
           title="No alerts"
           description={filter === 'all' ? 'Run a profile or quality check on a dataset to generate alerts.' : `No ${filter} alerts.`}
           action={<button onClick={() => setFilter('all')} className="btn-secondary">Clear Filter</button>}
         />
       ) : (
        <div className="card overflow-hidden divide-y divide-slate-100">
          {filtered.map(alert => (
            <div
              key={alert._id}
              className={`px-6 py-4 flex items-start gap-4 transition-colors ${
                !alert.is_read ? 'bg-blue-50/30' : 'bg-white'
              }`}
            >
              {/* Unread dot */}
              <div className="mt-1.5 flex-shrink-0">
                {!alert.is_read
                  ? <span className="w-2 h-2 rounded-full bg-blue-500 block" />
                  : <span className="w-2 h-2 rounded-full bg-transparent block" />
                }
              </div>

              <div className="flex-1 min-w-0">
                <div className="flex items-center gap-2 flex-wrap mb-1">
                  <SeverityBadge severity={alert.severity} />
                  <AlertTypeBadge type={alert.alert_type} />
                  {!alert.is_read && (
                    <span className="text-[10px] font-semibold text-blue-600 bg-blue-100 px-1.5 py-0.5 rounded">NEW</span>
                  )}
                </div>
                <p className="text-sm text-slate-800 font-medium">{alert.message}</p>
                <div className="flex items-center gap-3 mt-1.5 text-xs text-slate-400">
                  <span className="font-medium text-slate-500">{alert.dataset_name}</span>
                  <span>·</span>
                  <span title={toIST(alert.triggered_at)}>{formatTime(alert.triggered_at)}</span>
                </div>
              </div>

              <div className="flex items-center gap-2 flex-shrink-0">
                <Link
                  to={`/dataset/${alert.dataset_id}?tab=alerts`}
                  className="text-slate-400 hover:text-slate-600 transition-colors"
                  title="View dataset"
                >
                  <ExternalLink size={15} />
                </Link>
                {!alert.is_read && (
                  <button
                    onClick={() => markRead(alert._id)}
                    title="Mark as read"
                    className="text-slate-400 hover:text-slate-600 transition-colors"
                  >
                    <BellOff size={15} />
                  </button>
                )}
              </div>
            </div>
          ))}
        </div>
      )}
    </div>
  )
}
