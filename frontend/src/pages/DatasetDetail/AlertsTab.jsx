import { useEffect, useState } from 'react'
import { Link } from 'react-router-dom'
import { alertsAPI } from '../../services/api'
import { PageSpinner } from '../../components/LoadingSpinner'
import ErrorAlert from '../../components/ErrorAlert'
import EmptyState from '../../components/EmptyState'
import { SeverityBadge, AlertTypeBadge } from '../../components/Badge'
import toast from 'react-hot-toast'

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

export default function DatasetAlertsTab({ datasetId }) {
  const [alerts,  setAlerts]  = useState([])
  const [loading, setLoading] = useState(true)
  const [error,   setError]   = useState(null)

  const load = async () => {
    setLoading(true); setError(null)
    try   { setAlerts(await alertsAPI.getAll(datasetId)) }
    catch (e) { setError(e.message) }
    finally   { setLoading(false) }
  }

  useEffect(() => { load() }, [datasetId])

  const markRead = async (alertId) => {
    try {
      await alertsAPI.markRead(alertId)
      setAlerts(a => a.map(al => al._id === alertId ? { ...al, is_read: true } : al))
      toast.success('Marked as read')
    } catch (e) {
      toast.error(e.message)
    }
  }

  if (loading) return <PageSpinner text="Loading alerts…" />
  if (error)   return <ErrorAlert message={error} onRetry={load} />

  return (
    <div className="space-y-4 animate-fade-in max-w-3xl">

      {alerts.length === 0 ? (
        <EmptyState title="No alerts for this dataset" description="No quality or anomaly alerts have been triggered." />
      ) : (
        <>
          <p className="text-sm text-slate-500">{alerts.length} alert{alerts.length !== 1 ? 's' : ''}</p>
          {alerts.map(alert => (
            <div key={alert._id} className={`card p-5 ${!alert.is_read ? 'border-l-4 border-l-blue-500' : ''}`}>
              <div className="flex items-start justify-between gap-3">
                <div className="flex items-start gap-3 min-w-0">
                  {!alert.is_read && (
                    <span className="w-2 h-2 rounded-full bg-blue-500 flex-shrink-0 mt-1.5" />
                  )}
                  <div className="min-w-0">
                    <div className="flex flex-wrap items-center gap-2 mb-1">
                      <SeverityBadge severity={alert.severity} />
                      <AlertTypeBadge type={alert.alert_type} />
                    </div>
                    <p className="text-sm text-slate-700">{alert.message}</p>
                    {alert.details && typeof alert.details === 'object' && Object.keys(alert.details).length > 0 && (
                      <p className="text-xs text-slate-400 mt-1">
                        {Object.entries(alert.details).map(([k, v]) => `${k.replace(/_/g,' ')}: ${Array.isArray(v) ? v.slice(0,3).join(', ') : v}`).join(' · ')}
                      </p>
                    )}
                    <p className="text-xs text-slate-400 mt-2" title={toIST(alert.triggered_at)}>{formatTime(alert.triggered_at)}</p>
                  </div>
                </div>
                {!alert.is_read && (
                  <button
                    onClick={() => markRead(alert._id)}
                    className="text-xs text-blue-600 hover:text-blue-700 font-medium flex-shrink-0 whitespace-nowrap"
                  >
                    Mark read
                  </button>
                )}
              </div>
            </div>
          ))}
        </>
      )}
    </div>
  )
}
