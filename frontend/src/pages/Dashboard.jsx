import { useEffect, useState } from 'react'
import { Link } from 'react-router-dom'
import {
  Database, Bell, AlertTriangle, FileText, Clock,
  ChevronRight, TrendingUp,
} from 'lucide-react'
import {
  PieChart, Pie, Cell, Tooltip, ResponsiveContainer, Legend,
  BarChart, Bar, XAxis, YAxis, CartesianGrid,
} from 'recharts'
import { datasetsAPI, alertsAPI } from '../services/api'
import StatCard from '../components/StatCard'
import { SeverityBadge, FileTypeBadge, AlertTypeBadge } from '../components/Badge'
import { PageSpinner } from '../components/LoadingSpinner'
import ErrorAlert from '../components/ErrorAlert'

const SEVERITY_COLORS = {
  critical: '#ef4444',
  high:     '#f97316',
  medium:   '#eab308',
  low:      '#3b82f6',
}

function timeAgo(dateStr) {
  const diff = Date.now() - new Date(dateStr).getTime()
  const mins  = Math.floor(diff / 60000)
  const hours = Math.floor(diff / 3600000)
  const days  = Math.floor(diff / 86400000)
  if (mins < 1)   return 'just now'
  if (mins < 60)  return `${mins}m ago`
  if (hours < 24) return `${hours}h ago`
  return `${days}d ago`
}

export default function Dashboard() {
  const [datasets, setDatasets] = useState(null)
  const [alerts,   setAlerts]   = useState(null)
  const [loading,  setLoading]  = useState(true)
  const [error,    setError]    = useState(null)

  const load = async () => {
    setLoading(true); setError(null)
    try {
      const [ds, al] = await Promise.all([datasetsAPI.getAll(), alertsAPI.getAll()])
      setDatasets(ds)
      setAlerts(al)
    } catch (e) {
      setError(e.message)
    } finally {
      setLoading(false)
    }
  }

  useEffect(() => { load() }, [])

  if (loading) return <PageSpinner text="Loading dashboard…" />
  if (error)   return (
    <div className="p-8"><ErrorAlert message={error} onRetry={load} /></div>
  )

  // ── Derived stats ──────────────────────────────────────────────────────────
  const totalDatasets  = datasets?.length ?? 0
  const totalAlerts    = alerts?.length ?? 0
  const criticalAlerts = alerts?.filter(a => a.severity === 'critical').length ?? 0
  const unreadAlerts   = alerts?.filter(a => !a.is_read).length ?? 0

  // Severity pie data
  const severityCounts = ['critical','high','medium','low'].map(s => ({
    name: s.charAt(0).toUpperCase() + s.slice(1),
    value: alerts.filter(a => a.severity === s).length,
    color: SEVERITY_COLORS[s],
  })).filter(d => d.value > 0)

  // Alert type bar data
  const typeCounts = {}
  alerts.forEach(a => { typeCounts[a.alert_type] = (typeCounts[a.alert_type] || 0) + 1 })
  const alertTypeData = Object.entries(typeCounts).map(([k, v]) => ({
    name: { high_missing: 'High Missing', anomaly_detected: 'Anomaly', quality_degraded: 'Quality Drop', drift_detected: 'Drift' }[k] || k,
    count: v,
  }))

  // File type bar data
  const csvCount  = datasets.filter(d => d.file_type === 'csv').length
  const jsonCount = datasets.filter(d => d.file_type === 'json').length

  const recentDatasets = [...datasets].sort((a,b) => new Date(b.uploaded_at) - new Date(a.uploaded_at)).slice(0, 5)
  const recentAlerts   = [...alerts]  .sort((a,b) => new Date(b.triggered_at) - new Date(a.triggered_at)).slice(0, 6)

  return (
    <div className="p-8 space-y-8 animate-fade-in">

      {/* ── Header ── */}
      <div>
        <h1 className="text-2xl font-bold text-slate-900">Dashboard</h1>
        <p className="text-sm text-slate-500 mt-1">Overview of your data quality ecosystem</p>
      </div>

      {/* ── Stat Cards ── */}
      <div className="grid grid-cols-2 xl:grid-cols-4 gap-5">
        <StatCard title="Total Datasets"   value={totalDatasets}  icon={Database}       color="blue"
          subtitle={`${csvCount} CSV · ${jsonCount} JSON`} />
        <StatCard title="Total Alerts"     value={totalAlerts}    icon={Bell}           color="orange"
          subtitle={`${unreadAlerts} unread`} />
        <StatCard title="Critical Alerts"  value={criticalAlerts} icon={AlertTriangle}  color="red"
          subtitle="Needs immediate action" />
        <StatCard title="This Week"
          value={datasets.filter(d => Date.now() - new Date(d.uploaded_at) < 7*86400000).length}
          icon={TrendingUp} color="green" subtitle="Datasets uploaded" />
      </div>

      {/* ── Charts Row ── */}
      <div className="grid grid-cols-1 lg:grid-cols-3 gap-6">

        {/* Alert Severity Pie */}
        <div className="card">
          <div className="card-header">
            <h2 className="font-semibold text-slate-800 text-sm">Alert Severity Distribution</h2>
          </div>
          <div className="card-body flex items-center justify-center">
            {severityCounts.length === 0 ? (
              <p className="text-slate-400 text-sm py-8">No alerts yet</p>
            ) : (
              <ResponsiveContainer width="100%" height={220}>
                <PieChart>
                  <Pie data={severityCounts} cx="50%" cy="50%" outerRadius={80}
                    dataKey="value" nameKey="name" stroke="none">
                    {severityCounts.map((d) => <Cell key={d.name} fill={d.color} />)}
                  </Pie>
                  <Tooltip formatter={(v, n) => [v, n]} />
                  <Legend iconType="circle" iconSize={8} />
                </PieChart>
              </ResponsiveContainer>
            )}
          </div>
        </div>

        {/* Alert Type Bar */}
        <div className="card">
          <div className="card-header">
            <h2 className="font-semibold text-slate-800 text-sm">Alerts by Type</h2>
          </div>
          <div className="card-body">
            {alertTypeData.length === 0 ? (
              <p className="text-slate-400 text-sm py-8 text-center">No alerts yet</p>
            ) : (
              <ResponsiveContainer width="100%" height={220}>
                <BarChart data={alertTypeData} layout="vertical" margin={{ left: 10, right: 20 }}>
                  <CartesianGrid strokeDasharray="3 3" stroke="#f1f5f9" horizontal={false} />
                  <XAxis type="number" tick={{ fontSize: 12, fill: '#94a3b8' }} />
                  <YAxis type="category" dataKey="name" width={100} tick={{ fontSize: 11, fill: '#64748b' }} />
                  <Tooltip cursor={{ fill: '#f8fafc' }} />
                  <Bar dataKey="count" fill="#3b82f6" radius={[0, 4, 4, 0]} />
                </BarChart>
              </ResponsiveContainer>
            )}
          </div>
        </div>

        {/* File Type Breakdown */}
        <div className="card">
          <div className="card-header">
            <h2 className="font-semibold text-slate-800 text-sm">Dataset File Types</h2>
          </div>
          <div className="card-body flex items-center justify-center">
            <ResponsiveContainer width="100%" height={220}>
              <PieChart>
                <Pie
                  data={[
                    { name: 'CSV',  value: csvCount  || 0, color: '#14b8a6' },
                    { name: 'JSON', value: jsonCount || 0, color: '#8b5cf6' },
                  ].filter(d => d.value > 0)}
                  cx="50%" cy="50%" outerRadius={80} dataKey="value" stroke="none"
                >
                  {[{ color: '#14b8a6' }, { color: '#8b5cf6' }].map((c, i) => (
                    <Cell key={i} fill={c.color} />
                  ))}
                </Pie>
                <Tooltip />
                <Legend iconType="circle" iconSize={8} />
              </PieChart>
            </ResponsiveContainer>
          </div>
        </div>
      </div>

      {/* ── Recent Activity Row ── */}
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">

        {/* Recent Datasets */}
        <div className="card">
          <div className="card-header">
            <h2 className="font-semibold text-slate-800 text-sm">Recent Datasets</h2>
            <Link to="/datasets" className="text-xs text-blue-600 hover:underline flex items-center gap-1">
              View all <ChevronRight size={13} />
            </Link>
          </div>
          <div>
            {recentDatasets.length === 0 ? (
              <p className="p-6 text-sm text-slate-400 text-center">No datasets uploaded yet</p>
            ) : (
              <table className="table-base">
                <thead>
                  <tr>
                    <th>Name</th>
                    <th>Type</th>
                    <th>Rows</th>
                    <th>Uploaded</th>
                    <th />
                  </tr>
                </thead>
                <tbody>
                  {recentDatasets.map(ds => (
                    <tr key={ds._id}>
                      <td className="font-medium text-slate-800 max-w-[140px] truncate">{ds.name}</td>
                      <td><FileTypeBadge type={ds.file_type} /></td>
                      <td className="text-slate-500">{ds.row_count?.toLocaleString()}</td>
                      <td className="text-slate-400 text-xs">{timeAgo(ds.uploaded_at)}</td>
                      <td>
                        <Link to={`/dataset/${ds._id}`}
                          className="text-blue-500 hover:text-blue-700">
                          <ChevronRight size={15} />
                        </Link>
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            )}
          </div>
        </div>

        {/* Recent Alerts */}
        <div className="card">
          <div className="card-header">
            <h2 className="font-semibold text-slate-800 text-sm">Recent Alerts</h2>
            <Link to="/alerts" className="text-xs text-blue-600 hover:underline flex items-center gap-1">
              View all <ChevronRight size={13} />
            </Link>
          </div>
          <div className="divide-y divide-slate-100">
            {recentAlerts.length === 0 ? (
              <p className="p-6 text-sm text-slate-400 text-center">No alerts yet</p>
            ) : recentAlerts.map(alert => (
              <div key={alert._id} className="px-5 py-3 flex items-start gap-3">
                <SeverityBadge severity={alert.severity} />
                <div className="flex-1 min-w-0">
                  <p className="text-xs font-medium text-slate-700 truncate">{alert.message}</p>
                  <p className="text-[11px] text-slate-400 mt-0.5">{alert.dataset_name} · {timeAgo(alert.triggered_at)}</p>
                </div>
              </div>
            ))}
          </div>
        </div>

      </div>
    </div>
  )
}
