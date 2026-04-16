import { useEffect, useState } from 'react'
import { AlertTriangle, Zap } from 'lucide-react'
import { datasetsAPI } from '../../services/api'
import { PageSpinner } from '../../components/LoadingSpinner'
import ErrorAlert from '../../components/ErrorAlert'
import EmptyState from '../../components/EmptyState'
import {
  BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer, Cell,
} from 'recharts'

const SEVERITY_COLORS = {
  high:   { bg: 'bg-red-50 border-red-200',    badge: 'bg-red-100 text-red-700',    bar: '#ef4444' },
  medium: { bg: 'bg-orange-50 border-orange-200', badge: 'bg-orange-100 text-orange-700', bar: '#f97316' },
  low:    { bg: 'bg-yellow-50 border-yellow-200', badge: 'bg-yellow-100 text-yellow-700', bar: '#eab308' },
}

function severityColor(severity) {
  return SEVERITY_COLORS[severity] || SEVERITY_COLORS.low
}

function AnomalyValueBadge({ value }) {
  return (
    <span className="inline-flex items-center px-2 py-0.5 rounded font-mono text-xs bg-red-100 text-red-700 border border-red-200 max-w-[120px] truncate">
      {String(value) || '""'}
    </span>
  )
}

export default function AnomaliesTab({ datasetId, dataset, refreshKey }) {
  const [data,    setData]    = useState(null)
  const [loading, setLoading] = useState(true)
  const [error,   setError]   = useState(null)
  const [method,  setMethod]  = useState('all')

  const load = async () => {
    setLoading(true); setError(null)
    try   { setData(await datasetsAPI.getAnomalies(datasetId, null, method)) }
    catch (e) { setError(e.message) }
    finally   { setLoading(false) }
  }

  useEffect(() => { load() }, [datasetId, method, refreshKey])

  if (loading) return <PageSpinner text="Detecting anomalies…" />
  if (error)   return <ErrorAlert message={error} onRetry={load} />

  const results   = data?.results ?? []
  const totalAnom = results.reduce((s, r) => s + r.anomaly_count, 0)

  const chartData = [...results]
    .sort((a, b) => b.anomaly_count - a.anomaly_count)
    .map(r => ({
      name:   r.column,
      count:  r.anomaly_count,
      color:  severityColor(r.severity || 'low').bar,
    }))

  return (
    <div className="space-y-6 animate-fade-in max-w-4xl">

      {/* Controls + summary */}
      <div className="flex items-center justify-between flex-wrap gap-3">
        <div className="flex items-center gap-3">
          <label className="text-sm font-medium text-slate-600">Method:</label>
          <select className="select-base w-44" value={method} onChange={e => setMethod(e.target.value)}>
            <option value="all">All methods</option>
            <option value="iqr">IQR</option>
            <option value="zscore">Z-score</option>
            <option value="isolation_forest">Isolation Forest</option>
          </select>
        </div>
        <div className="flex items-center gap-3 text-sm">
          <span className="font-semibold text-slate-800">{results.length} column{results.length !== 1 ? 's' : ''} affected</span>
          <span className="text-slate-400">·</span>
          <span className={`font-bold ${totalAnom > 0 ? 'text-red-600' : 'text-emerald-600'}`}>
            {totalAnom} anomal{totalAnom !== 1 ? 'ies' : 'y'}
          </span>
        </div>
      </div>

      {results.length === 0 ? (
        <EmptyState
          title="No anomalies detected"
          description="Your dataset looks clean! Try switching the detection method."
        />
      ) : (
        <>
          {/* Bar chart */}
          <div className="card">
            <div className="card-header">
              <h2 className="font-semibold text-slate-800 text-sm flex items-center gap-2">
                <AlertTriangle size={15} className="text-red-500" />
                Anomaly Count by Column
              </h2>
            </div>
            <div className="card-body">
              <ResponsiveContainer width="100%" height={Math.max(180, results.length * 36)}>
                <BarChart data={chartData} layout="vertical" margin={{ left: 10, right: 30 }}>
                  <CartesianGrid strokeDasharray="3 3" stroke="#f1f5f9" horizontal={false} />
                  <XAxis type="number" tick={{ fontSize: 11, fill: '#94a3b8' }} />
                  <YAxis type="category" dataKey="name" width={120} tick={{ fontSize: 11, fill: '#64748b' }} />
                  <Tooltip cursor={{ fill: '#fff8f0' }} formatter={v => [`${v} anomalies`, 'Count']} />
                  <Bar dataKey="count" radius={[0, 4, 4, 0]}>
                    {chartData.map((d, i) => <Cell key={i} fill={d.color} />)}
                  </Bar>
                </BarChart>
              </ResponsiveContainer>
            </div>
          </div>

          {/* Per-column detail cards */}
          <div className="space-y-4">
            {results.map(r => {
              const c = severityColor(r.severity || 'low')
              return (
                <div key={r.column} className={`rounded-xl border p-5 ${c.bg}`}>
                  <div className="flex items-center justify-between flex-wrap gap-2 mb-3">
                    <div className="flex items-center gap-2">
                      <Zap size={15} className="text-red-500" />
                      <span className="font-semibold text-slate-800">{r.column}</span>
                    </div>
                    <div className="flex items-center gap-2">
                      <span className={`text-xs font-bold px-2 py-0.5 rounded-full ${c.badge}`}>
                        {r.anomaly_count} anomal{r.anomaly_count !== 1 ? 'ies' : 'y'}
                      </span>
                      <div className="flex flex-wrap gap-1">
                        {(r.methods_used || []).map(m => (
                          <span key={m} className="text-[10px] font-medium bg-white/70 border border-slate-200 text-slate-500 px-1.5 py-0.5 rounded">
                            {m}
                          </span>
                        ))}
                      </div>
                    </div>
                  </div>
                  <div className="flex flex-wrap gap-1.5">
                    {(r.anomalies || []).slice(0, 30).map((v, i) => (
                      <AnomalyValueBadge key={i} value={v} />
                    ))}
                    {r.anomalies?.length > 30 && (
                      <span className="text-xs text-slate-400 self-center">
                        +{r.anomalies.length - 30} more
                      </span>
                    )}
                  </div>
                </div>
              )
            })}
          </div>
        </>
      )}
    </div>
  )
}
