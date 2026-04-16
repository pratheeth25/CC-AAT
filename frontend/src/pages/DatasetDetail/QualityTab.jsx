import { useEffect, useState, useRef } from 'react'
import { datasetsAPI } from '../../services/api'
import { PageSpinner } from '../../components/LoadingSpinner'
import ErrorAlert from '../../components/ErrorAlert'
import ScoreGauge from '../../components/ScoreGauge'
import { GradeBadge } from '../../components/Badge'
import { AlertTriangle, ShieldAlert, Minus } from 'lucide-react'

const DIM_LABELS = {
  completeness: 'Completeness',
  validity:     'Validity',
  consistency:  'Consistency',
  uniqueness:   'Uniqueness',
  security:     'Security',
  garbage:      'Garbage / Junk',
}

const DIM_COLORS = {
  completeness: '#3b82f6',
  validity:     '#f59e0b',
  consistency:  '#22c55e',
  uniqueness:   '#8b5cf6',
  security:     '#ef4444',
  garbage:      '#f97316',
}

function useCountUp(target, duration = 1200) {
  const [value, setValue] = useState(0)
  const raf = useRef(null)
  useEffect(() => {
    const start = performance.now()
    const tick = (now) => {
      const elapsed = now - start
      const progress = Math.min(elapsed / duration, 1)
      setValue(Math.round(progress * target))
      if (progress < 1) raf.current = requestAnimationFrame(tick)
    }
    raf.current = requestAnimationFrame(tick)
    return () => cancelAnimationFrame(raf.current)
  }, [target, duration])
  return value
}

export default function QualityTab({ datasetId, dataset }) {
  const [data,    setData]    = useState(null)
  const [loading, setLoading] = useState(true)
  const [error,   setError]   = useState(null)
  const [method,  setMethod]  = useState('all')

  const load = async () => {
    setLoading(true); setError(null)
    try   { setData(await datasetsAPI.getQuality(datasetId, null, method)) }
    catch (e) { setError(e.message) }
    finally   { setLoading(false) }
  }

  useEffect(() => { load() }, [datasetId, method])

  // Must be called unconditionally before any early returns — Rules of Hooks
  const animatedScore = useCountUp(data ? Math.round(data.total_score) : 0)

  if (loading) return <PageSpinner text="Calculating quality score…" />
  if (error)   return <ErrorAlert message={error} onRetry={load} />
  const penalties = data.penalties_applied || []
  const dimensions = data.dimension_scores || {}
  const hasSecurity = penalties.some(p => p.reason?.toLowerCase().includes('security') || p.reason?.toLowerCase().includes('xss') || p.reason?.toLowerCase().includes('sql'))
  const isF = data.grade === 'F-' || data.grade === 'F'

  return (
    <div className="space-y-6 animate-fade-in max-w-5xl">

      {/* Method selector */}
      <div className="flex items-center gap-3">
        <label className="text-sm font-medium text-slate-600">Anomaly method:</label>
        <select className="select-base w-48" value={method} onChange={e => setMethod(e.target.value)}>
          <option value="all">All methods</option>
          <option value="iqr">IQR only</option>
          <option value="zscore">Z-score only</option>
          <option value="isolation_forest">Isolation Forest</option>
        </select>
      </div>

      {/* Security Alert Banner */}
      {hasSecurity && (
        <div className="bg-red-50 border border-red-300 rounded-xl p-4 flex items-start gap-3">
          <ShieldAlert className="w-6 h-6 text-red-600 flex-shrink-0 mt-0.5" />
          <div>
            <h3 className="text-red-800 font-semibold text-sm">Security Threats Detected</h3>
            <p className="text-red-600 text-xs mt-1">
              This dataset contains potential security payloads (XSS, SQL injection, etc.).
              The quality score has been hard-capped. Clean or remove malicious rows before using this data.
            </p>
          </div>
        </div>
      )}

      {/* Score + Verdict */}
      <div className="grid grid-cols-1 md:grid-cols-3 gap-6">

        {/* Gauge with animated score */}
        <div className={`card flex flex-col items-center justify-center py-8 ${isF ? 'border-red-400 border-2' : ''} ${data.grade === 'F-' ? 'animate-pulse' : ''}`}>
          <ScoreGauge score={data.total_score} grade={data.grade} size={200} />
          <p className="text-2xl font-bold mt-2" style={{ color: data.total_score < 20 ? '#dc2626' : data.total_score < 40 ? '#ef4444' : data.total_score < 60 ? '#f97316' : data.total_score < 75 ? '#eab308' : data.total_score < 90 ? '#3b82f6' : '#22c55e' }}>
            {animatedScore}
          </p>
          <p className="text-sm text-slate-500 text-center">{data.verdict || 'Overall Quality Score'}</p>
        </div>

        {/* Dimension scores */}
        <div className="card md:col-span-2">
          <div className="card-header">
            <h2 className="font-semibold text-slate-800 text-sm">Dimension Scores</h2>
            <GradeBadge grade={data.grade} />
          </div>
          <div className="card-body space-y-3">
            {Object.entries(dimensions).map(([key, dim]) => {
              const color = DIM_COLORS[key] || '#3b82f6'
              const score = dim.score ?? 0
              const maxBar = 100
              const pct = Math.max(0, Math.min(100, score))
              return (
                <div key={key}>
                  <div className="flex items-center justify-between mb-1">
                    <span className="text-xs font-semibold text-slate-600">{DIM_LABELS[key] || key}</span>
                    <span className="text-xs font-bold" style={{ color }}>
                      {score >= 0 ? score.toFixed(1) : score.toFixed(1)} pts
                    </span>
                  </div>
                  <div className="h-2 bg-slate-100 rounded-full overflow-hidden">
                    <div
                      className="h-full rounded-full transition-all duration-700"
                      style={{ width: `${Math.max(pct, 0)}%`, backgroundColor: color }}
                    />
                  </div>
                </div>
              )
            })}
          </div>
        </div>
      </div>

      {/* Penalties Breakdown Table */}
      {penalties.length > 0 && (
        <div className="card">
          <div className="card-header">
            <h2 className="font-semibold text-slate-800 text-sm flex items-center gap-2">
              <Minus className="w-4 h-4 text-red-500" />
              Penalties Applied ({penalties.length})
            </h2>
          </div>
          <div className="card-body overflow-x-auto">
            <table className="w-full text-sm">
              <thead>
                <tr className="border-b border-slate-100">
                  <th className="text-left py-2 text-slate-500 font-medium">#</th>
                  <th className="text-left py-2 text-slate-500 font-medium">Reason</th>
                  <th className="text-right py-2 text-slate-500 font-medium">Deduction</th>
                </tr>
              </thead>
              <tbody>
                {penalties.map((p, i) => (
                  <tr key={i} className="border-b border-slate-50 hover:bg-slate-50/50">
                    <td className="py-2 text-slate-400 text-xs">{i + 1}</td>
                    <td className="py-2 text-slate-700">{p.reason}</td>
                    <td className="py-2 text-right font-mono font-semibold text-red-500">
                      −{typeof p.deduction === 'number' ? p.deduction.toFixed(1) : p.deduction}
                    </td>
                  </tr>
                ))}
              </tbody>
              <tfoot>
                <tr className="border-t-2 border-slate-200">
                  <td colSpan={2} className="py-2 font-semibold text-slate-700">Total penalties</td>
                  <td className="py-2 text-right font-mono font-bold text-red-600">
                    −{penalties.reduce((acc, p) => acc + (p.deduction || 0), 0).toFixed(1)}
                  </td>
                </tr>
              </tfoot>
            </table>
          </div>
        </div>
      )}

      {/* Legacy breakdown cards (if available) */}
      {data.breakdown && Object.keys(data.breakdown).length > 0 && (
        <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-4">
          {Object.entries(data.breakdown).map(([key, val]) => {
            if (typeof val !== 'object' || val === null) return null
            const color = DIM_COLORS[key] || '#3b82f6'
            return (
              <div key={key} className="card p-5">
                <div className="flex items-center justify-between mb-3">
                  <span className="text-sm font-semibold text-slate-700">{DIM_LABELS[key] || key}</span>
                </div>
                <div className="space-y-1">
                  {Object.entries(val).map(([k, v]) => (
                    <div key={k} className="flex justify-between text-xs">
                      <span className="text-slate-400">{k.replace(/_/g, ' ')}</span>
                      <span className="text-slate-600 font-medium">
                        {typeof v === 'number' ? v.toLocaleString() : String(v)}
                      </span>
                    </div>
                  ))}
                </div>
              </div>
            )
          })}
        </div>
      )}
    </div>
  )
}
