import { useEffect, useState } from 'react'
import { TrendingDown, TrendingUp, Minus, AlertTriangle } from 'lucide-react'
import {
  LineChart, Line, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer,
  ReferenceLine, Legend, Area, ComposedChart,
} from 'recharts'
import { datasetsAPI } from '../../services/api'
import { PageSpinner } from '../../components/LoadingSpinner'
import ErrorAlert from '../../components/ErrorAlert'
import EmptyState from '../../components/EmptyState'

const RISK_COLORS = {
  high: { bg: 'bg-red-50 border-red-300', text: 'text-red-700', icon: 'text-red-500' },
  medium: { bg: 'bg-orange-50 border-orange-300', text: 'text-orange-700', icon: 'text-orange-500' },
  low: { bg: 'bg-yellow-50 border-yellow-300', text: 'text-yellow-700', icon: 'text-yellow-500' },
  none: { bg: 'bg-emerald-50 border-emerald-300', text: 'text-emerald-700', icon: 'text-emerald-500' },
}

const TrendIcon = ({ trend }) => {
  if (trend === 'declining') return <TrendingDown className="w-5 h-5 text-red-500" />
  if (trend === 'improving') return <TrendingUp className="w-5 h-5 text-emerald-500" />
  return <Minus className="w-5 h-5 text-slate-400" />
}

export default function PredictionTab({ datasetId }) {
  const [data, setData] = useState(null)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState(null)
  const [sla, setSla] = useState(70)

  const load = async () => {
    setLoading(true); setError(null)
    try { setData(await datasetsAPI.getPrediction(datasetId, sla)) }
    catch (e) { setError(e.message) }
    finally { setLoading(false) }
  }

  useEffect(() => { load() }, [datasetId, sla])

  if (loading) return <PageSpinner text="Predicting quality trajectory…" />
  if (error) return <ErrorAlert message={error} onRetry={load} />

  if (!data?.historical?.length) {
    return (
      <EmptyState
        title="No version history"
        description="Upload multiple versions to see quality trend predictions."
      />
    )
  }

  const riskStyle = RISK_COLORS[data.breach_risk] || RISK_COLORS.none

  // Build chart data
  const chartData = [
    ...data.historical.map(h => ({ version: `v${h.version}`, actual: h.score, predicted: null })),
    // Bridge: last historical point is also first "predicted" point
    ...(data.predicted.length > 0
      ? [{ version: `v${data.historical[data.historical.length - 1].version}`, actual: data.historical[data.historical.length - 1].score, predicted: data.historical[data.historical.length - 1].score }]
      : []),
    ...data.predicted.map(p => ({ version: `v${p.version}`, actual: null, predicted: p.score })),
  ]

  // De-duplicate the bridge point with the last historical
  const seen = new Set()
  const uniqueChartData = chartData.filter(d => {
    if (seen.has(d.version)) return false
    seen.add(d.version)
    return true
  })

  // If we have predicted points, re-add the bridge
  if (data.predicted.length > 0) {
    const lastHistIdx = uniqueChartData.findIndex(d => d.version === `v${data.historical[data.historical.length - 1].version}`)
    if (lastHistIdx >= 0) {
      uniqueChartData[lastHistIdx].predicted = uniqueChartData[lastHistIdx].actual
    }
  }

  return (
    <div className="space-y-6 animate-fade-in max-w-4xl">

      {/* Controls */}
      <div className="flex items-center gap-4 flex-wrap">
        <label className="text-sm font-medium text-slate-600">SLA Threshold:</label>
        <input
          type="number"
          className="select-base w-24 text-center"
          value={sla}
          min={0}
          max={100}
          onChange={e => setSla(Number(e.target.value))}
        />
      </div>

      {/* Risk banner */}
      <div className={`rounded-xl border p-5 ${riskStyle.bg}`}>
        <div className="flex items-center gap-3">
          <AlertTriangle className={`w-6 h-6 ${riskStyle.icon}`} />
          <div>
            <h2 className={`font-semibold text-sm ${riskStyle.text}`}>
              Breach Risk: <span className="uppercase">{data.breach_risk}</span>
            </h2>
            <p className="text-xs text-slate-600 mt-0.5">{data.message}</p>
          </div>
        </div>
      </div>

      {/* Stats row */}
      <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
        {[
          { label: 'Current Score', value: data.current_score != null ? data.current_score.toFixed(0) : '—', color: 'text-slate-800' },
          { label: 'Trend', value: data.trend, color: data.trend === 'declining' ? 'text-red-600' : data.trend === 'improving' ? 'text-emerald-600' : 'text-slate-600', icon: <TrendIcon trend={data.trend} /> },
          { label: 'Slope', value: `${data.slope > 0 ? '+' : ''}${data.slope} pts/ver`, color: data.slope < 0 ? 'text-red-600' : 'text-emerald-600' },
          { label: 'Breach Version', value: data.estimated_breach_version ? `v${data.estimated_breach_version}` : 'None', color: data.estimated_breach_version ? 'text-red-600' : 'text-emerald-600' },
        ].map(({ label, value, color, icon }) => (
          <div key={label} className="card p-4">
            <p className="text-xs text-slate-400 font-medium">{label}</p>
            <div className="flex items-center gap-2 mt-1">
              {icon}
              <p className={`text-lg font-bold ${color} capitalize`}>{value}</p>
            </div>
          </div>
        ))}
      </div>

      {/* Chart */}
      <div className="card">
        <div className="card-header">
          <h2 className="font-semibold text-slate-800 text-sm">Quality Score Trajectory</h2>
        </div>
        <div className="card-body">
          <ResponsiveContainer width="100%" height={320}>
            <ComposedChart data={uniqueChartData} margin={{ top: 10, right: 30, bottom: 10, left: 10 }}>
              <CartesianGrid strokeDasharray="3 3" stroke="#f1f5f9" />
              <XAxis dataKey="version" tick={{ fontSize: 12, fill: '#64748b' }} />
              <YAxis domain={[0, 100]} tick={{ fontSize: 12, fill: '#64748b' }} />
              <Tooltip
                contentStyle={{ borderRadius: '8px', border: '1px solid #e2e8f0', fontSize: 13 }}
                formatter={(v, name) => [v != null ? v.toFixed(1) : '—', name === 'actual' ? 'Actual' : 'Predicted']}
              />
              <Legend />
              <ReferenceLine
                y={data.sla_threshold}
                stroke="#ef4444"
                strokeDasharray="6 4"
                label={{ value: `SLA (${data.sla_threshold})`, position: 'right', fill: '#ef4444', fontSize: 11 }}
              />
              <Line
                type="monotone"
                dataKey="actual"
                stroke="#3b82f6"
                strokeWidth={2.5}
                dot={{ r: 4, fill: '#3b82f6' }}
                name="Actual"
                connectNulls={false}
              />
              <Line
                type="monotone"
                dataKey="predicted"
                stroke="#f59e0b"
                strokeWidth={2}
                strokeDasharray="6 3"
                dot={{ r: 4, fill: '#f59e0b', strokeDasharray: '' }}
                name="Predicted"
                connectNulls={false}
              />
            </ComposedChart>
          </ResponsiveContainer>
        </div>
      </div>

      {/* Version scores table */}
      {data.historical.length > 0 && (
        <div className="card overflow-hidden">
          <div className="card-header">
            <h2 className="font-semibold text-slate-800 text-sm">Historical Scores</h2>
          </div>
          <table className="table-base">
            <thead>
              <tr>
                <th>Version</th>
                <th>Score</th>
                <th>Status</th>
              </tr>
            </thead>
            <tbody>
              {data.historical.map(h => (
                <tr key={h.version}>
                  <td className="font-medium text-slate-700">v{h.version}</td>
                  <td className="font-mono text-sm font-semibold" style={{ color: h.score >= data.sla_threshold ? '#22c55e' : '#ef4444' }}>
                    {h.score.toFixed(1)}
                  </td>
                  <td>
                    {h.score >= data.sla_threshold
                      ? <span className="text-xs font-medium bg-emerald-100 text-emerald-700 px-2 py-0.5 rounded-full">Above SLA</span>
                      : <span className="text-xs font-medium bg-red-100 text-red-700 px-2 py-0.5 rounded-full">Below SLA</span>}
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
