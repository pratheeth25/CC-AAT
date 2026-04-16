import { useEffect, useState, useCallback } from 'react'
import {
  FileText, Code2, Users, RefreshCw, Copy, Download,
  CheckCircle, AlertTriangle, ShieldAlert, TrendingDown,
  ChevronDown, ChevronUp, Loader2,
} from 'lucide-react'
import { datasetsAPI } from '../../services/api'
import { PageSpinner } from '../../components/LoadingSpinner'
import ErrorAlert from '../../components/ErrorAlert'

// ── Helpers ──────────────────────────────────────────────────────────────────

const SEVERITY_STYLES = {
  high:   'bg-red-100 text-red-700 border-red-300',
  medium: 'bg-orange-100 text-orange-700 border-orange-300',
  low:    'bg-yellow-100 text-yellow-700 border-yellow-300',
}

const RISK_ICON = {
  high: <AlertTriangle size={13} className="text-red-500 flex-shrink-0" />,
  medium: <AlertTriangle size={13} className="text-orange-400 flex-shrink-0" />,
  low: <AlertTriangle size={13} className="text-yellow-400 flex-shrink-0" />,
  none: <CheckCircle size={13} className="text-emerald-500 flex-shrink-0" />,
}

function ScoreBar({ label, score, color }) {
  const pct = Math.max(0, Math.min(100, score))
  return (
    <div className="flex items-center gap-3">
      <span className="text-xs text-slate-500 w-28 shrink-0 capitalize">{label.replace(/_/g, ' ')}</span>
      <div className="flex-1 h-2 bg-slate-100 rounded-full overflow-hidden">
        <div className="h-full rounded-full transition-all duration-700" style={{ width: `${pct}%`, backgroundColor: color }} />
      </div>
      <span className="text-xs font-mono font-semibold w-12 text-right" style={{ color }}>{score.toFixed(1)}</span>
    </div>
  )
}

const DIM_COLORS = {
  completeness: '#3b82f6',
  validity: '#f59e0b',
  consistency: '#22c55e',
  uniqueness: '#8b5cf6',
  security: '#ef4444',
  garbage: '#f97316',
}

function scoreColor(s) {
  if (s >= 90) return '#22c55e'
  if (s >= 75) return '#3b82f6'
  if (s >= 60) return '#f59e0b'
  if (s >= 40) return '#f97316'
  return '#ef4444'
}

// ── Main Component ────────────────────────────────────────────────────────────

export default function ReportTab({ datasetId }) {
  const [report, setReport] = useState(null)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState(null)
  const [format, setFormat] = useState('visual')   // 'visual' | 'text' | 'json'
  const [copied, setCopied] = useState(false)
  const [expandedSections, setExpandedSections] = useState({ anomalies: true, pii: true, drift: false, sec: false })

  const load = useCallback(async () => {
    setLoading(true); setError(null)
    try { setReport(await datasetsAPI.getReport(datasetId)) }
    catch (e) { setError(e.message) }
    finally { setLoading(false) }
  }, [datasetId])

  useEffect(() => { load() }, [load])

  const toggleSection = (key) =>
    setExpandedSections(prev => ({ ...prev, [key]: !prev[key] }))

  const copyContent = () => {
    const content = format === 'json'
      ? JSON.stringify(report.json_report, null, 2)
      : format === 'text'
      ? report.text_report
      : report.executive_summary
    navigator.clipboard.writeText(content).then(() => {
      setCopied(true)
      setTimeout(() => setCopied(false), 2000)
    })
  }

  const downloadReport = () => {
    const isJson = format === 'json'
    const content = isJson
      ? JSON.stringify(report.json_report, null, 2)
      : format === 'text'
      ? report.text_report
      : report.executive_summary
    const blob = new Blob([content], { type: isJson ? 'application/json' : 'text/plain' })
    const url = URL.createObjectURL(blob)
    const a = document.createElement('a')
    a.href = url
    a.download = `report_${datasetId}_${format}.${isJson ? 'json' : 'txt'}`
    a.click()
    URL.revokeObjectURL(url)
  }

  if (loading) return <PageSpinner text="Generating comprehensive report…" />
  if (error) return <ErrorAlert message={error} onRetry={load} />

  const jr = report.json_report
  const overall = jr.quality_score.overall
  const grade = jr.quality_score.grade
  const color = scoreColor(overall)

  // ── Format switcher bar ──
  const tabs = [
    { id: 'visual', label: 'Visual Report', icon: <FileText size={14} /> },
    { id: 'text',   label: 'Text Report',   icon: <FileText size={14} /> },
    { id: 'json',   label: 'JSON',          icon: <Code2 size={14} /> },
  ]

  return (
    <div className="space-y-5 animate-fade-in max-w-5xl">

      {/* Toolbar */}
      <div className="flex items-center justify-between flex-wrap gap-3">
        <div className="flex items-center gap-1 bg-slate-100 rounded-lg p-1">
          {tabs.map(t => (
            <button
              key={t.id}
              onClick={() => setFormat(t.id)}
              className={`flex items-center gap-1.5 px-3 py-1.5 rounded-md text-sm font-medium transition-all ${
                format === t.id
                  ? 'bg-white shadow-sm text-slate-800'
                  : 'text-slate-500 hover:text-slate-700'
              }`}
            >
              {t.icon}{t.label}
            </button>
          ))}
        </div>
        <div className="flex items-center gap-2">
          <button onClick={load} className="btn-secondary gap-1.5">
            <RefreshCw size={13} /> Regenerate
          </button>
          <button onClick={copyContent} className="btn-secondary gap-1.5">
            {copied ? <CheckCircle size={13} className="text-emerald-500" /> : <Copy size={13} />}
            {copied ? 'Copied!' : 'Copy'}
          </button>
          <button onClick={downloadReport} className="btn-primary gap-1.5">
            <Download size={13} /> Download
          </button>
        </div>
      </div>

      {/* ── VISUAL REPORT ── */}
      {format === 'visual' && (
        <div className="space-y-5">

          {/* Executive Summary Banner */}
          <div className={`rounded-xl border-2 p-5 ${overall >= 75 ? 'border-emerald-300 bg-emerald-50' : overall >= 60 ? 'border-blue-300 bg-blue-50' : overall >= 40 ? 'border-orange-300 bg-orange-50' : 'border-red-300 bg-red-50'}`}>
            <div className="flex items-start gap-4">
              <div className="text-center bg-white rounded-xl px-4 py-3 shadow-sm border border-slate-200 min-w-[80px]">
                <div className="text-3xl font-black" style={{ color }}>{overall.toFixed(0)}</div>
                <div className="text-xs text-slate-400 font-medium">/ 100</div>
                <div className="text-lg font-bold mt-0.5" style={{ color }}>{grade}</div>
              </div>
              <div className="flex-1">
                <div className="flex items-center gap-2 mb-2">
                  <Users size={15} className="text-slate-500" />
                  <span className="text-xs font-semibold text-slate-500 uppercase tracking-wide">Executive Summary</span>
                </div>
                <pre className="text-sm text-slate-700 whitespace-pre-wrap font-sans leading-relaxed">
                  {report.executive_summary}
                </pre>
              </div>
            </div>
          </div>

          {/* Key Issues + Dataset Summary */}
          <div className="grid grid-cols-1 md:grid-cols-3 gap-4">

            {/* Dataset Summary */}
            <div className="card p-5 space-y-2">
              <h3 className="text-xs font-semibold text-slate-500 uppercase tracking-wide mb-3">Dataset</h3>
              {[
                { label: 'Name', value: jr.dataset_summary.name },
                { label: 'Type', value: jr.dataset_summary.file_type?.toUpperCase() },
                { label: 'Rows', value: jr.dataset_summary.rows?.toLocaleString() },
                { label: 'Columns', value: jr.dataset_summary.columns },
              ].map(({ label, value }) => (
                <div key={label} className="flex justify-between items-center text-sm">
                  <span className="text-slate-400">{label}</span>
                  <span className="font-medium text-slate-700">{value}</span>
                </div>
              ))}
              <div className="pt-2 text-xs text-slate-400">
                Generated {new Date(report.generated_at).toLocaleString('en-IN', { timeZone: 'Asia/Kolkata' })} IST
              </div>
            </div>

            {/* Key Issues */}
            <div className="card p-5 md:col-span-2">
              <h3 className="text-xs font-semibold text-slate-500 uppercase tracking-wide mb-3">Key Issues</h3>
              <ul className="space-y-2">
                {jr.key_issues.map((issue, i) => (
                  <li key={i} className="flex items-start gap-2 text-sm text-slate-700">
                    <span className="mt-0.5 w-5 h-5 flex-shrink-0 rounded-full bg-red-100 text-red-600 text-xs font-bold flex items-center justify-center">{i + 1}</span>
                    {issue}
                  </li>
                ))}
              </ul>
            </div>
          </div>

          {/* Quality Dimensions */}
          <div className="card p-5">
            <h3 className="text-xs font-semibold text-slate-500 uppercase tracking-wide mb-4">Quality Dimensions</h3>
            <div className="space-y-3">
              {Object.entries(jr.quality_score.dimensions).map(([key, score]) => (
                <ScoreBar key={key} label={key} score={score} color={DIM_COLORS[key] || '#3b82f6'} />
              ))}
            </div>
          </div>

          {/* Anomalies */}
          {jr.anomalies.length > 0 && (
            <div className="card overflow-hidden">
              <button
                className="card-header w-full flex items-center justify-between cursor-pointer hover:bg-slate-50"
                onClick={() => toggleSection('anomalies')}
              >
                <span className="font-semibold text-slate-800 text-sm flex items-center gap-2">
                  <AlertTriangle size={14} className="text-orange-500" />
                  Anomalies ({jr.anomalies.length} columns)
                </span>
                {expandedSections.anomalies ? <ChevronUp size={14} /> : <ChevronDown size={14} />}
              </button>
              {expandedSections.anomalies && (
                <table className="table-base">
                  <thead><tr><th>Column</th><th>Detection Methods</th><th>Affected Rows</th><th>Severity</th></tr></thead>
                  <tbody>
                    {jr.anomalies.map(a => (
                      <tr key={a.column}>
                        <td className="font-medium text-slate-800">{a.column}</td>
                        <td className="text-slate-500 text-xs">{a.type}</td>
                        <td className="font-mono text-sm">{a.affected_rows}</td>
                        <td><span className={`text-xs font-semibold px-2 py-0.5 rounded-full border ${SEVERITY_STYLES[a.severity] || ''}`}>{a.severity}</span></td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              )}
            </div>
          )}

          {/* Drift */}
          {jr.drift.length > 0 && (
            <div className="card overflow-hidden">
              <button
                className="card-header w-full flex items-center justify-between cursor-pointer hover:bg-slate-50"
                onClick={() => toggleSection('drift')}
              >
                <span className="font-semibold text-slate-800 text-sm flex items-center gap-2">
                  <TrendingDown size={14} className="text-red-500" />
                  Drift ({jr.drift.filter(d => d.drift_detected).length} drifted)
                </span>
                {expandedSections.drift ? <ChevronUp size={14} /> : <ChevronDown size={14} />}
              </button>
              {expandedSections.drift && (
                <table className="table-base">
                  <thead><tr><th>Column</th><th>Status</th><th>p-value</th><th>Severity</th></tr></thead>
                  <tbody>
                    {jr.drift.map(d => (
                      <tr key={d.column}>
                        <td className="font-medium text-slate-800">{d.column}</td>
                        <td>
                          {d.drift_detected
                            ? <span className="text-xs font-semibold bg-red-100 text-red-700 px-2 py-0.5 rounded-full">Drifted</span>
                            : <span className="text-xs font-semibold bg-emerald-100 text-emerald-700 px-2 py-0.5 rounded-full">Stable</span>}
                        </td>
                        <td className="font-mono text-sm">{d.p_value != null ? d.p_value.toFixed(4) : '—'}</td>
                        <td><span className={`text-xs font-semibold px-2 py-0.5 rounded-full border ${SEVERITY_STYLES[d.severity] || ''}`}>{d.severity}</span></td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              )}
            </div>
          )}

          {/* Security + PII side-by-side */}
          <div className="grid grid-cols-1 md:grid-cols-2 gap-4">

            {/* Security */}
            <div className="card overflow-hidden">
              <button
                className="card-header w-full flex items-center justify-between cursor-pointer hover:bg-slate-50"
                onClick={() => toggleSection('sec')}
              >
                <span className="font-semibold text-slate-800 text-sm flex items-center gap-2">
                  <ShieldAlert size={14} className="text-violet-500" />
                  Security
                </span>
                {expandedSections.sec ? <ChevronUp size={14} /> : <ChevronDown size={14} />}
              </button>
              {expandedSections.sec && (
                <div className="card-body space-y-2">
                  {jr.security_issues.map((s, i) => (
                    <div key={i} className="flex items-start gap-2 text-sm">
                      {s.includes('No security') ? <CheckCircle size={13} className="text-emerald-500 mt-0.5 flex-shrink-0" /> : <AlertTriangle size={13} className="text-red-500 mt-0.5 flex-shrink-0" />}
                      <span className="text-slate-700">{s}</span>
                    </div>
                  ))}
                </div>
              )}
            </div>

            {/* PII */}
            <div className="card overflow-hidden">
              <button
                className="card-header w-full flex items-center justify-between cursor-pointer hover:bg-slate-50"
                onClick={() => toggleSection('pii')}
              >
                <span className="font-semibold text-slate-800 text-sm flex items-center gap-2">
                  🔒 PII Risks ({jr.pii_risks.length})
                </span>
                {expandedSections.pii ? <ChevronUp size={14} /> : <ChevronDown size={14} />}
              </button>
              {expandedSections.pii && (
                <div className="card-body">
                  {jr.pii_risks.length === 0 ? (
                    <div className="flex items-center gap-2 text-sm text-emerald-600">
                      <CheckCircle size={13} /> No PII detected
                    </div>
                  ) : (
                    <div className="space-y-2">
                      {jr.pii_risks.map((p, i) => (
                        <div key={i} className="flex items-center justify-between text-sm">
                          <span className="font-medium text-slate-700">{p.column}</span>
                          <div className="flex items-center gap-2">
                            <span className="text-xs bg-violet-100 text-violet-700 px-2 py-0.5 rounded-full">{p.pii_type}</span>
                            <span className={`text-xs font-semibold px-2 py-0.5 rounded-full border ${SEVERITY_STYLES[p.risk_level] || ''}`}>{p.risk_level}</span>
                          </div>
                        </div>
                      ))}
                    </div>
                  )}
                </div>
              )}
            </div>
          </div>

          {/* Recommendations */}
          <div className="card p-5">
            <h3 className="text-xs font-semibold text-slate-500 uppercase tracking-wide mb-4 flex items-center gap-2">
              ✅ Recommendations
            </h3>
            <ol className="space-y-3">
              {jr.recommendations.map((rec, i) => (
                <li key={i} className="flex items-start gap-3">
                  <span className="w-6 h-6 rounded-full bg-blue-100 text-blue-700 text-xs font-bold flex items-center justify-center flex-shrink-0 mt-0.5">{i + 1}</span>
                  <span className="text-sm text-slate-700 leading-relaxed">{rec}</span>
                </li>
              ))}
            </ol>
          </div>

        </div>
      )}

      {/* ── TEXT REPORT ── */}
      {format === 'text' && (
        <div className="card">
          <pre className="font-mono text-[13px] text-slate-700 p-6 bg-slate-950 text-slate-100 rounded-xl overflow-x-auto leading-relaxed whitespace-pre">
            {report.text_report}
          </pre>
        </div>
      )}

      {/* ── JSON REPORT ── */}
      {format === 'json' && (
        <div className="card">
          <pre className="font-mono text-[12px] p-6 bg-slate-950 text-emerald-300 rounded-xl overflow-x-auto leading-relaxed">
            {JSON.stringify(report.json_report, null, 2)}
          </pre>
        </div>
      )}
    </div>
  )
}
