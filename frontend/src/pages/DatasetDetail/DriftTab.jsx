import { useState } from 'react'
import { ArrowLeftRight, GitCompare, TrendingDown, TrendingUp, Minus, Plus, Trash, Columns, Rows, AlertCircle } from 'lucide-react'
import { datasetsAPI } from '../../services/api'
import EmptyState from '../../components/EmptyState'

function versionLabel(v) {
  const date = v.created_at
    ? new Date(v.created_at).toLocaleDateString('en-IN', {
        day: '2-digit', month: 'short', year: 'numeric', timeZone: 'Asia/Kolkata',
      })
    : null
  return date ? `v${v.version_number} — ${date}` : `v${v.version_number}`
}

function DeltaBadge({ value, suffix = '' }) {
  if (value == null || value === 0) return <span className="text-slate-400 text-xs">no change</span>
  const color = value > 0 ? 'text-amber-600' : 'text-blue-600'
  const icon = value > 0
    ? <TrendingUp size={12} className="text-amber-500" />
    : <TrendingDown size={12} className="text-blue-500" />
  return (
    <span className={`inline-flex items-center gap-1 text-xs font-mono ${color}`}>
      {icon} {value > 0 ? '+' : ''}{typeof value === 'number' && !Number.isInteger(value) ? value.toFixed(4) : value}{suffix}
    </span>
  )
}

export default function DriftTab({ datasetId, dataset, refreshKey }) {
  const versions = dataset?.versions ?? []
  const defaultA = versions[0]?.version_number ?? 1
  const defaultB = versions[versions.length - 1]?.version_number ?? 1

  const [vA,      setVA]      = useState(defaultA)
  const [vB,      setVB]      = useState(defaultB)
  const [result,  setResult]  = useState(null)
  const [loading, setLoading] = useState(false)
  const [error,   setError]   = useState(null)

  const swap = () => { setVA(vB); setVB(vA); setResult(null); setError(null) }

  const compare = async () => {
    if (vA === vB) { setError('Version A and B must differ.'); return }
    setLoading(true); setError(null); setResult(null)
    try   { setResult(await datasetsAPI.getChangeSummary(datasetId, vA, vB)) }
    catch (e) { setError(e.message) }
    finally   { setLoading(false) }
  }

  if (versions.length < 2) {
    return (
      <EmptyState
        title="No versions to compare"
        description="Upload or clean a dataset to create another version, then compare changes."
      />
    )
  }

  const cols = result?.columns ?? {}
  const rows = result?.rows ?? {}
  const missingChanges = result?.missing_changes ?? {}
  const distShift = result?.distribution_shift ?? {}
  const catChanges = result?.categorical_changes ?? []

  const totalChanges = (cols.added?.length ?? 0) + (cols.removed?.length ?? 0) + (cols.type_changes?.length ?? 0) +
    Object.keys(missingChanges).length + Object.keys(distShift).length + catChanges.length + (rows.change !== 0 ? 1 : 0)

  return (
    <div className="space-y-6 animate-fade-in max-w-5xl">

      {/* Version selector */}
      <div className="card p-5">
        <h2 className="font-semibold text-slate-800 text-sm mb-4 flex items-center gap-2">
          <GitCompare size={15} />
          Compare Versions
        </h2>
        <div className="flex items-center gap-3 flex-wrap">
          <div className="flex flex-col gap-1">
            <label className="text-xs font-medium text-slate-500 uppercase tracking-wide">Baseline (A)</label>
            <select className="select-base w-52" value={vA}
              onChange={e => { setVA(Number(e.target.value)); setResult(null); setError(null) }}>
              {versions.map(v => (
                <option key={v.version_number} value={v.version_number} disabled={v.version_number === vB}>
                  {versionLabel(v)}
                </option>
              ))}
            </select>
          </div>

          <button onClick={swap}
            className="mt-5 p-2 rounded-lg border border-slate-200 text-slate-400 hover:text-indigo-600 hover:border-indigo-300 hover:bg-indigo-50 transition-colors"
            title="Swap versions">
            <ArrowLeftRight size={16} />
          </button>

          <div className="flex flex-col gap-1">
            <label className="text-xs font-medium text-slate-500 uppercase tracking-wide">Current (B)</label>
            <select className="select-base w-52" value={vB}
              onChange={e => { setVB(Number(e.target.value)); setResult(null); setError(null) }}>
              {versions.map(v => (
                <option key={v.version_number} value={v.version_number} disabled={v.version_number === vA}>
                  {versionLabel(v)}
                </option>
              ))}
            </select>
          </div>

          <button onClick={compare} disabled={loading || vA === vB}
            className="mt-5 btn-primary disabled:opacity-50 disabled:cursor-not-allowed">
            {loading ? (
              <span className="flex items-center gap-2">
                <span className="w-4 h-4 border-2 border-white/30 border-t-white rounded-full animate-spin" />
                Comparing…
              </span>
            ) : (
              <span className="flex items-center gap-2"><GitCompare size={14} /> Compare</span>
            )}
          </button>
        </div>

        {vA === vB && (
          <p className="mt-3 text-xs text-amber-600 bg-amber-50 border border-amber-200 rounded px-3 py-1.5">
            Select two different versions to compare.
          </p>
        )}
        {error && <p className="mt-3 text-sm text-red-600">{error}</p>}
      </div>

      {result && (
        <>
          {/* Summary cards */}
          <div className="grid grid-cols-2 sm:grid-cols-4 gap-4">
            <div className="card p-4">
              <p className="text-xs text-slate-400 font-medium">Rows</p>
              <p className="text-xl font-bold text-slate-800 mt-1">{rows.before ?? '?'} → {rows.after ?? '?'}</p>
              <DeltaBadge value={rows.change} suffix={rows.percent_change != null ? ` (${rows.percent_change}%)` : ''} />
            </div>
            <div className="card p-4">
              <p className="text-xs text-slate-400 font-medium">Cols Added</p>
              <p className="text-xl font-bold text-emerald-600 mt-1">{cols.added?.length ?? 0}</p>
            </div>
            <div className="card p-4">
              <p className="text-xs text-slate-400 font-medium">Cols Removed</p>
              <p className="text-xl font-bold text-red-600 mt-1">{cols.removed?.length ?? 0}</p>
            </div>
            <div className="card p-4">
              <p className="text-xs text-slate-400 font-medium">Total Changes</p>
              <p className={`text-xl font-bold mt-1 ${totalChanges > 0 ? 'text-amber-600' : 'text-emerald-600'}`}>{totalChanges}</p>
            </div>
          </div>

          {/* Column changes */}
          {(cols.added?.length > 0 || cols.removed?.length > 0 || cols.type_changes?.length > 0) && (
            <div className="card overflow-hidden">
              <div className="card-header">
                <h2 className="font-semibold text-slate-800 text-sm flex items-center gap-2">
                  <Columns size={15} /> Column Changes
                </h2>
              </div>
              <div className="card-body space-y-3">
                {cols.added?.length > 0 && (
                  <div>
                    <span className="text-xs font-medium text-emerald-600 uppercase">Added</span>
                    <div className="flex flex-wrap gap-1.5 mt-1">
                      {cols.added.map(c => (
                        <span key={c} className="text-xs bg-emerald-50 text-emerald-700 border border-emerald-200 px-2 py-0.5 rounded font-mono">
                          + {c}
                        </span>
                      ))}
                    </div>
                  </div>
                )}
                {cols.removed?.length > 0 && (
                  <div>
                    <span className="text-xs font-medium text-red-600 uppercase">Removed</span>
                    <div className="flex flex-wrap gap-1.5 mt-1">
                      {cols.removed.map(c => (
                        <span key={c} className="text-xs bg-red-50 text-red-700 border border-red-200 px-2 py-0.5 rounded font-mono">
                          − {c}
                        </span>
                      ))}
                    </div>
                  </div>
                )}
                {cols.type_changes?.length > 0 && (
                  <div>
                    <span className="text-xs font-medium text-amber-600 uppercase">Type Changes</span>
                    <div className="space-y-1 mt-1">
                      {cols.type_changes.map(tc => (
                        <div key={tc.column} className="text-xs text-slate-600">
                          <span className="font-mono font-semibold">{tc.column}</span>: {tc.before} → {tc.after}
                        </div>
                      ))}
                    </div>
                  </div>
                )}
              </div>
            </div>
          )}

          {/* Missing value changes */}
          {Object.keys(missingChanges).length > 0 && (
            <div className="card overflow-hidden">
              <div className="card-header">
                <h2 className="font-semibold text-slate-800 text-sm flex items-center gap-2">
                  <AlertCircle size={15} className="text-amber-500" /> Missing Value Changes
                </h2>
              </div>
              <table className="table-base">
                <thead>
                  <tr>
                    <th>Column</th>
                    <th>Before</th>
                    <th>After</th>
                    <th>Delta</th>
                  </tr>
                </thead>
                <tbody>
                  {Object.entries(missingChanges).map(([col, m]) => (
                    <tr key={col} className={m.delta < 0 ? 'bg-emerald-50/30' : m.delta > 0 ? 'bg-red-50/30' : ''}>
                      <td className="font-semibold text-slate-800 font-mono">{col}</td>
                      <td className="text-slate-600">{m.before}</td>
                      <td className="text-slate-600">{m.after}</td>
                      <td><DeltaBadge value={m.delta} /></td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          )}

          {/* Distribution shift */}
          {Object.keys(distShift).length > 0 && (
            <div className="card overflow-hidden">
              <div className="card-header">
                <h2 className="font-semibold text-slate-800 text-sm flex items-center gap-2">
                  <TrendingUp size={15} className="text-indigo-500" /> Numeric Distribution Changes
                </h2>
              </div>
              <table className="table-base">
                <thead>
                  <tr>
                    <th>Column</th>
                    <th>Mean (A)</th>
                    <th>Mean (B)</th>
                    <th>Mean Δ</th>
                    <th>Std (A)</th>
                    <th>Std (B)</th>
                    <th>Range (A)</th>
                    <th>Range (B)</th>
                  </tr>
                </thead>
                <tbody>
                  {Object.entries(distShift).map(([col, d]) => (
                    <tr key={col}>
                      <td className="font-semibold text-slate-800 font-mono">{col}</td>
                      <td className="font-mono text-sm text-slate-600">{d.mean_before}</td>
                      <td className="font-mono text-sm text-slate-600">{d.mean_after}</td>
                      <td><DeltaBadge value={d.mean_delta} /></td>
                      <td className="font-mono text-sm text-slate-500">{d.std_before}</td>
                      <td className="font-mono text-sm text-slate-500">{d.std_after}</td>
                      <td className="text-xs text-slate-400">{d.min_before} – {d.max_before}</td>
                      <td className="text-xs text-slate-400">{d.min_after} – {d.max_after}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          )}

          {/* Categorical changes */}
          {catChanges.length > 0 && (
            <div className="card overflow-hidden">
              <div className="card-header">
                <h2 className="font-semibold text-slate-800 text-sm flex items-center gap-2">
                  📋 Categorical Value Changes
                </h2>
              </div>
              <div className="card-body space-y-4">
                {catChanges.map(cc => (
                  <div key={cc.column} className="border border-slate-100 rounded-lg p-3">
                    <span className="font-mono font-semibold text-slate-800 text-sm">{cc.column}</span>
                    <div className="flex flex-wrap gap-3 mt-2">
                      {cc.added_values?.length > 0 && (
                        <div>
                          <span className="text-[10px] text-emerald-600 font-semibold uppercase">New values</span>
                          <div className="flex flex-wrap gap-1 mt-0.5">
                            {cc.added_values.map(v => (
                              <span key={v} className="text-xs bg-emerald-50 text-emerald-700 border border-emerald-200 px-1.5 py-0.5 rounded">
                                {v}
                              </span>
                            ))}
                          </div>
                        </div>
                      )}
                      {cc.removed_values?.length > 0 && (
                        <div>
                          <span className="text-[10px] text-red-600 font-semibold uppercase">Removed values</span>
                          <div className="flex flex-wrap gap-1 mt-0.5">
                            {cc.removed_values.map(v => (
                              <span key={v} className="text-xs bg-red-50 text-red-700 border border-red-200 px-1.5 py-0.5 rounded">
                                {v}
                              </span>
                            ))}
                          </div>
                        </div>
                      )}
                    </div>
                  </div>
                ))}
              </div>
            </div>
          )}

          {/* No changes state */}
          {totalChanges === 0 && (
            <div className="card p-8 text-center">
              <p className="text-emerald-600 font-semibold">✅ No changes detected between these versions.</p>
            </div>
          )}
        </>
      )}
    </div>
  )
}
