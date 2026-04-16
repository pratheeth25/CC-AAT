import { useEffect, useState } from 'react'
import { Search } from 'lucide-react'
import { datasetsAPI } from '../../services/api'
import { PageSpinner } from '../../components/LoadingSpinner'
import ErrorAlert from '../../components/ErrorAlert'

function MissingBar({ pct }) {
  const color = pct >= 50 ? '#ef4444' : pct >= 20 ? '#f97316' : pct > 0 ? '#eab308' : '#22c55e'
  return (
    <div className="flex items-center gap-2">
      <div className="h-1.5 w-24 bg-slate-100 rounded-full overflow-hidden flex-shrink-0">
        <div className="h-full rounded-full" style={{ width: `${pct}%`, backgroundColor: color }} />
      </div>
      <span className="text-xs font-medium" style={{ color }}>{pct}%</span>
    </div>
  )
}

export default function ProfileTab({ datasetId, dataset }) {
  const [profile, setProfile] = useState(null)
  const [loading, setLoading] = useState(true)
  const [error,   setError]   = useState(null)
  const [search,  setSearch]  = useState('')
  const [expand,  setExpand]  = useState(null)

  const load = async () => {
    setLoading(true); setError(null)
    try   { setProfile(await datasetsAPI.getProfile(datasetId)) }
    catch (e) { setError(e.message) }
    finally   { setLoading(false) }
  }

  useEffect(() => { load() }, [datasetId])

  if (loading) return <PageSpinner text="Profiling dataset…" />
  if (error)   return <ErrorAlert message={error} onRetry={load} />

  const columns = Object.entries(profile.columns).filter(([name]) =>
    name.toLowerCase().includes(search.toLowerCase())
  )

  return (
    <div className="space-y-5 animate-fade-in max-w-5xl">

      {/* Summary cards */}
      <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
        {[
          { label: 'Total Columns',    value: profile.shape.columns              },
          { label: 'Total Rows',       value: profile.shape.rows?.toLocaleString() },
          { label: 'Duplicate Rows',   value: profile.duplicates.exact_duplicates },
          { label: 'Total Missing',    value: Object.values(profile.missing_values).reduce((s,v)=>s+v.count,0).toLocaleString() },
        ].map(({ label, value }) => (
          <div key={label} className="card p-4">
            <p className="text-xs text-slate-400 font-medium">{label}</p>
            <p className="text-2xl font-bold text-slate-800 mt-1">{value}</p>
          </div>
        ))}
      </div>

      {/* Duplicate detail */}
      <div className="card p-4 flex items-center gap-8 text-sm">
        <div>
          <span className="text-slate-400">Exact duplicates: </span>
          <span className="font-semibold text-slate-800">
            {profile.duplicates.exact_duplicates}
            <span className="text-slate-400 font-normal ml-1">
              ({profile.duplicates.exact_duplicate_percentage}%)
            </span>
          </span>
        </div>
        <div>
          <span className="text-slate-400">Logical duplicates: </span>
          <span className="font-semibold text-slate-800">
            {profile.duplicates.logical_duplicates}
            <span className="text-slate-400 font-normal ml-1">
              ({profile.duplicates.logical_duplicate_percentage}%)
            </span>
          </span>
        </div>
      </div>

      {/* Column search */}
      <div className="relative max-w-xs">
        <Search size={14} className="absolute left-3 top-1/2 -translate-y-1/2 text-slate-400" />
        <input className="input-base pl-8" placeholder="Search columns…" value={search}
          onChange={e => setSearch(e.target.value)} />
      </div>

      {/* Column table */}
      <div className="card overflow-hidden">
        <table className="table-base">
          <thead>
            <tr>
              <th>Column</th>
              <th>Type</th>
              <th>Unique</th>
              <th>Missing</th>
              <th>Sample Values</th>
              <th />
            </tr>
          </thead>
          <tbody>
            {columns.map(([name, stats]) => (
              <>
                <tr
                  key={name}
                  className={`cursor-pointer ${expand === name ? 'bg-blue-50/40' : ''}`}
                  onClick={() => setExpand(expand === name ? null : name)}
                >
                  <td className="font-medium text-slate-800">{name}</td>
                  <td>
                    <span className="text-xs font-mono bg-slate-100 text-slate-600 px-1.5 py-0.5 rounded">
                      {stats.dtype}
                    </span>
                  </td>
                  <td className="text-slate-600">{stats.unique_count?.toLocaleString()}</td>
                  <td><MissingBar pct={stats.missing_percentage} /></td>
                  <td className="max-w-[200px]">
                    <div className="flex flex-wrap gap-1">
                      {(stats.sample_values || []).slice(0,3).map((v, i) => (
                        <span key={i} className="text-[11px] font-mono bg-slate-100 text-slate-500 px-1.5 py-0.5 rounded truncate max-w-[80px]">
                          {String(v)}
                        </span>
                      ))}
                    </div>
                  </td>
                  <td className="text-slate-400 text-xs">{expand === name ? '▲' : '▼'}</td>
                </tr>

                {/* Expanded numeric stats */}
                {expand === name && (
                  <tr key={`${name}-exp`} className="bg-blue-50/30">
                    <td colSpan={6} className="px-4 py-3">
                      <div className="grid grid-cols-3 md:grid-cols-6 gap-3 text-xs">
                        {['min','max','mean','median','std','skewness','kurtosis','q1','q3'].map(k => (
                          stats[k] != null && (
                            <div key={k} className="bg-white rounded-lg p-2 border border-blue-100">
                              <p className="text-slate-400 capitalize">{k}</p>
                              <p className="font-semibold text-slate-700 mt-0.5">
                                {typeof stats[k] === 'number' ? stats[k].toFixed(3) : stats[k]}
                              </p>
                            </div>
                          )
                        ))}
                        {stats.top_values && Object.entries(stats.top_values).slice(0,5).map(([v, cnt]) => (
                          <div key={v} className="bg-white rounded-lg p-2 border border-blue-100">
                            <p className="text-slate-400 truncate">{`"${v}"`}</p>
                            <p className="font-semibold text-slate-700 mt-0.5">{cnt}×</p>
                          </div>
                        ))}
                      </div>
                    </td>
                  </tr>
                )}
              </>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  )
}
