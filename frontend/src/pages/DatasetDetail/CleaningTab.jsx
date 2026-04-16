import { useEffect, useState } from 'react'
import { Wrench, CheckCircle2, ChevronDown, ChevronUp } from 'lucide-react'
import toast from 'react-hot-toast'
import { datasetsAPI } from '../../services/api'
import { PageSpinner } from '../../components/LoadingSpinner'
import ErrorAlert from '../../components/ErrorAlert'
import EmptyState from '../../components/EmptyState'

const ISSUE_COLORS = {
  missing_numeric:     'bg-blue-50   text-blue-700   border-blue-200',
  missing_categorical: 'bg-indigo-50 text-indigo-700 border-indigo-200',
  duplicate_rows:      'bg-purple-50 text-purple-700 border-purple-200',
  date_format:         'bg-amber-50  text-amber-700  border-amber-200',
  country_synonym:     'bg-cyan-50   text-cyan-700   border-cyan-200',
  casing_inconsistency:'bg-teal-50   text-teal-700   border-teal-200',
  email_format:        'bg-orange-50 text-orange-700 border-orange-200',
  default:             'bg-slate-50  text-slate-700  border-slate-200',
}
const issueColor = (type) => ISSUE_COLORS[type] ?? ISSUE_COLORS.default

const DEFAULT_FORM = {
  fix_missing_numeric:    true,
  fix_missing_categorical:true,
  fix_duplicates:         true,
  standardize_dates:      true,
  normalize_case:         '',
  remove_outliers:        false,
}

export default function CleaningTab({ datasetId, dataset, onCleanSuccess }) {
  const [suggestions, setSuggestions] = useState(null)
  const [loadingRep,  setLoadingRep]  = useState(true)
  const [errorRep,    setErrorRep]    = useState(null)
  const [form,        setForm]        = useState(DEFAULT_FORM)
  const [cleaning,    setCleaning]    = useState(false)
  const [result,      setResult]      = useState(null)
  const [showSuggest, setShowSuggest] = useState(true)

  const loadSuggestions = async () => {
    setLoadingRep(true); setErrorRep(null)
    try   { setSuggestions(await datasetsAPI.getRepairs(datasetId)) }
    catch (e) { setErrorRep(e.message) }
    finally   { setLoadingRep(false) }
  }

  useEffect(() => { loadSuggestions() }, [datasetId])

  const toggle = (key) => setForm(f => ({ ...f, [key]: !f[key] }))

  const handleClean = async () => {
    setCleaning(true); setResult(null)
    try {
      const payload = {
        fix_missing_numeric:     form.fix_missing_numeric,
        fix_missing_categorical: form.fix_missing_categorical,
        fix_duplicates:          form.fix_duplicates,
        standardize_dates:       form.standardize_dates,
        normalize_case:          form.normalize_case || null,
        remove_outliers:         form.remove_outliers,
      }
      const res = await datasetsAPI.clean(datasetId, payload)
      setResult(res)
      toast.success(`Version v${res.new_version} created!`)
      onCleanSuccess?.()
    } catch (e) {
      toast.error(e.message)
    } finally {
      setCleaning(false)
    }
  }

  return (
    <div className="space-y-6 animate-fade-in max-w-2xl">

      {/* ── Suggestions ── */}
      <div className="card">
        <button
          onClick={() => setShowSuggest(s => !s)}
          className="card-header w-full text-left flex items-center justify-between"
        >
          <div className="flex items-center gap-2">
            <h2 className="font-semibold text-slate-800 text-sm flex items-center gap-2">
              <Wrench size={14} className="text-slate-500" />
              Repair Suggestions
            </h2>
            {!loadingRep && (
              <span className="text-xs font-semibold bg-amber-100 text-amber-700 px-2 py-0.5 rounded-full">
                {suggestions?.suggestions?.length ?? 0} issues
              </span>
            )}
          </div>
          {showSuggest ? <ChevronUp size={14} /> : <ChevronDown size={14} />}
        </button>

        {showSuggest && (
          loadingRep ? (
            <div className="p-6"><PageSpinner text="Loading suggestions…" /></div>
          ) : errorRep ? (
            <div className="p-4"><ErrorAlert message={errorRep} onRetry={loadSuggestions} /></div>
          ) : !suggestions?.suggestions?.length ? (
            <div className="p-6"><EmptyState title="No repair suggestions" description="Dataset looks great!" /></div>
          ) : (
            <div className="divide-y divide-slate-100">
              {suggestions.suggestions.map((s, i) => (
                <div key={i} className="px-6 py-3 flex items-start gap-3">
                  <span className={`text-[11px] font-semibold border px-2 py-0.5 rounded-full flex-shrink-0 mt-0.5 ${issueColor(s.issue_type)}`}>
                    {s.issue_type.replace(/_/g,' ')}
                  </span>
                  <div className="flex-1 min-w-0">
                    <p className="text-sm text-slate-700">
                      <span className="font-medium text-slate-900">[{s.column}]</span>{' '}
                      {s.suggestion}
                    </p>
                    {s.details && typeof s.details === 'object' && (
                      <p className="text-xs text-slate-400 mt-0.5">
                        {Object.entries(s.details).map(([k, v]) => `${k.replace(/_/g,' ')}: ${v}`).join(' · ')}
                      </p>
                    )}
                  </div>
                </div>
              ))}
            </div>
          )
        )}
      </div>

      {/* ── Cleaning form ── */}
      {!result && (
        <div className="card">
          <div className="card-header">
            <h2 className="font-semibold text-slate-800 text-sm">Apply Cleaning Pipeline</h2>
            <span className="text-xs text-slate-400">Creates a new version</span>
          </div>
          <div className="card-body space-y-3">
            {[
              { key: 'fix_missing_numeric',     label: 'Fix missing numeric values',     desc: 'Fill with column mean' },
              { key: 'fix_missing_categorical', label: 'Fix missing categorical values',  desc: 'Fill with most common value' },
              { key: 'fix_duplicates',          label: 'Remove exact duplicate rows',     desc: 'Drop all exact duplicates' },
              { key: 'standardize_dates',       label: 'Standardize dates to ISO 8601',   desc: 'YYYY-MM-DD format' },
            ].map(({ key, label, desc }) => (
              <label key={key} className="flex items-center gap-3 cursor-pointer group">
                <div
                  onClick={() => toggle(key)}
                  className={`w-5 h-5 rounded border-2 flex items-center justify-center transition-colors flex-shrink-0 cursor-pointer ${
                    form[key] ? 'bg-blue-600 border-blue-600' : 'border-slate-300 group-hover:border-slate-400'
                  }`}
                >
                  {form[key] && <CheckCircle2 size={12} className="text-white" />}
                </div>
                <div>
                  <p className="text-sm font-medium text-slate-700">{label}</p>
                  <p className="text-xs text-slate-400">{desc}</p>
                </div>
              </label>
            ))}

            {/* Normalize case */}
            <div className="flex items-center gap-3 pt-1">
              <div className="w-5 flex-shrink-0" />
              <div className="flex items-center gap-3">
                <label className="text-sm font-medium text-slate-700">Normalize text casing:</label>
                <select
                  className="select-base w-36"
                  value={form.normalize_case}
                  onChange={e => setForm(f => ({ ...f, normalize_case: e.target.value }))}
                >
                  <option value="">None</option>
                  <option value="lower">Lowercase</option>
                  <option value="upper">UPPERCASE</option>
                  <option value="title">Title Case</option>
                </select>
              </div>
            </div>

            {/* Remove outliers */}
            <label className="flex items-center gap-3 cursor-pointer group">
              <div
                onClick={() => toggle('remove_outliers')}
                className={`w-5 h-5 rounded border-2 flex items-center justify-center transition-colors flex-shrink-0 cursor-pointer ${
                  form.remove_outliers ? 'bg-red-500 border-red-500' : 'border-slate-300 group-hover:border-slate-400'
                }`}
              >
                {form.remove_outliers && <CheckCircle2 size={12} className="text-white" />}
              </div>
              <div>
                <p className="text-sm font-medium text-slate-700">Remove outliers (IQR method)</p>
                <p className="text-xs text-orange-500 font-medium">⚠ Removes rows — use with caution</p>
              </div>
            </label>

            <div className="pt-2">
              <button
                onClick={handleClean}
                disabled={cleaning}
                className="btn-primary w-full justify-center py-2.5"
              >
                {cleaning ? (
                  <span className="flex items-center gap-2">
                    <span className="w-4 h-4 border-2 border-white/30 border-t-white rounded-full animate-spin" />
                    Applying cleaning pipeline…
                  </span>
                ) : (
                  '✓ Apply Cleaning Pipeline'
                )}
              </button>
            </div>
          </div>
        </div>
      )}

      {/* ── Success result ── */}
      {result && (
        <div className="card border-2 border-emerald-300 bg-emerald-50">
          <div className="card-body space-y-4">
            <div className="flex items-center gap-3">
              <div className="w-10 h-10 rounded-full bg-emerald-100 flex items-center justify-center">
                <CheckCircle2 size={20} className="text-emerald-600" />
              </div>
              <div>
                <h3 className="font-bold text-emerald-800">Cleaning Complete!</h3>
                <p className="text-sm text-emerald-600">
                  Version v{result.new_version} created · {result.rows_before?.toLocaleString()} → {result.rows_after?.toLocaleString()} rows
                </p>
              </div>
            </div>

            {result.fixes_applied?.length > 0 && (
              <div className="bg-white/70 rounded-lg p-4 space-y-2">
                <p className="text-xs font-semibold text-slate-600 uppercase tracking-wide">
                  {result.fixes_applied.length} fix{result.fixes_applied.length !== 1 ? 'es' : ''} applied
                </p>
                {result.fixes_applied.map((fix, i) => (
                  <div key={i} className="flex items-start gap-2 text-sm">
                    <CheckCircle2 size={13} className="text-emerald-500 flex-shrink-0 mt-0.5" />
                    <span className="text-slate-700">{fix}</span>
                  </div>
                ))}
              </div>
            )}

            <div className="flex gap-3 pt-1">
              <button onClick={() => { setResult(null); loadSuggestions() }} className="btn-primary">
                Clean Again
              </button>
            </div>
          </div>
        </div>
      )}
    </div>
  )
}
