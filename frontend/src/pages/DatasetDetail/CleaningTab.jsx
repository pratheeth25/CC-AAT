import { useEffect, useState } from 'react'
import {
  Wrench, CheckCircle2, ChevronDown, ChevronUp,
  TrendingUp, AlertTriangle, Zap, Shield, Database,
  Clock, Tag, RefreshCw, XCircle,
} from 'lucide-react'
import toast from 'react-hot-toast'
import { datasetsAPI } from '../../services/api'
import { PageSpinner } from '../../components/LoadingSpinner'
import ErrorAlert from '../../components/ErrorAlert'
import EmptyState from '../../components/EmptyState'

// ── helpers ─────────────────────────────────────────────────────────────────

const DIMENSION_META = {
  completeness: { label: 'Completeness', color: 'bg-blue-100 text-blue-700',   icon: '◉' },
  validity:     { label: 'Validity',     color: 'bg-orange-100 text-orange-700', icon: '✓' },
  consistency:  { label: 'Consistency',  color: 'bg-purple-100 text-purple-700', icon: '≡' },
  uniqueness:   { label: 'Uniqueness',   color: 'bg-cyan-100 text-cyan-700',     icon: '◇' },
  garbage:      { label: 'Integrity',    color: 'bg-slate-100 text-slate-600',   icon: '⊘' },
}
const dimMeta = (d) => DIMENSION_META[d] ?? { label: d, color: 'bg-slate-100 text-slate-600', icon: '•' }

const CONFIDENCE_META = {
  high:   { label: 'High confidence',   color: 'bg-emerald-100 text-emerald-700' },
  medium: { label: 'Medium confidence', color: 'bg-amber-100 text-amber-700' },
  low:    { label: 'Low confidence',    color: 'bg-red-100 text-red-700' },
}
const confMeta = (c) => CONFIDENCE_META[c] ?? CONFIDENCE_META.medium

const gradeColor = (g) => ({
  A: 'text-emerald-600', B: 'text-green-600',
  C: 'text-amber-600',   D: 'text-orange-600', F: 'text-red-600',
}[g] ?? 'text-slate-600')

const scoreBarColor = (score) =>
  score >= 80 ? 'bg-emerald-500' :
  score >= 60 ? 'bg-amber-500'   :
  score >= 40 ? 'bg-orange-500'  : 'bg-red-500'

const DEFAULT_FORM = {
  fix_missing_numeric:    true,
  fix_missing_categorical:true,
  fix_duplicates:         true,
  standardize_dates:      true,
  fix_emails:             true,
  normalize_countries:    true,
  normalize_case:         '',
  remove_outliers:        false,
}

// ── sub-components ──────────────────────────────────────────────────────────

function ScoreJourney({ steps, scoreBefore, scoreAfter }) {
  const points = [
    { label: 'Baseline', score: scoreBefore },
    ...steps.filter(s => s.score_gain !== 0).map(s => ({
      label: s.name,
      score: s.score_after,
      gain:  s.score_gain,
    })),
  ]
  const max = Math.max(...points.map(p => p.score), scoreAfter)
  const min = Math.min(...points.map(p => p.score), scoreBefore) - 5

  return (
    <div className="card">
      <div className="card-header">
        <h3 className="font-semibold text-slate-800 text-sm flex items-center gap-2">
          <TrendingUp size={14} className="text-blue-500" />
          Score Journey
        </h3>
        <span className="text-xs text-slate-400">Quality improvement per step</span>
      </div>
      <div className="card-body">
        <div className="flex items-end gap-2 overflow-x-auto pb-2">
          {points.map((p, i) => {
            const heightPct = ((p.score - min) / (max - min + 1)) * 100
            return (
              <div key={i} className="flex flex-col items-center gap-1 flex-shrink-0 min-w-[60px]">
                <span className="text-[10px] font-bold text-slate-600">{Math.round(p.score)}</span>
                {p.gain !== undefined && (
                  <span className={`text-[9px] font-semibold px-1 rounded ${p.gain >= 0 ? 'text-emerald-600 bg-emerald-50' : 'text-red-600 bg-red-50'}`}>
                    {p.gain >= 0 ? '+' : ''}{p.gain.toFixed(1)}
                  </span>
                )}
                <div
                  className={`w-8 rounded-t transition-all ${scoreBarColor(p.score)}`}
                  style={{ height: `${Math.max(heightPct, 15)}px` }}
                />
                <span className="text-[9px] text-slate-400 text-center leading-tight max-w-[60px] truncate" title={p.label}>
                  {i === 0 ? 'Baseline' : p.label.split(' ').slice(0,2).join(' ')}
                </span>
              </div>
            )
          })}
        </div>
      </div>
    </div>
  )
}

function BeforeAfterCard({ scoreBefore, scoreAfter, gradeBefore, gradeAfter, scoresDelta, improvementPct, rowsBefore, rowsAfter }) {
  return (
    <div className="grid grid-cols-2 gap-4">
      <div className="card bg-slate-50 border-slate-200">
        <div className="card-body">
          <p className="text-xs text-slate-500 font-medium uppercase tracking-wide mb-2">Before</p>
          <div className="flex items-end gap-2">
            <span className="text-4xl font-black text-slate-700">{Math.round(scoreBefore)}</span>
            <span className={`text-xl font-bold mb-1 ${gradeColor(gradeBefore)}`}>{gradeBefore}</span>
          </div>
          <div className="w-full bg-slate-200 rounded-full h-1.5 mt-2">
            <div className={`h-1.5 rounded-full ${scoreBarColor(scoreBefore)}`} style={{ width: `${scoreBefore}%` }} />
          </div>
          <p className="text-xs text-slate-400 mt-2">{rowsBefore?.toLocaleString()} rows</p>
        </div>
      </div>
      <div className="card bg-emerald-50 border-emerald-200">
        <div className="card-body">
          <p className="text-xs text-emerald-600 font-medium uppercase tracking-wide mb-2">After</p>
          <div className="flex items-end gap-2">
            <span className="text-4xl font-black text-emerald-700">{Math.round(scoreAfter)}</span>
            <span className={`text-xl font-bold mb-1 ${gradeColor(gradeAfter)}`}>{gradeAfter}</span>
          </div>
          <div className="w-full bg-emerald-100 rounded-full h-1.5 mt-2">
            <div className={`h-1.5 rounded-full ${scoreBarColor(scoreAfter)}`} style={{ width: `${scoreAfter}%` }} />
          </div>
          <p className="text-xs text-emerald-600 mt-2 font-medium">
            {rowsAfter?.toLocaleString()} rows · {scoresDelta >= 0 ? '+' : ''}{scoresDelta} pts ({improvementPct >= 0 ? '+' : ''}{improvementPct}%)
          </p>
        </div>
      </div>
    </div>
  )
}

function StepCard({ step }) {
  const [open, setOpen] = useState(false)
  const dim  = dimMeta(step.dimension)
  const conf = confMeta(step.confidence)

  return (
    <div className={`rounded-xl border bg-white shadow-sm overflow-hidden transition-shadow hover:shadow-md ${step.score_gain > 0 ? 'border-emerald-200' : step.score_gain < 0 ? 'border-red-200' : 'border-slate-200'}`}>
      <button
        onClick={() => setOpen(o => !o)}
        className="w-full flex items-start gap-3 p-4 text-left"
      >
        <div className={`flex-shrink-0 mt-0.5 w-14 h-8 rounded-lg flex items-center justify-center text-sm font-bold ${
          step.score_gain > 0  ? 'bg-emerald-100 text-emerald-700' :
          step.score_gain < 0  ? 'bg-red-100 text-red-700'         :
                                  'bg-slate-100 text-slate-500'
        }`}>
          {step.score_gain > 0 ? '+' : ''}{step.score_gain.toFixed(1)}
        </div>
        <div className="flex-1 min-w-0">
          <div className="flex flex-wrap items-center gap-2 mb-1">
            <span className="text-sm font-semibold text-slate-800">{step.name}</span>
            <span className={`text-[10px] font-semibold px-2 py-0.5 rounded-full ${dim.color}`}>
              {dim.icon} {dim.label}
            </span>
            <span className={`text-[10px] font-semibold px-2 py-0.5 rounded-full ${conf.color}`}>
              {conf.label}
            </span>
          </div>
          <p className="text-xs text-slate-500 truncate">{step.description}</p>
          <div className="flex gap-4 mt-1 text-[10px] text-slate-400">
            <span>{step.rows_affected.toLocaleString()} rows affected</span>
            <span>{step.score_before} → {step.score_after}</span>
          </div>
        </div>
        {step.changes?.length > 0 && (
          <div className="flex-shrink-0 text-slate-400">
            {open ? <ChevronUp size={14} /> : <ChevronDown size={14} />}
          </div>
        )}
      </button>
      {open && step.changes?.length > 0 && (
        <div className="border-t border-slate-100 bg-slate-50 px-4 py-3 space-y-1.5">
          {step.changes.map((c, i) => (
            <div key={i} className="flex items-start gap-2 text-xs text-slate-600">
              <span className="text-emerald-500 flex-shrink-0 mt-0.5">→</span>
              <span>{c}</span>
            </div>
          ))}
        </div>
      )}
    </div>
  )
}

function RemainingIssues({ issues }) {
  if (!issues?.length) return null
  return (
    <div className="card border-amber-200 bg-amber-50">
      <div className="card-header">
        <h3 className="font-semibold text-amber-800 text-sm flex items-center gap-2">
          <AlertTriangle size={14} className="text-amber-600" />
          Remaining Issues
        </h3>
      </div>
      <div className="card-body space-y-2">
        {issues.map((issue, i) => (
          <div key={i} className="flex items-start gap-2 text-sm text-amber-800">
            <AlertTriangle size={12} className="text-amber-500 flex-shrink-0 mt-0.5" />
            <span>{issue}</span>
          </div>
        ))}
      </div>
    </div>
  )
}

// ── main component ───────────────────────────────────────────────────────────

export default function CleaningTab({ datasetId, dataset, onCleanSuccess }) {
  const [suggestions, setSuggestions] = useState(null)
  const [loadingRep,  setLoadingRep]  = useState(true)
  const [errorRep,    setErrorRep]    = useState(null)
  const [qualityScore, setQualityScore] = useState(null)
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

  // Fetch quality score to show critical banner
  useEffect(() => {
    datasetsAPI.getQuality(datasetId).then(q => {
      setQualityScore(q?.total_score ?? null)
    }).catch(() => {})
  }, [datasetId])

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
        fix_emails:              form.fix_emails,
        normalize_countries:     form.normalize_countries,
        normalize_case:          form.normalize_case || null,
        remove_outliers:         form.remove_outliers,
      }
      const res = await datasetsAPI.clean(datasetId, payload)
      setResult(res)
      toast.success(`Cleaned — v${res.new_version} created (+${res.score_delta} pts)`)
      onCleanSuccess?.()
    } catch (e) {
      toast.error(e.message)
    } finally {
      setCleaning(false)
    }
  }

  // ─── Result view ──────────────────────────────────────────────────────────
  if (result) {
    // Skipped — already clean
    if (result.skipped) {
      return (
        <div className="space-y-4 animate-fade-in max-w-2xl">
          <div className="rounded-xl border-2 border-emerald-300 bg-emerald-50 p-5 flex items-start gap-4">
            <CheckCircle2 size={24} className="text-emerald-500 flex-shrink-0 mt-0.5" />
            <div className="flex-1">
              <p className="font-bold text-emerald-800 text-base">
                No Cleaning Needed — {Math.round(result.score_before)}/100
              </p>
              <p className="text-sm text-emerald-700 mt-1">
                Dataset quality is already above 80. No changes were made.
              </p>
            </div>
          </div>
          <button onClick={() => setResult(null)} className="btn-secondary text-sm">
            ← Back
          </button>
        </div>
      )
    }

    const activeSteps = result.steps?.filter(s => s.rows_affected > 0 || s.score_gain !== 0) ?? []
    return (
      <div className="space-y-6 animate-fade-in max-w-2xl">
        <div className="flex items-center justify-between">
          <div className="flex items-center gap-3">
            <div className="w-10 h-10 rounded-full bg-emerald-100 flex items-center justify-center">
              <CheckCircle2 size={20} className="text-emerald-600" />
            </div>
            <div>
              <h2 className="font-bold text-slate-900">Cleaning Complete</h2>
              <p className="text-sm text-slate-500">
                Version v{result.new_version} saved · {activeSteps.length} step{activeSteps.length !== 1 ? 's' : ''} applied
              </p>
            </div>
          </div>
          <button
            onClick={() => { setResult(null); loadSuggestions() }}
            className="btn-secondary flex items-center gap-1.5 text-sm"
          >
            <RefreshCw size={13} />
            Clean Again
          </button>
        </div>

        <BeforeAfterCard
          scoreBefore={result.score_before}
          scoreAfter={result.score_after}
          gradeBefore={result.grade_before}
          gradeAfter={result.grade_after}
          scoresDelta={result.score_delta}
          improvementPct={result.improvement_pct}
          rowsBefore={result.rows_before}
          rowsAfter={result.rows_after}
        />

        {activeSteps.length > 0 && (
          <ScoreJourney steps={activeSteps} scoreBefore={result.score_before} scoreAfter={result.score_after} />
        )}

        {activeSteps.length > 0 && (
          <div className="space-y-3">
            <h3 className="font-semibold text-slate-700 text-sm flex items-center gap-2">
              <Zap size={13} className="text-amber-500" />
              Repair Steps ({activeSteps.length})
            </h3>
            {activeSteps.map((step, i) => <StepCard key={i} step={step} />)}
          </div>
        )}

        <RemainingIssues issues={result.remaining_issues} />

        {result.generated_at && (
          <p className="text-[10px] text-slate-400 flex items-center gap-1">
            <Clock size={10} />
            Generated {new Date(result.generated_at).toLocaleString()}
          </p>
        )}
      </div>
    )
  }

  // ─── Config form ──────────────────────────────────────────────────────────
  // High quality gate — no cleaning needed above 80
  if (qualityScore !== null && qualityScore > 80) {
    return (
      <div className="space-y-4 animate-fade-in max-w-2xl">
        <div className="rounded-xl border-2 border-emerald-300 bg-emerald-50 p-5 flex items-start gap-4">
          <CheckCircle2 size={24} className="text-emerald-500 flex-shrink-0 mt-0.5" />
          <div className="flex-1">
            <p className="font-bold text-emerald-800 text-base">
              Dataset is Already High Quality — {Math.round(qualityScore)}/100
            </p>
            <p className="text-sm text-emerald-700 mt-1">
              This dataset scores above 80. No cleaning or repair suggestions are required.
              You can continue using it as-is.
            </p>
          </div>
        </div>
      </div>
    )
  }

  return (
    <div className="space-y-6 animate-fade-in max-w-2xl">

      {/* Critical quality banner — shown when score < 40 */}
      {qualityScore !== null && qualityScore < 40 && (
        <div className="rounded-xl border-2 border-red-300 bg-red-50 p-4 flex items-start gap-3">
          <XCircle size={20} className="text-red-500 flex-shrink-0 mt-0.5" />
          <div className="flex-1 min-w-0">
            <p className="font-bold text-red-800 text-sm">
              Critical Quality Score: {Math.round(qualityScore)}/100
            </p>
            <p className="text-sm text-red-700 mt-0.5">
              This dataset has severe data quality issues. Review the repair suggestions below,
              then apply the cleaning pipeline to fix them.
            </p>
            <p className="text-xs text-red-500 mt-1 font-medium">
              Recommended: enable all cleaning steps before applying
            </p>
          </div>
        </div>
      )}

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
                  <span className="text-[11px] font-semibold border px-2 py-0.5 rounded-full flex-shrink-0 mt-0.5 bg-amber-50 text-amber-700 border-amber-200">
                    {s.issue_type?.replace(/_/g,' ')}
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

      <div className="card">
        <div className="card-header">
          <h2 className="font-semibold text-slate-800 text-sm">Apply Cleaning Pipeline</h2>
          <span className="text-xs text-slate-400">Creates a new version · score recalculates after each step</span>
        </div>
        <div className="card-body space-y-3">
          {[
            { key: 'fix_missing_numeric',     label: 'Fill missing numeric values',    desc: 'Mean/median auto-selected by skew',   icon: <Database size={13} className="text-blue-500" /> },
            { key: 'fix_missing_categorical', label: 'Fill missing categorical values', desc: 'Mode (most common value) imputation',  icon: <Tag size={13} className="text-indigo-500" /> },
            { key: 'fix_duplicates',          label: 'Remove exact duplicate rows',     desc: 'Drops all exact duplicates',           icon: <Shield size={13} className="text-cyan-500" /> },
            { key: 'standardize_dates',       label: 'Standardize dates → ISO 8601',   desc: 'YYYY-MM-DD, drops impossible dates',   icon: <Clock size={13} className="text-amber-500" /> },
            { key: 'fix_emails',              label: 'Validate & repair email columns', desc: 'Regex validation, sets invalid → NaN', icon: <CheckCircle2 size={13} className="text-orange-500" /> },
            { key: 'normalize_countries',     label: 'Normalize country synonyms',      desc: 'USA / US / America → United States',  icon: <Zap size={13} className="text-purple-500" /> },
          ].map(({ key, label, desc, icon }) => (
            <label key={key} className="flex items-center gap-3 cursor-pointer group">
              <div
                onClick={() => toggle(key)}
                className={`w-5 h-5 rounded border-2 flex items-center justify-center transition-colors flex-shrink-0 cursor-pointer ${
                  form[key] ? 'bg-blue-600 border-blue-600' : 'border-slate-300 group-hover:border-slate-400'
                }`}
              >
                {form[key] && <CheckCircle2 size={12} className="text-white" />}
              </div>
              <div className="flex items-start gap-2">
                <span className="mt-0.5 flex-shrink-0">{icon}</span>
                <div>
                  <p className="text-sm font-medium text-slate-700">{label}</p>
                  <p className="text-xs text-slate-400">{desc}</p>
                </div>
              </div>
            </label>
          ))}

          <div className="flex items-center gap-3 pt-1 pl-8">
            <label className="text-sm font-medium text-slate-700">Text casing:</label>
            <select
              className="select-base w-36"
              value={form.normalize_case}
              onChange={e => setForm(f => ({ ...f, normalize_case: e.target.value }))}
            >
              <option value="">None (trim only)</option>
              <option value="lower">Lowercase</option>
              <option value="upper">UPPERCASE</option>
              <option value="title">Title Case</option>
            </select>
          </div>

          <label className="flex items-center gap-3 cursor-pointer group border-t border-slate-100 pt-3 mt-1">
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
              <p className="text-xs text-orange-500 font-medium">⚠ Destructive — removes rows</p>
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
                  Running pipeline…
                </span>
              ) : (
                <span className="flex items-center gap-2">
                  <Zap size={15} />
                  Apply Cleaning Pipeline
                </span>
              )}
            </button>
          </div>
        </div>
      </div>
    </div>
  )
}
