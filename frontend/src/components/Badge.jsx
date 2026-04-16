/**
 * Badge — severity, quality grade, alert type, file type
 */
export function SeverityBadge({ severity }) {
  const map = {
    critical: 'bg-red-100 text-red-700 border-red-200',
    high:     'bg-orange-100 text-orange-700 border-orange-200',
    medium:   'bg-yellow-100 text-yellow-700 border-yellow-200',
    low:      'bg-sky-100 text-sky-700 border-sky-200',
  }
  return (
    <span className={`inline-flex items-center px-2 py-0.5 rounded-full text-xs font-semibold border ${map[severity] || map.low}`}>
      {severity?.charAt(0).toUpperCase() + severity?.slice(1)}
    </span>
  )
}

export function GradeBadge({ grade }) {
  const map = {
    A:    'bg-emerald-100 text-emerald-700 border-emerald-200',
    B:    'bg-blue-100 text-blue-700 border-blue-200',
    C:    'bg-yellow-100 text-yellow-700 border-yellow-200',
    D:    'bg-orange-100 text-orange-700 border-orange-200',
    F:    'bg-red-100 text-red-700 border-red-200',
    'F-': 'bg-red-200 text-red-900 border-red-400 animate-pulse',
  }
  return (
    <span className={`inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-bold border ${map[grade] || map.F}`}>
      Grade {grade}
    </span>
  )
}

export function FileTypeBadge({ type }) {
  const map = {
    csv:  'bg-teal-100 text-teal-700',
    json: 'bg-violet-100 text-violet-700',
  }
  return (
    <span className={`inline-flex items-center px-2 py-0.5 rounded text-xs font-medium uppercase ${map[type] || 'bg-slate-100 text-slate-600'}`}>
      {type}
    </span>
  )
}

export function AlertTypeBadge({ type }) {
  const map = {
    high_missing:     { label: 'High Missing',     cls: 'bg-amber-100 text-amber-700' },
    anomaly_detected: { label: 'Anomaly',           cls: 'bg-red-100 text-red-700'    },
    quality_degraded: { label: 'Quality Drop',      cls: 'bg-orange-100 text-orange-700' },
    drift_detected:   { label: 'Data Drift',        cls: 'bg-violet-100 text-violet-700' },
  }
  const t = map[type] || { label: type, cls: 'bg-slate-100 text-slate-600' }
  return (
    <span className={`inline-flex items-center px-2 py-0.5 rounded text-xs font-medium ${t.cls}`}>
      {t.label}
    </span>
  )
}
