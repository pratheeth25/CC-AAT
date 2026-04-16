import { useEffect, useState } from 'react'
import { ShieldAlert, Eye, AlertTriangle } from 'lucide-react'
import { datasetsAPI } from '../../services/api'
import { PageSpinner } from '../../components/LoadingSpinner'
import ErrorAlert from '../../components/ErrorAlert'
import EmptyState from '../../components/EmptyState'

const PII_LABELS = {
  email: 'Email Address',
  phone: 'Phone Number',
  credit_card: 'Credit Card',
  aadhaar: 'Aadhaar Number',
  passport: 'Passport Number',
  ipv4: 'IP Address',
  ssn: 'SSN',
}

const RISK_STYLES = {
  high:   { bg: 'bg-red-50 border-red-300',    text: 'text-red-700',     badge: 'bg-red-100 text-red-800' },
  medium: { bg: 'bg-orange-50 border-orange-300', text: 'text-orange-700',  badge: 'bg-orange-100 text-orange-800' },
  low:    { bg: 'bg-yellow-50 border-yellow-300',  text: 'text-yellow-700',  badge: 'bg-yellow-100 text-yellow-800' },
  none:   { bg: 'bg-emerald-50 border-emerald-300', text: 'text-emerald-700', badge: 'bg-emerald-100 text-emerald-800' },
}

const CONFIDENCE_DOT = {
  high: 'bg-red-500',
  medium: 'bg-orange-400',
  low: 'bg-yellow-400',
}

export default function PIITab({ datasetId }) {
  const [data, setData] = useState(null)
  const [loading, setLoading] = useState(true)
  const [error, setError] = useState(null)

  const load = async () => {
    setLoading(true); setError(null)
    try { setData(await datasetsAPI.getPii(datasetId)) }
    catch (e) { setError(e.message) }
    finally { setLoading(false) }
  }

  useEffect(() => { load() }, [datasetId])

  if (loading) return <PageSpinner text="Scanning for PII…" />
  if (error) return <ErrorAlert message={error} onRetry={load} />

  const columns = data?.columns ?? []
  const riskLevel = data?.risk_level ?? 'none'
  const riskStyle = RISK_STYLES[riskLevel] || RISK_STYLES.none

  return (
    <div className="space-y-6 animate-fade-in max-w-4xl">

      {/* Risk banner */}
      <div className={`rounded-xl border p-5 ${riskStyle.bg}`}>
        <div className="flex items-center gap-3">
          <ShieldAlert className={`w-6 h-6 ${riskStyle.text}`} />
          <div>
            <h2 className={`font-semibold text-sm ${riskStyle.text}`}>
              PII Risk Level: <span className="uppercase">{riskLevel}</span>
            </h2>
            <p className="text-xs text-slate-600 mt-0.5">
              {columns.length === 0
                ? 'No personally identifiable information detected in this dataset.'
                : `${columns.length} column${columns.length !== 1 ? 's' : ''} contain potential PII.`}
            </p>
          </div>
        </div>
      </div>

      {columns.length === 0 ? (
        <EmptyState
          title="No PII detected"
          description="All columns passed the PII scan. No emails, phones, cards, or IDs found."
        />
      ) : (
        <>
          {/* Summary table */}
          <div className="card overflow-hidden">
            <div className="card-header">
              <h2 className="font-semibold text-slate-800 text-sm flex items-center gap-2">
                <Eye size={15} className="text-violet-500" />
                PII Columns ({columns.length})
              </h2>
            </div>
            <table className="table-base">
              <thead>
                <tr>
                  <th>Column</th>
                  <th>PII Type</th>
                  <th>Confidence</th>
                  <th>Matches</th>
                  <th>Ratio</th>
                </tr>
              </thead>
              <tbody>
                {columns.map(col => (
                  <tr key={col.column_name}>
                    <td className="font-semibold text-slate-800">{col.column_name}</td>
                    <td>
                      <span className="text-xs font-medium bg-violet-100 text-violet-700 px-2 py-0.5 rounded-full">
                        {PII_LABELS[col.pii_type] || col.pii_type}
                      </span>
                    </td>
                    <td>
                      <span className="flex items-center gap-1.5">
                        <span className={`w-2 h-2 rounded-full ${CONFIDENCE_DOT[col.confidence] || 'bg-slate-300'}`} />
                        <span className="text-sm capitalize">{col.confidence}</span>
                      </span>
                    </td>
                    <td className="font-mono text-sm">
                      {col.match_count} / {col.total_rows}
                    </td>
                    <td className="font-mono text-sm">
                      {(col.match_ratio * 100).toFixed(1)}%
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>

          {/* Sample matches per column */}
          <div className="space-y-4">
            {columns.map(col => (
              <div key={col.column_name} className="card p-5">
                <div className="flex items-center justify-between mb-3">
                  <div className="flex items-center gap-2">
                    <AlertTriangle size={14} className="text-orange-500" />
                    <span className="font-semibold text-slate-800 text-sm">{col.column_name}</span>
                    <span className="text-xs text-slate-400">— {PII_LABELS[col.pii_type] || col.pii_type}</span>
                  </div>
                  <span className={`text-xs font-bold px-2 py-0.5 rounded-full ${RISK_STYLES[col.confidence]?.badge || ''}`}>
                    {col.confidence}
                  </span>
                </div>
                <div className="flex flex-wrap gap-1.5">
                  {(col.sample_matches || []).map((v, i) => (
                    <span key={i} className="inline-flex items-center px-2 py-0.5 rounded font-mono text-xs bg-violet-50 text-violet-700 border border-violet-200 max-w-[200px] truncate">
                      {String(v)}
                    </span>
                  ))}
                </div>
              </div>
            ))}
          </div>
        </>
      )}
    </div>
  )
}
