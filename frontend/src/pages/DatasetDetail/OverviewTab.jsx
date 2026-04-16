import { Layers, Calendar, HardDrive, Hash, CheckCircle } from 'lucide-react'
import { FileTypeBadge } from '../../components/Badge'

function fmt(bytes) {
  if (bytes < 1024) return `${bytes} B`
  if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(1)} KB`
  return `${(bytes / 1024 / 1024).toFixed(1)} MB`
}

function fmtDate(str) {
  return new Date(str).toLocaleString('en-US', { dateStyle: 'medium', timeStyle: 'short' })
}

export default function OverviewTab({ dataset }) {
  return (
    <div className="space-y-6 animate-fade-in max-w-4xl">

      {/* Metadata cards */}
      <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
        {[
          { icon: Hash,       label: 'Rows',     value: dataset.row_count?.toLocaleString()   },
          { icon: Layers,     label: 'Columns',  value: dataset.col_count                     },
          { icon: HardDrive,  label: 'Size',     value: fmt(dataset.size_bytes)               },
          { icon: Calendar,   label: 'Uploaded', value: fmtDate(dataset.uploaded_at)          },
        ].map(({ icon: Icon, label, value }) => (
          <div key={label} className="card p-4 flex items-center gap-3">
            <div className="w-9 h-9 rounded-lg bg-slate-100 flex items-center justify-center flex-shrink-0">
              <Icon size={16} className="text-slate-500" />
            </div>
            <div>
              <p className="text-xs text-slate-400 font-medium">{label}</p>
              <p className="text-sm font-semibold text-slate-800 mt-0.5">{value}</p>
            </div>
          </div>
        ))}
      </div>

      {/* Version history */}
      <div className="card">
        <div className="card-header">
          <h2 className="font-semibold text-slate-800 text-sm">Version History</h2>
          <span className="text-xs text-slate-400">{dataset.versions?.length} version{dataset.versions?.length !== 1 ? 's' : ''}</span>
        </div>
        <div className="divide-y divide-slate-100">
          {[...(dataset.versions || [])].reverse().map(ver => (
            <div key={ver.version_number} className="px-6 py-4 flex items-start gap-4">
              <div className={`w-8 h-8 rounded-full flex items-center justify-center text-xs font-bold flex-shrink-0 ${
                ver.version_number === dataset.current_version
                  ? 'bg-blue-100 text-blue-700'
                  : 'bg-slate-100 text-slate-500'
              }`}>
                v{ver.version_number}
              </div>
              <div className="flex-1 min-w-0">
                <div className="flex items-center gap-2">
                  <span className="text-sm font-medium text-slate-700">
                    Version {ver.version_number}
                  </span>
                  {ver.version_number === dataset.current_version && (
                    <span className="text-[10px] font-semibold text-emerald-600 bg-emerald-100 px-1.5 py-0.5 rounded">
                      CURRENT
                    </span>
                  )}
                </div>
                <p className="text-xs text-slate-400 mt-0.5">
                  {fmtDate(ver.created_at)}
                  &nbsp;·&nbsp;{ver.row_count?.toLocaleString()} rows × {ver.col_count} cols
                </p>
                {ver.changes_applied?.length > 0 && (
                  <ul className="mt-2 space-y-1">
                    {ver.changes_applied.map((c, i) => (
                      <li key={i} className="flex items-start gap-1.5 text-xs text-slate-500">
                        <CheckCircle size={11} className="text-emerald-500 flex-shrink-0 mt-0.5" />
                        {c}
                      </li>
                    ))}
                  </ul>
                )}
              </div>
            </div>
          ))}
        </div>
      </div>

      {/* File info */}
      <div className="card">
        <div className="card-header">
          <h2 className="font-semibold text-slate-800 text-sm">File Information</h2>
        </div>
        <div className="card-body grid grid-cols-2 gap-4">
          {[
            ['Original filename', dataset.original_filename],
            ['File type',         <FileTypeBadge key="ft" type={dataset.file_type} />],
            ['Dataset ID',        <code key="id" className="text-xs font-mono break-all">{dataset._id}</code>],
            ['Current version',   `v${dataset.current_version}`],
          ].map(([k, v]) => (
            <div key={k} className="flex flex-col gap-1">
              <span className="text-xs text-slate-400 font-medium">{k}</span>
              <span className="text-sm text-slate-700">{v}</span>
            </div>
          ))}
        </div>
      </div>

    </div>
  )
}
