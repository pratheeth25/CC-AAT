import { useCallback, useState } from 'react'
import { useNavigate } from 'react-router-dom'
import { UploadCloud, FileText, X, CheckCircle2, AlertCircle } from 'lucide-react'
import toast from 'react-hot-toast'
import { datasetsAPI } from '../services/api'

const ACCEPTED = ['.csv', '.json']
const MAX_MB   = 100

function fmt(bytes) {
  if (bytes < 1024) return `${bytes} B`
  if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(1)} KB`
  return `${(bytes / 1024 / 1024).toFixed(1)} MB`
}

export default function Upload() {
  const navigate = useNavigate()
  const [file,     setFile]     = useState(null)
  const [progress, setProgress] = useState(0)
  const [status,   setStatus]   = useState('idle') // idle | uploading | success | error
  const [result,   setResult]   = useState(null)
  const [errMsg,   setErrMsg]   = useState('')
  const [dragOver, setDragOver] = useState(false)

  const validateAndSet = (f) => {
    if (!f) return
    const ext = f.name.substring(f.name.lastIndexOf('.')).toLowerCase()
    if (!ACCEPTED.includes(ext)) {
      toast.error(`Only ${ACCEPTED.join(', ')} files are supported.`)
      return
    }
    if (f.size > MAX_MB * 1024 * 1024) {
      toast.error(`File exceeds ${MAX_MB} MB limit.`)
      return
    }
    setFile(f)
    setStatus('idle')
    setResult(null)
  }

  const handleDrop = useCallback((e) => {
    e.preventDefault(); setDragOver(false)
    validateAndSet(e.dataTransfer.files[0])
  }, [])

  const handleUpload = async () => {
    if (!file) return
    setStatus('uploading'); setProgress(0); setErrMsg('')
    try {
      const data = await datasetsAPI.upload(file, (evt) => {
        setProgress(Math.round((evt.loaded * 100) / (evt.total || 1)))
      })
      setResult(data)
      setStatus('success')
      toast.success('Dataset uploaded successfully!')
    } catch (e) {
      setStatus('error')
      setErrMsg(e.message)
      toast.error(e.message)
    }
  }

  const reset = () => { setFile(null); setStatus('idle'); setProgress(0); setResult(null) }

  return (
    <div className="p-8 max-w-2xl animate-fade-in">
      <div className="mb-6">
        <h1 className="text-2xl font-bold text-slate-900">Upload Dataset</h1>
        <p className="text-sm text-slate-500 mt-1">
          Supported formats: CSV, JSON — max {MAX_MB} MB
        </p>
      </div>

      {/* Drop zone */}
      <div
        onDragOver={(e) => { e.preventDefault(); setDragOver(true) }}
        onDragLeave={() => setDragOver(false)}
        onDrop={handleDrop}
        onClick={() => !file && document.getElementById('file-input').click()}
        className={`
          relative rounded-2xl border-2 border-dashed transition-all duration-200 cursor-pointer
          flex flex-col items-center justify-center gap-3 py-16 px-6 text-center
          ${dragOver
            ? 'border-blue-500 bg-blue-50'
            : file
            ? 'border-slate-300 bg-slate-50 cursor-default'
            : 'border-slate-300 bg-white hover:border-blue-400 hover:bg-blue-50/40'
          }
        `}
      >
        <input
          id="file-input"
          type="file"
          accept=".csv,.json"
          className="hidden"
          onChange={e => validateAndSet(e.target.files[0])}
        />

        {!file ? (
          <>
            <div className="w-16 h-16 rounded-2xl bg-blue-100 flex items-center justify-center">
              <UploadCloud size={30} className="text-blue-500" />
            </div>
            <div>
              <p className="font-semibold text-slate-700">Drag & drop your file here</p>
              <p className="text-sm text-slate-400 mt-1">or click to browse — CSV, JSON up to {MAX_MB} MB</p>
            </div>
          </>
        ) : (
          <div className="w-full flex items-center gap-4">
            <div className="w-12 h-12 rounded-xl bg-teal-100 flex items-center justify-center flex-shrink-0">
              <FileText size={22} className="text-teal-600" />
            </div>
            <div className="flex-1 text-left min-w-0">
              <p className="font-medium text-slate-800 truncate">{file.name}</p>
              <p className="text-sm text-slate-400">{fmt(file.size)}</p>
            </div>
            {status === 'idle' && (
              <button onClick={(e) => { e.stopPropagation(); reset() }}
                className="w-7 h-7 rounded-full bg-slate-200 hover:bg-slate-300 flex items-center justify-center transition-colors">
                <X size={14} className="text-slate-600" />
              </button>
            )}
          </div>
        )}
      </div>

      {/* Progress bar */}
      {status === 'uploading' && (
        <div className="mt-5">
          <div className="flex justify-between text-xs text-slate-500 mb-1.5">
            <span>Uploading…</span>
            <span>{progress}%</span>
          </div>
          <div className="h-2 bg-slate-200 rounded-full overflow-hidden">
            <div
              className="h-full bg-blue-500 rounded-full transition-all duration-300"
              style={{ width: `${progress}%` }}
            />
          </div>
        </div>
      )}

      {/* Success result */}
      {status === 'success' && result && (
        <div className="mt-5 rounded-xl border border-emerald-200 bg-emerald-50 p-5 animate-slide-up">
          <div className="flex items-center gap-2 mb-3">
            <CheckCircle2 size={18} className="text-emerald-600" />
            <span className="font-semibold text-emerald-700">Upload successful!</span>
          </div>
          <div className="grid grid-cols-2 gap-3 text-sm">
            {[
              ['Name',     result.name],
              ['Rows',     result.row_count?.toLocaleString()],
              ['Columns',  result.col_count],
              ['Version',  `v${result.current_version}`],
              ['Type',     result.file_type?.toUpperCase()],
              ['Size',     fmt(result.size_bytes)],
            ].map(([k, v]) => (
              <div key={k} className="flex gap-2">
                <span className="text-slate-500 w-20 flex-shrink-0">{k}</span>
                <span className="font-medium text-slate-800">{v}</span>
              </div>
            ))}
          </div>
          <div className="flex gap-3 mt-4">
            <button onClick={() => navigate(`/dataset/${result._id}`)} className="btn-primary">
              View Dataset
            </button>
            <button onClick={reset} className="btn-secondary">
              Upload Another
            </button>
          </div>
        </div>
      )}

      {/* Error */}
      {status === 'error' && (
        <div className="mt-5 rounded-xl border border-red-200 bg-red-50 p-4 flex items-start gap-3 animate-slide-up">
          <AlertCircle size={17} className="text-red-500 shrink-0 mt-0.5" />
          <div>
            <p className="text-sm font-semibold text-red-700">Upload failed</p>
            <p className="text-sm text-red-600 mt-0.5">{errMsg}</p>
          </div>
        </div>
      )}

      {/* Upload button */}
      {file && status === 'idle' && (
        <button onClick={handleUpload} className="btn-primary mt-5 w-full justify-center py-3 text-base">
          <UploadCloud size={18} /> Upload Dataset
        </button>
      )}
    </div>
  )
}
