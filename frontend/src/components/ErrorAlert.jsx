import { AlertCircle, RefreshCw } from 'lucide-react'

export default function ErrorAlert({ message, onRetry }) {
  return (
    <div className="rounded-xl border border-red-200 bg-red-50 p-5 flex items-start gap-3 animate-fade-in">
      <AlertCircle size={20} className="text-red-500 flex-shrink-0 mt-0.5" />
      <div className="flex-1 min-w-0">
        <p className="text-sm font-semibold text-red-700">Something went wrong</p>
        <p className="text-sm text-red-600 mt-1 break-words">{message}</p>
      </div>
      {onRetry && (
        <button onClick={onRetry} className="btn-secondary text-xs flex-shrink-0">
          <RefreshCw size={13} /> Retry
        </button>
      )}
    </div>
  )
}
