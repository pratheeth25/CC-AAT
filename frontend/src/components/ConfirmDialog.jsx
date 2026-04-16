import { useEffect, useRef } from 'react'
import { AlertTriangle, X } from 'lucide-react'

/**
 * Modal confirmation dialog.
 *
 * Props:
 *   open       : boolean
 *   title      : string
 *   message    : string
 *   confirmLabel  : string (default "Confirm")
 *   cancelLabel   : string (default "Cancel")
 *   danger     : boolean — style confirm button as destructive (default false)
 *   loading    : boolean — show spinner on confirm button
 *   onConfirm  : () => void
 *   onCancel   : () => void
 */
export default function ConfirmDialog({
  open,
  title = 'Are you sure?',
  message = '',
  confirmLabel = 'Confirm',
  cancelLabel = 'Cancel',
  danger = false,
  loading = false,
  onConfirm,
  onCancel,
}) {
  const confirmRef = useRef(null)

  useEffect(() => {
    if (open) confirmRef.current?.focus()
  }, [open])

  useEffect(() => {
    const handleKey = (e) => {
      if (!open) return
      if (e.key === 'Escape') onCancel?.()
    }
    document.addEventListener('keydown', handleKey)
    return () => document.removeEventListener('keydown', handleKey)
  }, [open, onCancel])

  if (!open) return null

  return (
    <div
      className="fixed inset-0 z-50 flex items-center justify-center bg-black/40 backdrop-blur-sm animate-fade-in"
      onClick={(e) => { if (e.target === e.currentTarget) onCancel?.() }}
    >
      <div className="bg-white rounded-2xl shadow-2xl w-full max-w-md mx-4 overflow-hidden">
        {/* Header */}
        <div className={`px-6 pt-6 pb-4 flex items-start gap-4 ${danger ? 'bg-red-50' : 'bg-slate-50'}`}>
          <div className={`w-10 h-10 rounded-full flex items-center justify-center flex-shrink-0 ${danger ? 'bg-red-100' : 'bg-slate-200'}`}>
            <AlertTriangle size={18} className={danger ? 'text-red-600' : 'text-slate-600'} />
          </div>
          <div className="flex-1 min-w-0">
            <h2 className="font-semibold text-slate-900 text-base">{title}</h2>
            {message && <p className="text-sm text-slate-500 mt-1 leading-relaxed">{message}</p>}
          </div>
          <button onClick={onCancel} className="text-slate-400 hover:text-slate-600 flex-shrink-0">
            <X size={18} />
          </button>
        </div>

        {/* Actions */}
        <div className="px-6 py-4 flex items-center justify-end gap-3 bg-white">
          <button onClick={onCancel} disabled={loading} className="btn-secondary">
            {cancelLabel}
          </button>
          <button
            ref={confirmRef}
            onClick={onConfirm}
            disabled={loading}
            className={`px-4 py-2 text-sm font-semibold rounded-lg transition-colors flex items-center gap-2 ${
              danger
                ? 'bg-red-600 hover:bg-red-700 text-white disabled:opacity-50'
                : 'btn-primary'
            }`}
          >
            {loading && (
              <span className="w-4 h-4 border-2 border-white/40 border-t-white rounded-full animate-spin" />
            )}
            {confirmLabel}
          </button>
        </div>
      </div>
    </div>
  )
}
