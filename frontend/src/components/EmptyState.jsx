import { Inbox } from 'lucide-react'

export default function EmptyState({ title = 'No data', description, action }) {
  return (
    <div className="flex flex-col items-center justify-center py-16 text-center gap-3 animate-fade-in">
      <div className="w-14 h-14 rounded-full bg-slate-100 flex items-center justify-center">
        <Inbox size={26} className="text-slate-400" />
      </div>
      <div>
        <p className="text-slate-700 font-semibold">{title}</p>
        {description && <p className="text-sm text-slate-400 mt-1 max-w-xs">{description}</p>}
      </div>
      {action && <div className="mt-2">{action}</div>}
    </div>
  )
}
