export default function StatCard({ title, value, subtitle, icon: Icon, color = 'blue', trend }) {
  const colorMap = {
    blue:    { bg: 'bg-blue-50',    icon: 'bg-blue-100 text-blue-600',    value: 'text-blue-700'    },
    green:   { bg: 'bg-emerald-50', icon: 'bg-emerald-100 text-emerald-600', value: 'text-emerald-700' },
    red:     { bg: 'bg-red-50',     icon: 'bg-red-100 text-red-600',       value: 'text-red-700'     },
    orange:  { bg: 'bg-orange-50',  icon: 'bg-orange-100 text-orange-600', value: 'text-orange-700'  },
    purple:  { bg: 'bg-violet-50',  icon: 'bg-violet-100 text-violet-600', value: 'text-violet-700'  },
    slate:   { bg: 'bg-slate-50',   icon: 'bg-slate-100 text-slate-600',   value: 'text-slate-700'   },
  }
  const c = colorMap[color] || colorMap.blue

  return (
    <div className={`card p-5 flex items-start gap-4 animate-fade-in`}>
      {Icon && (
        <div className={`w-11 h-11 rounded-xl flex items-center justify-center flex-shrink-0 ${c.icon}`}>
          <Icon size={20} />
        </div>
      )}
      <div className="min-w-0 flex-1">
        <p className="text-xs font-semibold text-slate-500 uppercase tracking-wider truncate">{title}</p>
        <p className={`text-2xl font-bold mt-0.5 ${c.value}`}>{value ?? '—'}</p>
        {subtitle && <p className="text-xs text-slate-400 mt-0.5 truncate">{subtitle}</p>}
        {trend && (
          <p className={`text-xs font-medium mt-1 ${trend.positive ? 'text-emerald-600' : 'text-red-500'}`}>
            {trend.positive ? '↑' : '↓'} {trend.label}
          </p>
        )}
      </div>
    </div>
  )
}
