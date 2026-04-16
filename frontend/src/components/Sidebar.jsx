import { NavLink } from 'react-router-dom'
import {
  LayoutDashboard,
  Database,
  UploadCloud,
  Bell,
  BarChart3,
  Zap,
} from 'lucide-react'

const navItems = [
  { to: '/dashboard', icon: LayoutDashboard, label: 'Dashboard' },
  { to: '/datasets',  icon: Database,        label: 'Datasets'  },
  { to: '/upload',    icon: UploadCloud,     label: 'Upload'    },
  { to: '/alerts',    icon: Bell,            label: 'Alerts'    },
]

export default function Sidebar() {
  return (
    <aside className="w-60 flex-shrink-0 bg-slate-900 flex flex-col h-full">

      {/* ── Brand ── */}
      <div className="px-5 pt-6 pb-5 border-b border-slate-800">
        <div className="flex items-center gap-3">
          <div className="w-8 h-8 bg-gradient-to-br from-blue-500 to-violet-600 rounded-lg flex items-center justify-center flex-shrink-0">
            <BarChart3 size={16} className="text-white" />
          </div>
          <div>
            <span className="font-bold text-white tracking-tight">DataQA</span>
            <p className="text-xs text-slate-500 mt-0.5">Quality Analyzer</p>
          </div>
        </div>
      </div>

      {/* ── Navigation ── */}
      <nav className="flex-1 px-3 py-4 space-y-1">
        <p className="px-2 mb-3 text-xs font-semibold text-slate-600 uppercase tracking-wider">
          Main
        </p>
        {navItems.map(({ to, icon: Icon, label }) => (
          <NavLink
            key={to}
            to={to}
            className={({ isActive }) =>
              `flex items-center gap-3 px-3 py-2.5 rounded-lg text-sm font-medium transition-colors duration-150 ${
                isActive
                  ? 'bg-blue-500/20 text-blue-400 border border-blue-500/30'
                  : 'text-slate-400 hover:bg-slate-800 hover:text-slate-100 border border-transparent'
              }`
            }
          >
            <Icon size={17} />
            {label}
          </NavLink>
        ))}
      </nav>

      {/* ── Footer ── */}
      <div className="px-5 py-4 border-t border-slate-800">
        <div className="flex items-center gap-2">
          <span className="w-2 h-2 rounded-full bg-emerald-400 flex-shrink-0 animate-pulse" />
          <span className="text-xs text-slate-500">Backend connected</span>
        </div>
        <p className="mt-1.5 text-[11px] text-slate-600">v1.0.0  ·  FastAPI + MongoDB</p>
      </div>
    </aside>
  )
}
