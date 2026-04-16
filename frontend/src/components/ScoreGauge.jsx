/**
 * SVG semicircle gauge for quality score display.
 */
const GRADE_COLOR = {
  A:    '#22c55e',
  B:    '#3b82f6',
  C:    '#eab308',
  D:    '#f97316',
  F:    '#ef4444',
  'F-': '#991b1b',
}

function scoreColor(score) {
  if (score >= 90) return GRADE_COLOR.A
  if (score >= 75) return GRADE_COLOR.B
  if (score >= 60) return GRADE_COLOR.C
  if (score >= 40) return GRADE_COLOR.D
  if (score >= 20) return GRADE_COLOR.F
  return GRADE_COLOR['F-']
}

export default function ScoreGauge({ score, grade, size = 200 }) {
  const cx = size / 2
  const cy = size * 0.55
  const r = size * 0.38
  const sw = size * 0.07
  const halfCirc = Math.PI * r
  const progress = Math.max(0, Math.min(score / 100, 1)) * halfCirc
  const color = scoreColor(score)
  const bgArc = `M ${cx - r} ${cy} A ${r} ${r} 0 0 1 ${cx + r} ${cy}`

  return (
    <div className="flex flex-col items-center">
      <svg width={size} height={size * 0.62} viewBox={`0 0 ${size} ${size * 0.62}`}>
        {/* Background track */}
        <path d={bgArc} fill="none" stroke="#e2e8f0" strokeWidth={sw} strokeLinecap="round" />
        {/* Progress arc */}
        <path
          d={bgArc}
          fill="none"
          stroke={color}
          strokeWidth={sw}
          strokeLinecap="round"
          strokeDasharray={`${progress} ${halfCirc}`}
          strokeDashoffset={0}
          style={{ transition: 'stroke-dasharray 0.8s ease' }}
        />
        {/* Score value */}
        <text
          x={cx}
          y={cy - r * 0.1}
          textAnchor="middle"
          fontSize={size * 0.18}
          fontWeight="700"
          fill={color}
          fontFamily="Inter, sans-serif"
        >
          {Math.round(score)}
        </text>
        <text
          x={cx}
          y={cy + size * 0.07}
          textAnchor="middle"
          fontSize={size * 0.065}
          fill="#94a3b8"
          fontFamily="Inter, sans-serif"
        >
          / 100
        </text>
      </svg>
      {grade && (
        <div
          className={`mt-1 px-3 py-0.5 rounded-full text-xs font-bold border ${grade === 'F-' ? 'animate-pulse ring-2 ring-red-400' : ''}`}
          style={{ color, borderColor: color + '40', backgroundColor: color + '15' }}
        >
          Grade&nbsp;{grade}
        </div>
      )}
    </div>
  )
}
