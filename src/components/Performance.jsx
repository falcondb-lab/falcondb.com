import { TrendingUp, Timer, BarChart3, ArrowUpRight } from 'lucide-react'
import { useI18n } from '../i18n'

const benchmarkStyles = [
  { icon: <TrendingUp className="w-5 h-5" />, color: 'text-emerald-400', bgColor: 'bg-emerald-400/10' },
  { icon: <Timer className="w-5 h-5" />, color: 'text-amber-400', bgColor: 'bg-amber-400/10' },
  { icon: <BarChart3 className="w-5 h-5" />, color: 'text-falcon-400', bgColor: 'bg-falcon-400/10' },
]

// true = feature supported
const comparisonData = [
  [true,  true,  false, false],
  [true,  false, true,  true],
  [true,  false, true,  false],
  [true,  false, false, false],
  [true,  true,  true,  true],
  [true,  false, false, false],
  [true,  true,  true,  false],
]

export default function Performance() {
  const { t } = useI18n()

  return (
    <section id="performance" className="relative py-24 lg:py-32">
      <div className="absolute top-0 left-0 right-0 h-px bg-gradient-to-r from-transparent via-white/[0.06] to-transparent" />
      <div className="absolute top-1/2 right-0 w-[500px] h-[500px] bg-falcon-600/5 rounded-full blur-[120px] -translate-y-1/2" />

      <div className="relative max-w-7xl mx-auto px-4 sm:px-6 lg:px-8">
        <div className="text-center mb-16">
          <p className="text-falcon-400 font-semibold text-sm tracking-wider uppercase mb-3">
            {t.perf.sectionLabel}
          </p>
          <h2 className="text-3xl sm:text-4xl lg:text-5xl font-bold text-white mb-4">
            {t.perf.title} <span className="gradient-text">{t.perf.titleHighlight}</span>
          </h2>
          <p className="max-w-2xl mx-auto text-gray-400 text-lg">
            {t.perf.subtitle}
          </p>
        </div>

        <div className="grid md:grid-cols-3 gap-5 mb-16">
          {t.perf.benchmarks.map((b, i) => (
            <div key={i} className="glass-card p-6 group hover:bg-white/[0.06] transition-all duration-300">
              <div className={`inline-flex items-center justify-center w-10 h-10 rounded-xl ${benchmarkStyles[i].bgColor} ${benchmarkStyles[i].color} mb-4`}>
                {benchmarkStyles[i].icon}
              </div>
              <h3 className="text-white font-semibold text-lg mb-1">{b.title}</h3>
              <p className="text-gray-400 text-sm leading-relaxed mb-4">{b.description}</p>
              <div className="pt-4 border-t border-white/[0.06]">
                <p className={`text-3xl font-bold ${benchmarkStyles[i].color}`}>{b.metric}</p>
                <p className="text-gray-500 text-xs mt-1">{b.metricLabel}</p>
              </div>
            </div>
          ))}
        </div>

        <div className="glass-card overflow-hidden">
          <div className="px-6 py-5 border-b border-white/[0.06]">
            <h3 className="text-white font-bold text-lg">{t.perf.comparisonTitle}</h3>
            <p className="text-gray-400 text-sm mt-1">{t.perf.comparisonSubtitle}</p>
          </div>
          <div className="overflow-x-auto">
            <table className="w-full text-sm">
              <thead>
                <tr className="border-b border-white/[0.06]">
                  <th className="text-left text-gray-400 font-medium px-6 py-3">Feature</th>
                  <th className="text-center text-falcon-400 font-semibold px-4 py-3">FalconDB</th>
                  <th className="text-center text-gray-400 font-medium px-4 py-3">PostgreSQL</th>
                  <th className="text-center text-gray-400 font-medium px-4 py-3">VoltDB</th>
                  <th className="text-center text-gray-400 font-medium px-4 py-3">SingleStore</th>
                </tr>
              </thead>
              <tbody>
                {t.perf.tableFeatures.map((feature, i) => (
                  <tr key={i} className="border-b border-white/[0.04] hover:bg-white/[0.02] transition-colors">
                    <td className="text-gray-300 px-6 py-3">{feature}</td>
                    {comparisonData[i].map((val, j) => (
                      <td key={j} className="text-center px-4 py-3">
                        <Check value={val} highlight={j === 0} />
                      </td>
                    ))}
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
          <div className="px-6 py-4 border-t border-white/[0.06]">
            <a
              href="https://github.com/falcondb-lab/falcondb/blob/main/benchmarks/README.md"
              target="_blank"
              rel="noopener noreferrer"
              className="inline-flex items-center gap-1.5 text-falcon-400 hover:text-falcon-300 text-sm font-medium transition-colors"
            >
              {t.perf.viewBenchmarks}
              <ArrowUpRight className="w-3.5 h-3.5" />
            </a>
          </div>
        </div>
      </div>
    </section>
  )
}

function Check({ value, highlight = false }) {
  if (value) {
    return (
      <span className={`inline-block w-5 h-5 rounded-full ${highlight ? 'bg-falcon-500/20 text-falcon-400' : 'bg-emerald-500/20 text-emerald-400'} text-xs leading-5`}>
        ✓
      </span>
    )
  }
  return <span className="inline-block text-gray-600">—</span>
}
