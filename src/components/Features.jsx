import {
  Zap,
  Shield,
  Database,
  GitBranch,
  Lock,
  Layers,
  RefreshCw,
  Eye,
  Binary,
} from 'lucide-react'
import { useI18n } from '../i18n'

const featureIcons = [
  { icon: <Zap className="w-5 h-5" />, color: 'text-amber-400 bg-amber-400/10' },
  { icon: <Shield className="w-5 h-5" />, color: 'text-emerald-400 bg-emerald-400/10' },
  { icon: <Database className="w-5 h-5" />, color: 'text-falcon-400 bg-falcon-400/10' },
  { icon: <GitBranch className="w-5 h-5" />, color: 'text-purple-400 bg-purple-400/10' },
  { icon: <Lock className="w-5 h-5" />, color: 'text-rose-400 bg-rose-400/10' },
  { icon: <RefreshCw className="w-5 h-5" />, color: 'text-cyan-400 bg-cyan-400/10' },
  { icon: <Layers className="w-5 h-5" />, color: 'text-orange-400 bg-orange-400/10' },
  { icon: <Eye className="w-5 h-5" />, color: 'text-sky-400 bg-sky-400/10' },
  { icon: <Binary className="w-5 h-5" />, color: 'text-lime-400 bg-lime-400/10' },
]

export default function Features() {
  const { t } = useI18n()

  return (
    <section id="features" className="relative py-24 lg:py-32">
      <div className="absolute inset-0">
        <div className="absolute top-0 left-0 right-0 h-px bg-gradient-to-r from-transparent via-white/[0.06] to-transparent" />
      </div>

      <div className="relative max-w-7xl mx-auto px-4 sm:px-6 lg:px-8">
        {/* Section header */}
        <div className="text-center mb-16">
          <p className="text-falcon-400 font-semibold text-sm tracking-wider uppercase mb-3">
            {t.features.sectionLabel}
          </p>
          <h2 className="text-3xl sm:text-4xl lg:text-5xl font-bold text-white mb-4">
            {t.features.title} <span className="gradient-text">{t.features.titleHighlight}</span>
          </h2>
          <p className="max-w-2xl mx-auto text-gray-400 text-lg">
            {t.features.subtitle}
          </p>
        </div>

        {/* Feature grid */}
        <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-5">
          {t.features.items.map((f, i) => (
            <div
              key={i}
              className="glass-card p-6 hover:bg-white/[0.06] transition-all duration-300 group"
            >
              <div className={`inline-flex items-center justify-center w-10 h-10 rounded-xl ${featureIcons[i].color} mb-4`}>
                {featureIcons[i].icon}
              </div>
              <h3 className="text-white font-semibold text-lg mb-2 group-hover:text-falcon-400 transition-colors">
                {f.title}
              </h3>
              <p className="text-gray-400 text-sm leading-relaxed">{f.desc}</p>
            </div>
          ))}
        </div>
      </div>
    </section>
  )
}
