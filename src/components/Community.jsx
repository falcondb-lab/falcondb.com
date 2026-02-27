import { Github, BookOpen, MessageSquare, Heart, ArrowUpRight } from 'lucide-react'
import { useI18n } from '../i18n'

// order must match t.community.links
const linkStyles = [
  { icon: <Github className="w-6 h-6" />, href: 'https://github.com/falcondb-lab/falcondb', color: 'group-hover:text-white', bg: 'group-hover:bg-white/10' },
  { icon: <BookOpen className="w-6 h-6" />, href: 'https://github.com/falcondb-lab/falcondb/blob/main/docs/README.md', color: 'group-hover:text-falcon-400', bg: 'group-hover:bg-falcon-400/10' },
  { icon: <MessageSquare className="w-6 h-6" />, href: 'https://github.com/falcondb-lab/falcondb/discussions', color: 'group-hover:text-cyan-400', bg: 'group-hover:bg-cyan-400/10' },
  { icon: <Heart className="w-6 h-6" />, href: 'https://github.com/falcondb-lab/falcondb/blob/main/CONTRIBUTING.md', color: 'group-hover:text-rose-400', bg: 'group-hover:bg-rose-400/10' },
]

const roadmapMeta = [
  { milestone: 'M1', status: 'done' },
  { milestone: 'M2', status: 'done' },
  { milestone: 'P2', status: 'planned' },
  { milestone: 'P2', status: 'planned' },
]

export default function Community() {
  const { t } = useI18n()

  return (
    <section id="community" className="relative py-24 lg:py-32">
      <div className="absolute top-0 left-0 right-0 h-px bg-gradient-to-r from-transparent via-white/[0.06] to-transparent" />

      <div className="relative max-w-7xl mx-auto px-4 sm:px-6 lg:px-8">
        <div className="text-center mb-16">
          <p className="text-falcon-400 font-semibold text-sm tracking-wider uppercase mb-3">
            {t.community.sectionLabel}
          </p>
          <h2 className="text-3xl sm:text-4xl lg:text-5xl font-bold text-white mb-4">
            {t.community.title} <span className="gradient-text">{t.community.titleHighlight}</span>
          </h2>
          <p className="max-w-2xl mx-auto text-gray-400 text-lg">
            {t.community.subtitle}
          </p>
        </div>

        <div className="grid sm:grid-cols-2 lg:grid-cols-4 gap-5 mb-16">
          {t.community.links.map((link, i) => (
            <a
              key={i}
              href={linkStyles[i].href}
              target="_blank"
              rel="noopener noreferrer"
              className="glass-card p-6 group hover:bg-white/[0.06] transition-all duration-300"
            >
              <div className={`inline-flex items-center justify-center w-12 h-12 rounded-xl bg-white/[0.06] text-gray-400 ${linkStyles[i].color} ${linkStyles[i].bg} transition-all mb-4`}>
                {linkStyles[i].icon}
              </div>
              <h3 className="text-white font-semibold mb-1 flex items-center gap-1.5">
                {link.title}
                <ArrowUpRight className="w-3.5 h-3.5 text-gray-500 group-hover:text-falcon-400 transition-colors" />
              </h3>
              <p className="text-gray-400 text-sm">{link.desc}</p>
            </a>
          ))}
        </div>

        <div className="glass-card p-6 lg:p-8 max-w-3xl mx-auto">
          <h3 className="text-white font-bold text-xl mb-2">{t.community.roadmapTitle}</h3>
          <p className="text-gray-400 text-sm mb-6">
            {t.community.roadmapSubtitle}{' '}
            <a
              href="https://github.com/falcondb-lab/falcondb/blob/main/docs/roadmap.md"
              target="_blank"
              rel="noopener noreferrer"
              className="text-falcon-400 hover:underline"
            >
              {t.community.roadmapLink}
            </a>
          </p>
          <div className="space-y-3">
            {t.community.roadmapItems.map((item, i) => (
              <div
                key={i}
                className="flex items-center gap-4 p-3 rounded-lg bg-white/[0.03] border border-white/[0.05]"
              >
                <span
                  className={`shrink-0 px-2.5 py-1 rounded-md text-xs font-bold font-mono ${
                    roadmapMeta[i].status === 'done'
                      ? 'bg-emerald-500/15 text-emerald-400 border border-emerald-500/20'
                      : 'bg-amber-500/15 text-amber-400 border border-amber-500/20'
                  }`}
                >
                  {roadmapMeta[i].milestone}
                </span>
                <span className="text-gray-300 text-sm">{item.label}</span>
                <span
                  className={`ml-auto text-xs font-medium ${
                    roadmapMeta[i].status === 'done' ? 'text-emerald-400' : 'text-gray-500'
                  }`}
                >
                  {roadmapMeta[i].status === 'done' ? t.community.shipped : t.community.planned}
                </span>
              </div>
            ))}
          </div>
        </div>
      </div>
    </section>
  )
}
