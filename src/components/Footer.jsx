import { Github, ExternalLink } from 'lucide-react'
import { useI18n } from '../i18n'

const footerHrefs = [
  [
    { href: 'https://github.com/falcondb-lab/falcondb', external: true },
    { href: 'https://github.com/falcondb-lab/falcondb/releases', external: true },
    { href: 'https://github.com/falcondb-lab/falcondb/blob/main/LICENSE', external: true },
    { href: 'https://github.com/falcondb-lab/falcondb/security', external: true },
  ],
  [
    { href: '#quickstart', external: false },
    { href: '#architecture', external: false },
    { href: 'https://github.com/falcondb-lab/falcondb/blob/main/docs/protocol_compatibility.md', external: true },
    { href: 'https://github.com/falcondb-lab/falcondb/blob/main/benchmarks/README.md', external: true },
  ],
  [
    { href: 'https://github.com/falcondb-lab/falcondb/blob/main/docs/roadmap.md', external: true },
    { href: 'https://github.com/falcondb-lab/falcondb/blob/main/CONTRIBUTING.md', external: true },
    { href: 'https://github.com/falcondb-lab/falcondb/blob/main/docs/consistency_evidence_map.md', external: true },
    { href: 'https://github.com/falcondb-lab/falcondb/blob/main/docs/rpo_rto.md', external: true },
  ],
]

export default function Footer() {
  const { t } = useI18n()

  return (
    <footer className="relative border-t border-white/[0.06]">
      <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-16">
        <div className="grid grid-cols-2 md:grid-cols-4 gap-8">
          {/* Brand */}
          <div className="col-span-2 md:col-span-1">
            <a href="#" className="flex items-center gap-2.5 mb-4">
              <div className="w-8 h-8 rounded-lg bg-gradient-to-br from-falcon-500 to-cyan-400 flex items-center justify-center text-white font-bold text-sm">
                F
              </div>
              <span className="text-lg font-bold text-white">FalconDB</span>
            </a>
            <p className="text-gray-500 text-sm leading-relaxed mb-4">
              {t.footer.description}
            </p>
            <a
              href="https://github.com/falcondb-lab/falcondb"
              target="_blank"
              rel="noopener noreferrer"
              className="inline-flex items-center gap-2 text-gray-400 hover:text-white transition-colors text-sm"
            >
              <Github className="w-4 h-4" />
              {t.footer.starOnGitHub}
            </a>
          </div>

          {/* Link columns */}
          {t.footer.columns.map((col, i) => (
            <div key={i}>
              <h4 className="text-white font-semibold text-sm mb-4">{col.title}</h4>
              <ul className="space-y-2.5">
                {col.links.map((label, j) => (
                  <li key={j}>
                    <a
                      href={footerHrefs[i][j].href}
                      target={footerHrefs[i][j].external ? '_blank' : undefined}
                      rel={footerHrefs[i][j].external ? 'noopener noreferrer' : undefined}
                      className="inline-flex items-center gap-1 text-gray-400 hover:text-white text-sm transition-colors"
                    >
                      {label}
                      {footerHrefs[i][j].external && <ExternalLink className="w-3 h-3" />}
                    </a>
                  </li>
                ))}
              </ul>
            </div>
          ))}
        </div>

        {/* Bottom bar */}
        <div className="mt-12 pt-8 border-t border-white/[0.06] flex flex-col sm:flex-row items-center justify-between gap-4">
          <p className="text-gray-500 text-sm">
            {t.footer.copyright.replace('{year}', new Date().getFullYear())}
          </p>
          <p className="text-gray-600 text-xs">
            {t.footer.builtWith}
          </p>
        </div>
      </div>
    </footer>
  )
}
