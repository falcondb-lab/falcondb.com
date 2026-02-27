import { Github, ExternalLink } from 'lucide-react'

const footerLinks = [
  {
    title: 'Project',
    links: [
      { label: 'GitHub', href: 'https://github.com/falcondb-lab/falcondb', external: true },
      { label: 'Releases', href: 'https://github.com/falcondb-lab/falcondb/releases', external: true },
      { label: 'License (Apache-2.0)', href: 'https://github.com/falcondb-lab/falcondb/blob/main/LICENSE', external: true },
      { label: 'Security Policy', href: 'https://github.com/falcondb-lab/falcondb/security', external: true },
    ],
  },
  {
    title: 'Documentation',
    links: [
      { label: 'Getting Started', href: '#quickstart' },
      { label: 'Architecture', href: '#architecture' },
      { label: 'Protocol Compatibility', href: 'https://github.com/falcondb-lab/falcondb/blob/main/docs/protocol_compatibility.md', external: true },
      { label: 'Benchmark Matrix', href: 'https://github.com/falcondb-lab/falcondb/blob/main/benchmarks/README.md', external: true },
    ],
  },
  {
    title: 'Resources',
    links: [
      { label: 'Roadmap', href: 'https://github.com/falcondb-lab/falcondb/blob/main/docs/roadmap.md', external: true },
      { label: 'Contributing', href: 'https://github.com/falcondb-lab/falcondb/blob/main/CONTRIBUTING.md', external: true },
      { label: 'Consistency Evidence', href: 'https://github.com/falcondb-lab/falcondb/blob/main/docs/consistency_evidence_map.md', external: true },
      { label: 'RPO / RTO', href: 'https://github.com/falcondb-lab/falcondb/blob/main/docs/rpo_rto.md', external: true },
    ],
  },
]

export default function Footer() {
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
              PG-compatible, distributed, memory-first OLTP database with deterministic transaction semantics.
            </p>
            <a
              href="https://github.com/falcondb-lab/falcondb"
              target="_blank"
              rel="noopener noreferrer"
              className="inline-flex items-center gap-2 text-gray-400 hover:text-white transition-colors text-sm"
            >
              <Github className="w-4 h-4" />
              Star on GitHub
            </a>
          </div>

          {/* Link columns */}
          {footerLinks.map((col, i) => (
            <div key={i}>
              <h4 className="text-white font-semibold text-sm mb-4">{col.title}</h4>
              <ul className="space-y-2.5">
                {col.links.map((link, j) => (
                  <li key={j}>
                    <a
                      href={link.href}
                      target={link.external ? '_blank' : undefined}
                      rel={link.external ? 'noopener noreferrer' : undefined}
                      className="inline-flex items-center gap-1 text-gray-400 hover:text-white text-sm transition-colors"
                    >
                      {link.label}
                      {link.external && <ExternalLink className="w-3 h-3" />}
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
            &copy; {new Date().getFullYear()} FalconDB Contributors. Apache-2.0 License.
          </p>
          <p className="text-gray-600 text-xs">
            Built with React &middot; Vite &middot; Tailwind CSS
          </p>
        </div>
      </div>
    </footer>
  )
}
