import { Github, BookOpen, MessageSquare, Heart, ArrowUpRight } from 'lucide-react'

const links = [
  {
    icon: <Github className="w-6 h-6" />,
    title: 'GitHub Repository',
    desc: 'Browse source code, open issues, and submit pull requests.',
    href: 'https://github.com/falcondb-lab/falcondb',
    color: 'group-hover:text-white',
    bg: 'group-hover:bg-white/10',
  },
  {
    icon: <BookOpen className="w-6 h-6" />,
    title: 'Documentation',
    desc: 'Architecture docs, protocol compatibility, consistency evidence, and benchmarks.',
    href: 'https://github.com/falcondb-lab/falcondb/blob/main/docs/README.md',
    color: 'group-hover:text-falcon-400',
    bg: 'group-hover:bg-falcon-400/10',
  },
  {
    icon: <MessageSquare className="w-6 h-6" />,
    title: 'Discussions',
    desc: 'Ask questions, share ideas, and connect with other FalconDB users.',
    href: 'https://github.com/falcondb-lab/falcondb/discussions',
    color: 'group-hover:text-cyan-400',
    bg: 'group-hover:bg-cyan-400/10',
  },
  {
    icon: <Heart className="w-6 h-6" />,
    title: 'Contributing',
    desc: 'Read the contributing guide and help shape the future of FalconDB.',
    href: 'https://github.com/falcondb-lab/falcondb/blob/main/CONTRIBUTING.md',
    color: 'group-hover:text-rose-400',
    bg: 'group-hover:bg-rose-400/10',
  },
]

const roadmapItems = [
  { milestone: 'M1', status: 'done', label: 'Core OLTP + Replication + Benchmarks' },
  { milestone: 'M2', status: 'done', label: 'gRPC WAL Streaming + Multi-Node Deploy' },
  { milestone: 'P2', status: 'planned', label: 'Raft Consensus + Disk Rowstore + Encryption' },
  { milestone: 'P2', status: 'planned', label: 'Online DDL + PITR + Resource Isolation' },
]

export default function Community() {
  return (
    <section id="community" className="relative py-24 lg:py-32">
      <div className="absolute top-0 left-0 right-0 h-px bg-gradient-to-r from-transparent via-white/[0.06] to-transparent" />

      <div className="relative max-w-7xl mx-auto px-4 sm:px-6 lg:px-8">
        <div className="text-center mb-16">
          <p className="text-falcon-400 font-semibold text-sm tracking-wider uppercase mb-3">
            Open Source
          </p>
          <h2 className="text-3xl sm:text-4xl lg:text-5xl font-bold text-white mb-4">
            Join the <span className="gradient-text">Community</span>
          </h2>
          <p className="max-w-2xl mx-auto text-gray-400 text-lg">
            FalconDB is Apache-2.0 licensed and built in the open. Contributions, feedback, and
            collaboration are welcome.
          </p>
        </div>

        {/* Community links */}
        <div className="grid sm:grid-cols-2 lg:grid-cols-4 gap-5 mb-16">
          {links.map((link, i) => (
            <a
              key={i}
              href={link.href}
              target="_blank"
              rel="noopener noreferrer"
              className="glass-card p-6 group hover:bg-white/[0.06] transition-all duration-300"
            >
              <div className={`inline-flex items-center justify-center w-12 h-12 rounded-xl bg-white/[0.06] text-gray-400 ${link.color} ${link.bg} transition-all mb-4`}>
                {link.icon}
              </div>
              <h3 className="text-white font-semibold mb-1 flex items-center gap-1.5">
                {link.title}
                <ArrowUpRight className="w-3.5 h-3.5 text-gray-500 group-hover:text-falcon-400 transition-colors" />
              </h3>
              <p className="text-gray-400 text-sm">{link.desc}</p>
            </a>
          ))}
        </div>

        {/* Roadmap */}
        <div className="glass-card p-6 lg:p-8 max-w-3xl mx-auto">
          <h3 className="text-white font-bold text-xl mb-2">Roadmap</h3>
          <p className="text-gray-400 text-sm mb-6">
            Transparent development milestones.{' '}
            <a
              href="https://github.com/falcondb-lab/falcondb/blob/main/docs/roadmap.md"
              target="_blank"
              rel="noopener noreferrer"
              className="text-falcon-400 hover:underline"
            >
              See full roadmap →
            </a>
          </p>
          <div className="space-y-3">
            {roadmapItems.map((item, i) => (
              <div
                key={i}
                className="flex items-center gap-4 p-3 rounded-lg bg-white/[0.03] border border-white/[0.05]"
              >
                <span
                  className={`shrink-0 px-2.5 py-1 rounded-md text-xs font-bold font-mono ${
                    item.status === 'done'
                      ? 'bg-emerald-500/15 text-emerald-400 border border-emerald-500/20'
                      : 'bg-amber-500/15 text-amber-400 border border-amber-500/20'
                  }`}
                >
                  {item.milestone}
                </span>
                <span className="text-gray-300 text-sm">{item.label}</span>
                <span
                  className={`ml-auto text-xs font-medium ${
                    item.status === 'done' ? 'text-emerald-400' : 'text-gray-500'
                  }`}
                >
                  {item.status === 'done' ? 'Shipped' : 'Planned'}
                </span>
              </div>
            ))}
          </div>
        </div>
      </div>
    </section>
  )
}
