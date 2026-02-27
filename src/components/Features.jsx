import {
  Zap,
  Shield,
  Database,
  GitBranch,
  Activity,
  Server,
  Lock,
  Gauge,
  Layers,
  RefreshCw,
  Eye,
  Binary,
} from 'lucide-react'

const features = [
  {
    icon: <Zap className="w-5 h-5" />,
    color: 'text-amber-400 bg-amber-400/10',
    title: 'Sub-Millisecond Latency',
    desc: 'Single-shard fast-path commits bypass 2PC entirely. In-memory MVCC with zero disk I/O on the hot path.',
  },
  {
    icon: <Shield className="w-5 h-5" />,
    color: 'text-emerald-400 bg-emerald-400/10',
    title: 'Deterministic Commit Guarantee',
    desc: 'If FalconDB returns "committed", that transaction survives any single-node crash, failover, and recovery — zero exceptions.',
  },
  {
    icon: <Database className="w-5 h-5" />,
    color: 'text-falcon-400 bg-falcon-400/10',
    title: 'PG Wire Protocol',
    desc: 'Connect with psql, pgbench, or any PostgreSQL driver. Drop-in compatible for standard OLTP workloads.',
  },
  {
    icon: <GitBranch className="w-5 h-5" />,
    color: 'text-purple-400 bg-purple-400/10',
    title: 'Distributed Architecture',
    desc: 'Shard-based routing with epoch-based cluster management. Scale out linearly with shard count.',
  },
  {
    icon: <Lock className="w-5 h-5" />,
    color: 'text-rose-400 bg-rose-400/10',
    title: 'MVCC + OCC Isolation',
    desc: 'Snapshot Isolation with optimistic concurrency control. CI-verified ACID guarantees with abort rate < 1%.',
  },
  {
    icon: <RefreshCw className="w-5 h-5" />,
    color: 'text-cyan-400 bg-cyan-400/10',
    title: 'WAL-Based Replication',
    desc: 'gRPC WAL streaming for primary–replica replication. Automatic promote and failover with zero data loss.',
  },
  {
    icon: <Layers className="w-5 h-5" />,
    color: 'text-orange-400 bg-orange-400/10',
    title: 'Dual Storage Engines',
    desc: 'In-memory Rowstore for maximum throughput, LSM disk-backed engine for larger-than-memory datasets.',
  },
  {
    icon: <Eye className="w-5 h-5" />,
    color: 'text-sky-400 bg-sky-400/10',
    title: '50+ SHOW Commands',
    desc: 'Deep observability with transaction stats, GC metrics, replication lag, and Prometheus-compatible endpoints.',
  },
  {
    icon: <Binary className="w-5 h-5" />,
    color: 'text-lime-400 bg-lime-400/10',
    title: 'Built in Rust',
    desc: 'Memory-safe, zero-cost abstractions, fearless concurrency. Minimal footprint with maximum performance.',
  },
]

export default function Features() {
  return (
    <section id="features" className="relative py-24 lg:py-32">
      <div className="absolute inset-0">
        <div className="absolute top-0 left-0 right-0 h-px bg-gradient-to-r from-transparent via-white/[0.06] to-transparent" />
      </div>

      <div className="relative max-w-7xl mx-auto px-4 sm:px-6 lg:px-8">
        {/* Section header */}
        <div className="text-center mb-16">
          <p className="text-falcon-400 font-semibold text-sm tracking-wider uppercase mb-3">
            Core Capabilities
          </p>
          <h2 className="text-3xl sm:text-4xl lg:text-5xl font-bold text-white mb-4">
            Engineered for <span className="gradient-text">OLTP Performance</span>
          </h2>
          <p className="max-w-2xl mx-auto text-gray-400 text-lg">
            Every component is purpose-built for transactional workloads — from the memory-first storage
            engine to the deterministic commit protocol.
          </p>
        </div>

        {/* Feature grid */}
        <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-5">
          {features.map((f, i) => (
            <div
              key={i}
              className="glass-card p-6 hover:bg-white/[0.06] transition-all duration-300 group"
            >
              <div className={`inline-flex items-center justify-center w-10 h-10 rounded-xl ${f.color} mb-4`}>
                {f.icon}
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
