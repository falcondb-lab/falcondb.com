import { TrendingUp, Timer, BarChart3, ArrowUpRight } from 'lucide-react'

const benchmarks = [
  {
    title: 'Bulk Insert (1M Rows)',
    description: 'In-memory MVCC with zero disk I/O dramatically outperforms PostgreSQL on write throughput.',
    metric: '~10x',
    metricLabel: 'faster INSERT vs PG 16',
    icon: <TrendingUp className="w-5 h-5" />,
    color: 'text-emerald-400',
    bgColor: 'bg-emerald-400/10',
  },
  {
    title: 'Fast-Path p99 Latency',
    description: 'Single-shard transactions bypass 2PC entirely — bounded p99 with abort rate under 1%.',
    metric: '< 1ms',
    metricLabel: 'p99 latency (fast-path)',
    icon: <Timer className="w-5 h-5" />,
    color: 'text-amber-400',
    bgColor: 'bg-amber-400/10',
  },
  {
    title: 'Scale-Out TPS',
    description: 'Linear throughput scaling from 1 to 8 shards with minimal scatter-gather overhead.',
    metric: '~Linear',
    metricLabel: 'TPS vs shard count',
    icon: <BarChart3 className="w-5 h-5" />,
    color: 'text-falcon-400',
    bgColor: 'bg-falcon-400/10',
  },
]

const comparisonRows = [
  { feature: 'PG Wire Compatibility', falcon: true, pg: true, voltdb: false, singlestore: false },
  { feature: 'In-Memory First', falcon: true, pg: false, voltdb: true, singlestore: true },
  { feature: 'Deterministic Commits', falcon: true, pg: false, voltdb: true, singlestore: false },
  { feature: 'Fast-Path (No 2PC)', falcon: true, pg: false, voltdb: false, singlestore: false },
  { feature: 'WAL Replication', falcon: true, pg: true, voltdb: true, singlestore: true },
  { feature: 'Written in Rust', falcon: true, pg: false, voltdb: false, singlestore: false },
  { feature: 'Open Source', falcon: true, pg: true, voltdb: true, singlestore: false },
]

export default function Performance() {
  return (
    <section id="performance" className="relative py-24 lg:py-32">
      <div className="absolute top-0 left-0 right-0 h-px bg-gradient-to-r from-transparent via-white/[0.06] to-transparent" />
      <div className="absolute top-1/2 right-0 w-[500px] h-[500px] bg-falcon-600/5 rounded-full blur-[120px] -translate-y-1/2" />

      <div className="relative max-w-7xl mx-auto px-4 sm:px-6 lg:px-8">
        <div className="text-center mb-16">
          <p className="text-falcon-400 font-semibold text-sm tracking-wider uppercase mb-3">
            Benchmarks
          </p>
          <h2 className="text-3xl sm:text-4xl lg:text-5xl font-bold text-white mb-4">
            Built for <span className="gradient-text">Raw Speed</span>
          </h2>
          <p className="max-w-2xl mx-auto text-gray-400 text-lg">
            Reproducible benchmarks against PostgreSQL 16, VoltDB, and SingleStore.
            Random seed fixed at 42.
          </p>
        </div>

        {/* Benchmark cards */}
        <div className="grid md:grid-cols-3 gap-5 mb-16">
          {benchmarks.map((b, i) => (
            <div key={i} className="glass-card p-6 group hover:bg-white/[0.06] transition-all duration-300">
              <div className={`inline-flex items-center justify-center w-10 h-10 rounded-xl ${b.bgColor} ${b.color} mb-4`}>
                {b.icon}
              </div>
              <h3 className="text-white font-semibold text-lg mb-1">{b.title}</h3>
              <p className="text-gray-400 text-sm leading-relaxed mb-4">{b.description}</p>
              <div className="pt-4 border-t border-white/[0.06]">
                <p className={`text-3xl font-bold ${b.color}`}>{b.metric}</p>
                <p className="text-gray-500 text-xs mt-1">{b.metricLabel}</p>
              </div>
            </div>
          ))}
        </div>

        {/* Comparison table */}
        <div className="glass-card overflow-hidden">
          <div className="px-6 py-5 border-b border-white/[0.06]">
            <h3 className="text-white font-bold text-lg">Feature Comparison</h3>
            <p className="text-gray-400 text-sm mt-1">How FalconDB compares to other OLTP databases</p>
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
                {comparisonRows.map((row, i) => (
                  <tr key={i} className="border-b border-white/[0.04] hover:bg-white/[0.02] transition-colors">
                    <td className="text-gray-300 px-6 py-3">{row.feature}</td>
                    <td className="text-center px-4 py-3">
                      <Check value={row.falcon} highlight />
                    </td>
                    <td className="text-center px-4 py-3">
                      <Check value={row.pg} />
                    </td>
                    <td className="text-center px-4 py-3">
                      <Check value={row.voltdb} />
                    </td>
                    <td className="text-center px-4 py-3">
                      <Check value={row.singlestore} />
                    </td>
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
              View full benchmark matrix
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
