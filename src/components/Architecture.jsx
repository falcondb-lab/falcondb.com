export default function Architecture() {
  const layers = [
    {
      label: 'Protocol Layer',
      items: ['PG Wire Protocol (TCP)', 'Native Protocol (TCP/TLS)'],
      color: 'from-falcon-500/20 to-falcon-600/20',
      border: 'border-falcon-500/30',
    },
    {
      label: 'SQL Frontend',
      items: ['sqlparser-rs', 'Binder', 'Type Resolution'],
      color: 'from-blue-500/20 to-blue-600/20',
      border: 'border-blue-500/30',
    },
    {
      label: 'Planner / Router',
      items: ['Query Optimization', 'Shard Routing', 'Fast-Path Detection'],
      color: 'from-purple-500/20 to-purple-600/20',
      border: 'border-purple-500/30',
    },
    {
      label: 'Executor',
      items: ['Row-at-a-Time', 'Fused Streaming Aggregates', 'Zero-Copy Iteration'],
      color: 'from-cyan-500/20 to-cyan-600/20',
      border: 'border-cyan-500/30',
    },
    {
      label: 'Transaction + Storage',
      items: ['MVCC / OCC', 'MemTable + LSM', 'WAL', 'GC', 'USTM Tiered Memory'],
      color: 'from-emerald-500/20 to-emerald-600/20',
      border: 'border-emerald-500/30',
    },
    {
      label: 'Cluster Layer',
      items: ['ShardMap', 'Replication', 'Failover', 'Epoch Management'],
      color: 'from-amber-500/20 to-amber-600/20',
      border: 'border-amber-500/30',
    },
  ]

  const crates = [
    'falcon_common',
    'falcon_storage',
    'falcon_txn',
    'falcon_sql_frontend',
    'falcon_planner',
    'falcon_executor',
    'falcon_protocol_pg',
    'falcon_protocol_native',
    'falcon_cluster',
    'falcon_observability',
    'falcon_server',
    'falcon_bench',
  ]

  return (
    <section id="architecture" className="relative py-24 lg:py-32">
      <div className="absolute top-0 left-0 right-0 h-px bg-gradient-to-r from-transparent via-white/[0.06] to-transparent" />

      <div className="relative max-w-7xl mx-auto px-4 sm:px-6 lg:px-8">
        <div className="text-center mb-16">
          <p className="text-falcon-400 font-semibold text-sm tracking-wider uppercase mb-3">
            System Design
          </p>
          <h2 className="text-3xl sm:text-4xl lg:text-5xl font-bold text-white mb-4">
            Layered <span className="gradient-text">Architecture</span>
          </h2>
          <p className="max-w-2xl mx-auto text-gray-400 text-lg">
            A clean separation of concerns from protocol handling down to cluster management,
            with each layer optimized for its responsibility.
          </p>
        </div>

        <div className="grid lg:grid-cols-2 gap-10 items-start">
          {/* Architecture layers */}
          <div className="space-y-3">
            {layers.map((layer, i) => (
              <div
                key={i}
                className={`rounded-xl border ${layer.border} bg-gradient-to-r ${layer.color} p-4 transition-all duration-300 hover:scale-[1.01]`}
              >
                <p className="text-white font-semibold text-sm mb-2">{layer.label}</p>
                <div className="flex flex-wrap gap-2">
                  {layer.items.map((item, j) => (
                    <span
                      key={j}
                      className="px-2.5 py-1 bg-black/30 rounded-md text-gray-300 text-xs font-mono"
                    >
                      {item}
                    </span>
                  ))}
                </div>
              </div>
            ))}
          </div>

          {/* Crate structure */}
          <div className="glass-card p-6 lg:p-8">
            <h3 className="text-white font-bold text-xl mb-2">Crate Structure</h3>
            <p className="text-gray-400 text-sm mb-6">
              Modular Rust workspace with clean dependency boundaries between crates.
            </p>
            <div className="grid grid-cols-2 gap-2">
              {crates.map((crate, i) => (
                <div
                  key={i}
                  className="flex items-center gap-2 px-3 py-2.5 rounded-lg bg-white/[0.04] border border-white/[0.06] hover:border-falcon-500/30 hover:bg-falcon-500/5 transition-all"
                >
                  <div className="w-1.5 h-1.5 rounded-full bg-falcon-400" />
                  <span className="text-gray-300 text-xs font-mono">{crate}</span>
                </div>
              ))}
            </div>

            <div className="mt-6 p-4 rounded-lg bg-white/[0.03] border border-white/[0.06]">
              <h4 className="text-white font-semibold text-sm mb-2">Transaction Model</h4>
              <div className="space-y-2 text-xs text-gray-400">
                <div className="flex items-start gap-2">
                  <span className="text-emerald-400 font-mono font-bold shrink-0">Fast</span>
                  <span>LocalTxn — single-shard OCC, no 2PC overhead</span>
                </div>
                <div className="flex items-start gap-2">
                  <span className="text-amber-400 font-mono font-bold shrink-0">Slow</span>
                  <span>GlobalTxn — cross-shard XA-2PC with prepare/commit</span>
                </div>
              </div>
            </div>
          </div>
        </div>
      </div>
    </section>
  )
}
