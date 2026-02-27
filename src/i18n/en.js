export default {
  nav: {
    features: 'Features',
    architecture: 'Architecture',
    performance: 'Performance',
    quickstart: 'Quick Start',
    community: 'Community',
    docs: 'Docs',
    github: 'GitHub',
  },

  hero: {
    badge: 'Open Source · Rust-Powered · PG-Compatible',
    titleThe: 'The',
    titleHighlight: 'Memory-First',
    titleEnd: 'OLTP Database',
    subtitle: 'PG-compatible, distributed, deterministic transaction semantics.',
    subtitleLine2: 'Built in Rust for microsecond-level latency with provable consistency.',
    statLatency: '< 1ms',
    statLatencyLabel: 'p99 Latency',
    statAcid: 'ACID',
    statAcidLabel: 'Guaranteed',
    statFunctions: '500+',
    statFunctionsLabel: 'SQL Functions',
    ctaGetStarted: 'Get Started',
    ctaGitHub: 'View on GitHub',
    terminalPrompt: 'psql -h localhost -p 5433',
    terminalListening: 'FalconDB v1.2 — listening on 0.0.0.0:5433',
  },

  features: {
    sectionLabel: 'Core Capabilities',
    title: 'Engineered for',
    titleHighlight: 'OLTP Performance',
    subtitle: 'Every component is purpose-built for transactional workloads — from the memory-first storage engine to the deterministic commit protocol.',
    items: [
      {
        title: 'Sub-Millisecond Latency',
        desc: 'Single-shard fast-path commits bypass 2PC entirely. In-memory MVCC with zero disk I/O on the hot path.',
      },
      {
        title: 'Deterministic Commit Guarantee',
        desc: 'If FalconDB returns "committed", that transaction survives any single-node crash, failover, and recovery — zero exceptions.',
      },
      {
        title: 'PG Wire Protocol',
        desc: 'Connect with psql, pgbench, or any PostgreSQL driver. Drop-in compatible for standard OLTP workloads.',
      },
      {
        title: 'Distributed Architecture',
        desc: 'Shard-based routing with epoch-based cluster management. Scale out linearly with shard count.',
      },
      {
        title: 'MVCC + OCC Isolation',
        desc: 'Snapshot Isolation with optimistic concurrency control. CI-verified ACID guarantees with abort rate < 1%.',
      },
      {
        title: 'WAL-Based Replication',
        desc: 'gRPC WAL streaming for primary–replica replication. Automatic promote and failover with zero data loss.',
      },
      {
        title: 'Dual Storage Engines',
        desc: 'In-memory Rowstore for maximum throughput, LSM disk-backed engine for larger-than-memory datasets.',
      },
      {
        title: '50+ SHOW Commands',
        desc: 'Deep observability with transaction stats, GC metrics, replication lag, and Prometheus-compatible endpoints.',
      },
      {
        title: 'Built in Rust',
        desc: 'Memory-safe, zero-cost abstractions, fearless concurrency. Minimal footprint with maximum performance.',
      },
    ],
  },

  arch: {
    sectionLabel: 'System Design',
    title: 'Layered',
    titleHighlight: 'Architecture',
    subtitle: 'A clean separation of concerns from protocol handling down to cluster management, with each layer optimized for its responsibility.',
    layers: [
      { label: 'Protocol Layer', items: ['PG Wire Protocol (TCP)', 'Native Protocol (TCP/TLS)'] },
      { label: 'SQL Frontend', items: ['sqlparser-rs', 'Binder', 'Type Resolution'] },
      { label: 'Planner / Router', items: ['Query Optimization', 'Shard Routing', 'Fast-Path Detection'] },
      { label: 'Executor', items: ['Row-at-a-Time', 'Fused Streaming Aggregates', 'Zero-Copy Iteration'] },
      { label: 'Transaction + Storage', items: ['MVCC / OCC', 'MemTable + LSM', 'WAL', 'GC', 'USTM Tiered Memory'] },
      { label: 'Cluster Layer', items: ['ShardMap', 'Replication', 'Failover', 'Epoch Management'] },
    ],
    crateTitle: 'Crate Structure',
    crateSubtitle: 'Modular Rust workspace with clean dependency boundaries between crates.',
    txnTitle: 'Transaction Model',
    txnFast: 'LocalTxn — single-shard OCC, no 2PC overhead',
    txnSlow: 'GlobalTxn — cross-shard XA-2PC with prepare/commit',
  },

  perf: {
    sectionLabel: 'Benchmarks',
    title: 'Built for',
    titleHighlight: 'Raw Speed',
    subtitle: 'Reproducible benchmarks against PostgreSQL 16, VoltDB, and SingleStore. Random seed fixed at 42.',
    benchmarks: [
      {
        title: 'Bulk Insert (1M Rows)',
        description: 'In-memory MVCC with zero disk I/O dramatically outperforms PostgreSQL on write throughput.',
        metric: '~10x',
        metricLabel: 'faster INSERT vs PG 16',
      },
      {
        title: 'Fast-Path p99 Latency',
        description: 'Single-shard transactions bypass 2PC entirely — bounded p99 with abort rate under 1%.',
        metric: '< 1ms',
        metricLabel: 'p99 latency (fast-path)',
      },
      {
        title: 'Scale-Out TPS',
        description: 'Linear throughput scaling from 1 to 8 shards with minimal scatter-gather overhead.',
        metric: '~Linear',
        metricLabel: 'TPS vs shard count',
      },
    ],
    comparisonTitle: 'Feature Comparison',
    comparisonSubtitle: 'How FalconDB compares to other OLTP databases',
    viewBenchmarks: 'View full benchmark matrix',
    tableFeatures: ['PG Wire Compatibility', 'In-Memory First', 'Deterministic Commits', 'Fast-Path (No 2PC)', 'WAL Replication', 'Written in Rust', 'Open Source'],
  },

  quick: {
    sectionLabel: 'Get Started',
    title: 'Up and Running in',
    titleHighlight: 'Minutes',
    subtitle: 'Requires Rust 1.75+. Clone, build, connect — use any PostgreSQL client.',
    tabs: {
      linux: 'Linux / macOS',
      windows: 'Windows',
      replication: 'Replication Demo',
    },
    steps: {
      linux: [
        { title: 'Clone & Build' },
        { title: 'Start FalconDB' },
        { title: 'Connect with psql' },
        { title: 'Or run the full demo' },
      ],
      windows: [
        { title: 'Clone & Build' },
        { title: 'Start FalconDB' },
        { title: 'Or run the full demo' },
      ],
      replication: [
        { title: 'Start Primary + Replica' },
        { title: 'E2E Failover Test' },
      ],
    },
    prereqTitle: 'Prerequisites',
    prereqText: 'Rust 1.75+, Git, and optionally psql for interactive sessions. FalconDB listens on port 5433 by default.',
    copied: 'Copied',
    copy: 'Copy',
  },

  community: {
    sectionLabel: 'Open Source',
    title: 'Join the',
    titleHighlight: 'Community',
    subtitle: 'FalconDB is Apache-2.0 licensed and built in the open. Contributions, feedback, and collaboration are welcome.',
    links: [
      { title: 'GitHub Repository', desc: 'Browse source code, open issues, and submit pull requests.' },
      { title: 'Documentation', desc: 'Architecture docs, protocol compatibility, consistency evidence, and benchmarks.' },
      { title: 'Discussions', desc: 'Ask questions, share ideas, and connect with other FalconDB users.' },
      { title: 'Contributing', desc: 'Read the contributing guide and help shape the future of FalconDB.' },
    ],
    roadmapTitle: 'Roadmap',
    roadmapSubtitle: 'Transparent development milestones.',
    roadmapLink: 'See full roadmap →',
    shipped: 'Shipped',
    planned: 'Planned',
    roadmapItems: [
      { label: 'Core OLTP + Replication + Benchmarks' },
      { label: 'gRPC WAL Streaming + Multi-Node Deploy' },
      { label: 'Raft Consensus + Disk Rowstore + Encryption' },
      { label: 'Online DDL + PITR + Resource Isolation' },
    ],
  },

  footer: {
    description: 'PG-compatible, distributed, memory-first OLTP database with deterministic transaction semantics.',
    starOnGitHub: 'Star on GitHub',
    columns: [
      {
        title: 'Project',
        links: ['GitHub', 'Releases', 'License (Apache-2.0)', 'Security Policy'],
      },
      {
        title: 'Documentation',
        links: ['Getting Started', 'Architecture', 'Protocol Compatibility', 'Benchmark Matrix'],
      },
      {
        title: 'Resources',
        links: ['Roadmap', 'Contributing', 'Consistency Evidence', 'RPO / RTO'],
      },
    ],
    copyright: '© {year} FalconDB Contributors. Apache-2.0 License.',
    builtWith: 'Built with React · Vite · Tailwind CSS',
  },
}
