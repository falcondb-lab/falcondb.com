export default {
  // Navbar
  nav: {
    features: '核心特性',
    architecture: '系统架构',
    performance: '性能基准',
    quickstart: '快速开始',
    community: '社区',
    docs: '文档',
    github: 'GitHub',
  },

  // Hero
  hero: {
    badge: '开源 · Rust 驱动 · 兼容 PostgreSQL',
    titleThe: '内存优先的',
    titleHighlight: '分布式',
    titleEnd: 'OLTP 数据库',
    subtitle: '兼容 PostgreSQL 协议，分布式部署，确定性事务语义。',
    subtitleLine2: '基于 Rust 构建，微秒级延迟，可证明的一致性保证。',
    statLatency: '< 1ms',
    statLatencyLabel: 'p99 延迟',
    statAcid: 'ACID',
    statAcidLabel: '完整保证',
    statFunctions: '500+',
    statFunctionsLabel: 'SQL 函数',
    ctaGetStarted: '快速开始',
    ctaGitHub: '查看 GitHub',
    terminalPrompt: 'psql -h localhost -p 5433',
    terminalListening: 'FalconDB v1.2 — 监听 0.0.0.0:5433',
  },

  // Features
  features: {
    sectionLabel: '核心能力',
    title: '为',
    titleHighlight: 'OLTP 性能而生',
    subtitle: '每个组件都为事务型工作负载专门设计——从内存优先的存储引擎到确定性提交协议。',
    items: [
      {
        title: '亚毫秒延迟',
        desc: '单分片快速路径提交完全绕过 2PC。热路径上的内存 MVCC 实现零磁盘 I/O。',
      },
      {
        title: '确定性提交保证',
        desc: '如果 FalconDB 返回"已提交"，该事务将在任何单节点崩溃、故障转移和恢复中存活——零例外。',
      },
      {
        title: 'PG 线协议',
        desc: '使用 psql、pgbench 或任何 PostgreSQL 驱动连接。标准 OLTP 工作负载的即插即用兼容。',
      },
      {
        title: '分布式架构',
        desc: '基于分片路由和纪元管理的集群架构。吞吐量随分片数量线性扩展。',
      },
      {
        title: 'MVCC + OCC 隔离',
        desc: '快照隔离 + 乐观并发控制。CI 验证的 ACID 保证，中止率 < 1%。',
      },
      {
        title: '基于 WAL 的复制',
        desc: 'gRPC WAL 流式传输实现主从复制。自动提升和故障转移，零数据丢失。',
      },
      {
        title: '双存储引擎',
        desc: '内存 Rowstore 提供最大吞吐量，LSM 磁盘引擎支持超出内存容量的数据集。',
      },
      {
        title: '50+ SHOW 命令',
        desc: '深度可观测性：事务统计、GC 指标、复制延迟和 Prometheus 兼容端点。',
      },
      {
        title: 'Rust 构建',
        desc: '内存安全、零成本抽象、无畏并发。最小运行时开销，最大化性能表现。',
      },
    ],
  },

  // Architecture
  arch: {
    sectionLabel: '系统设计',
    title: '分层',
    titleHighlight: '架构',
    subtitle: '从协议处理到集群管理的清晰关注点分离，每一层都针对其职责进行了优化。',
    layers: [
      { label: '协议层', items: ['PG 线协议 (TCP)', '原生协议 (TCP/TLS)'] },
      { label: 'SQL 前端', items: ['sqlparser-rs', 'Binder', '类型解析'] },
      { label: '计划器 / 路由器', items: ['查询优化', '分片路由', '快速路径检测'] },
      { label: '执行器', items: ['逐行执行', '融合流式聚合', '零拷贝迭代'] },
      { label: '事务 + 存储', items: ['MVCC / OCC', 'MemTable + LSM', 'WAL', 'GC', 'USTM 分级内存'] },
      { label: '集群层', items: ['ShardMap', '复制', '故障转移', '纪元管理'] },
    ],
    crateTitle: 'Crate 结构',
    crateSubtitle: '模块化 Rust 工作空间，Crate 之间具有清晰的依赖边界。',
    txnTitle: '事务模型',
    txnFast: 'LocalTxn — 单分片 OCC，无 2PC 开销',
    txnSlow: 'GlobalTxn — 跨分片 XA-2PC，准备/提交',
  },

  // Performance
  perf: {
    sectionLabel: '性能基准',
    title: '为',
    titleHighlight: '极致速度而生',
    subtitle: '与 PostgreSQL 16、VoltDB 和 SingleStore 的可复现基准测试。随机种子固定为 42。',
    benchmarks: [
      {
        title: '批量插入 (100 万行)',
        description: '内存 MVCC 零磁盘 I/O，写入吞吐量大幅超越 PostgreSQL。',
        metric: '~10x',
        metricLabel: 'INSERT 速度 vs PG 16',
      },
      {
        title: '快速路径 p99 延迟',
        description: '单分片事务完全绕过 2PC——有界 p99，中止率低于 1%。',
        metric: '< 1ms',
        metricLabel: 'p99 延迟（快速路径）',
      },
      {
        title: '横向扩展 TPS',
        description: '1 到 8 个分片的线性吞吐量扩展，scatter-gather 开销最小。',
        metric: '~线性',
        metricLabel: 'TPS vs 分片数',
      },
    ],
    comparisonTitle: '功能对比',
    comparisonSubtitle: 'FalconDB 与其他 OLTP 数据库的对比',
    viewBenchmarks: '查看完整基准测试矩阵',
    tableFeatures: ['PG 线协议兼容', '内存优先', '确定性提交', '快速路径（无 2PC）', 'WAL 复制', 'Rust 编写', '开源'],
  },

  // Quick Start
  quick: {
    sectionLabel: '快速开始',
    title: '几分钟内',
    titleHighlight: '启动运行',
    subtitle: '需要 Rust 1.75+。克隆、构建、连接——使用任何 PostgreSQL 客户端。',
    tabs: {
      linux: 'Linux / macOS',
      windows: 'Windows',
      replication: '主从复制演示',
    },
    steps: {
      linux: [
        { title: '克隆 & 构建' },
        { title: '启动 FalconDB' },
        { title: '使用 psql 连接' },
        { title: '或运行完整演示' },
      ],
      windows: [
        { title: '克隆 & 构建' },
        { title: '启动 FalconDB' },
        { title: '或运行完整演示' },
      ],
      replication: [
        { title: '启动主节点 + 副本' },
        { title: '端到端故障转移测试' },
      ],
    },
    prereqTitle: '前置条件',
    prereqText: 'Rust 1.75+、Git，以及可选的 psql 用于交互式会话。FalconDB 默认监听 5433 端口。',
    copied: '已复制',
    copy: '复制',
  },

  // Community
  community: {
    sectionLabel: '开源社区',
    title: '加入',
    titleHighlight: '社区',
    subtitle: 'FalconDB 采用 Apache-2.0 许可证，开放构建。欢迎贡献、反馈和协作。',
    links: [
      { title: 'GitHub 仓库', desc: '浏览源代码、提交 Issue 和 Pull Request。' },
      { title: '项目文档', desc: '架构文档、协议兼容性、一致性证据和基准测试。' },
      { title: '社区讨论', desc: '提出问题、分享想法，与其他 FalconDB 用户交流。' },
      { title: '参与贡献', desc: '阅读贡献指南，共同塑造 FalconDB 的未来。' },
    ],
    roadmapTitle: '路线图',
    roadmapSubtitle: '透明的开发里程碑。',
    roadmapLink: '查看完整路线图 →',
    shipped: '已发布',
    planned: '计划中',
    roadmapItems: [
      { label: '核心 OLTP + 复制 + 基准测试' },
      { label: 'gRPC WAL 流式传输 + 多节点部署' },
      { label: 'Raft 共识 + 磁盘 Rowstore + 加密' },
      { label: '在线 DDL + PITR + 资源隔离' },
    ],
  },

  // Footer
  footer: {
    description: '兼容 PostgreSQL 的分布式内存优先 OLTP 数据库，具有确定性事务语义。',
    starOnGitHub: '在 GitHub 上 Star',
    columns: [
      {
        title: '项目',
        links: ['GitHub', '版本发布', '许可证 (Apache-2.0)', '安全策略'],
      },
      {
        title: '文档',
        links: ['快速开始', '系统架构', '协议兼容性', '基准测试矩阵'],
      },
      {
        title: '资源',
        links: ['路线图', '参与贡献', '一致性证据', 'RPO / RTO'],
      },
    ],
    copyright: '© {year} FalconDB 贡献者。Apache-2.0 许可证。',
    builtWith: '使用 React · Vite · Tailwind CSS 构建',
  },
}
