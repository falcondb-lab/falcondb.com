export default {
  // Navbar
  nav: {
    features: 'Fonctionnalités',
    architecture: 'Architecture',
    performance: 'Performance',
    quickstart: 'Démarrage',
    community: 'Communauté',
    docs: 'Docs',
    github: 'GitHub',
  },

  // Hero
  hero: {
    badge: 'Open Source · Propulsé par Rust · Compatible PG',
    titleThe: 'La base de données',
    titleHighlight: 'Memory-First',
    titleEnd: 'OLTP',
    subtitle: 'Compatible PG, distribuée, avec sémantique de transaction déterministe.',
    subtitleLine2: 'Construite en Rust pour une latence de l\'ordre de la microseconde avec une cohérence prouvable.',
    statLatency: '< 1ms',
    statLatencyLabel: 'Latence p99',
    statAcid: 'ACID',
    statAcidLabel: 'Garanti',
    statFunctions: '500+',
    statFunctionsLabel: 'Fonctions SQL',
    ctaGetStarted: 'Commencer',
    ctaGitHub: 'Voir sur GitHub',
    terminalPrompt: 'psql -h localhost -p 5433',
    terminalListening: 'FalconDB v1.2 — écoute sur 0.0.0.0:5433',
  },

  // Features
  features: {
    sectionLabel: 'Capacités clés',
    title: 'Conçu pour la',
    titleHighlight: 'Performance OLTP',
    subtitle: 'Chaque composant est spécialement conçu pour les charges transactionnelles — du moteur de stockage en mémoire au protocole de validation déterministe.',
    items: [
      {
        title: 'Latence sub-milliseconde',
        desc: 'Les validations fast-path mono-shard contournent entièrement le 2PC. MVCC en mémoire sans I/O disque sur le chemin critique.',
      },
      {
        title: 'Garantie de validation déterministe',
        desc: 'Si FalconDB retourne "committed", cette transaction survit à tout crash de nœud, basculement et récupération — sans exception.',
      },
      {
        title: 'Protocole PG Wire',
        desc: 'Connectez-vous avec psql, pgbench ou tout pilote PostgreSQL. Compatible de manière transparente pour les charges OLTP standard.',
      },
      {
        title: 'Architecture distribuée',
        desc: 'Routage par shard avec gestion de cluster basée sur les époques. Montée en charge linéaire avec le nombre de shards.',
      },
      {
        title: 'Isolation MVCC + OCC',
        desc: 'Isolation par snapshot avec contrôle de concurrence optimiste. Garanties ACID vérifiées par CI avec un taux d\'abandon < 1%.',
      },
      {
        title: 'Réplication basée sur WAL',
        desc: 'Streaming WAL gRPC pour la réplication primaire–réplica. Promotion automatique et basculement sans perte de données.',
      },
      {
        title: 'Double moteur de stockage',
        desc: 'Rowstore en mémoire pour un débit maximal, moteur LSM sur disque pour les jeux de données dépassant la mémoire.',
      },
      {
        title: '50+ commandes SHOW',
        desc: 'Observabilité approfondie avec stats de transactions, métriques GC, retard de réplication et endpoints compatibles Prometheus.',
      },
      {
        title: 'Construit en Rust',
        desc: 'Sûreté mémoire, abstractions à coût zéro, concurrence sans crainte. Empreinte minimale avec performance maximale.',
      },
    ],
  },

  // Architecture
  arch: {
    sectionLabel: 'Conception système',
    title: 'Architecture',
    titleHighlight: 'en couches',
    subtitle: 'Une séparation claire des responsabilités, de la gestion du protocole à la gestion du cluster, chaque couche étant optimisée pour sa fonction.',
    layers: [
      { label: 'Couche protocole', items: ['PG Wire Protocol (TCP)', 'Native Protocol (TCP/TLS)'] },
      { label: 'Frontend SQL', items: ['sqlparser-rs', 'Binder', 'Type Resolution'] },
      { label: 'Planificateur / Routeur', items: ['Query Optimization', 'Shard Routing', 'Fast-Path Detection'] },
      { label: 'Exécuteur', items: ['Row-at-a-Time', 'Fused Streaming Aggregates', 'Zero-Copy Iteration'] },
      { label: 'Transaction + Stockage', items: ['MVCC / OCC', 'MemTable + LSM', 'WAL', 'GC', 'USTM Tiered Memory'] },
      { label: 'Couche cluster', items: ['ShardMap', 'Replication', 'Failover', 'Epoch Management'] },
    ],
    crateTitle: 'Structure des crates',
    crateSubtitle: 'Workspace Rust modulaire avec des frontières de dépendances claires entre les crates.',
    txnTitle: 'Modèle transactionnel',
    txnFast: 'LocalTxn — OCC mono-shard, sans surcharge 2PC',
    txnSlow: 'GlobalTxn — XA-2PC cross-shard avec prepare/commit',
  },

  // Performance
  perf: {
    sectionLabel: 'Benchmarks',
    title: 'Conçu pour la',
    titleHighlight: 'vitesse brute',
    subtitle: 'Benchmarks reproductibles contre PostgreSQL 16, VoltDB et SingleStore. Graine aléatoire fixée à 42.',
    benchmarks: [
      {
        title: 'Insertion en masse (1M lignes)',
        description: 'Le MVCC en mémoire sans I/O disque surpasse dramatiquement PostgreSQL en débit d\'écriture.',
        metric: '~10x',
        metricLabel: 'INSERT plus rapide vs PG 16',
      },
      {
        title: 'Latence p99 Fast-Path',
        description: 'Les transactions mono-shard contournent entièrement le 2PC — p99 borné avec un taux d\'abandon < 1%.',
        metric: '< 1ms',
        metricLabel: 'latence p99 (fast-path)',
      },
      {
        title: 'TPS Scale-Out',
        description: 'Montée en charge linéaire du débit de 1 à 8 shards avec un overhead scatter-gather minimal.',
        metric: '~Linéaire',
        metricLabel: 'TPS vs nombre de shards',
      },
    ],
    comparisonTitle: 'Comparaison des fonctionnalités',
    comparisonSubtitle: 'Comment FalconDB se compare aux autres bases OLTP',
    viewBenchmarks: 'Voir la matrice complète des benchmarks',
    tableFeatures: ['Compatibilité PG Wire', 'Memory-First', 'Validations déterministes', 'Fast-Path (sans 2PC)', 'Réplication WAL', 'Écrit en Rust', 'Open Source'],
  },

  // Quick Start
  quick: {
    sectionLabel: 'Démarrage',
    title: 'Opérationnel en',
    titleHighlight: 'quelques minutes',
    subtitle: 'Nécessite Rust 1.75+. Clonez, compilez, connectez — utilisez n\'importe quel client PostgreSQL.',
    tabs: {
      linux: 'Linux / macOS',
      windows: 'Windows',
      replication: 'Démo réplication',
    },
    steps: {
      linux: [
        { title: 'Cloner & Compiler' },
        { title: 'Démarrer FalconDB' },
        { title: 'Se connecter avec psql' },
        { title: 'Ou lancer la démo complète' },
      ],
      windows: [
        { title: 'Cloner & Compiler' },
        { title: 'Démarrer FalconDB' },
        { title: 'Ou lancer la démo complète' },
      ],
      replication: [
        { title: 'Démarrer Primaire + Réplica' },
        { title: 'Test de basculement E2E' },
      ],
    },
    prereqTitle: 'Prérequis',
    prereqText: 'Rust 1.75+, Git, et optionnellement psql pour les sessions interactives. FalconDB écoute sur le port 5433 par défaut.',
    copied: 'Copié',
    copy: 'Copier',
  },

  // Community
  community: {
    sectionLabel: 'Open Source',
    title: 'Rejoignez la',
    titleHighlight: 'communauté',
    subtitle: 'FalconDB est sous licence Apache-2.0 et développé en open source. Contributions, retours et collaboration sont les bienvenus.',
    links: [
      { title: 'Dépôt GitHub', desc: 'Parcourez le code source, ouvrez des issues et soumettez des pull requests.' },
      { title: 'Documentation', desc: 'Docs d\'architecture, compatibilité protocole, preuves de cohérence et benchmarks.' },
      { title: 'Discussions', desc: 'Posez des questions, partagez des idées et connectez-vous avec d\'autres utilisateurs FalconDB.' },
      { title: 'Contribuer', desc: 'Lisez le guide de contribution et participez à l\'avenir de FalconDB.' },
    ],
    roadmapTitle: 'Feuille de route',
    roadmapSubtitle: 'Jalons de développement transparents.',
    roadmapLink: 'Voir la feuille de route complète →',
    shipped: 'Livré',
    planned: 'Prévu',
    roadmapItems: [
      { label: 'OLTP Core + Réplication + Benchmarks' },
      { label: 'Streaming WAL gRPC + Déploiement multi-nœuds' },
      { label: 'Consensus Raft + Rowstore disque + Chiffrement' },
      { label: 'DDL en ligne + PITR + Isolation des ressources' },
    ],
  },

  // Footer
  footer: {
    description: 'Base de données OLTP compatible PG, distribuée, memory-first avec sémantique de transaction déterministe.',
    starOnGitHub: 'Star sur GitHub',
    columns: [
      {
        title: 'Projet',
        links: ['GitHub', 'Versions', 'Licence (Apache-2.0)', 'Politique de sécurité'],
      },
      {
        title: 'Documentation',
        links: ['Démarrage', 'Architecture', 'Compatibilité protocole', 'Matrice de benchmarks'],
      },
      {
        title: 'Ressources',
        links: ['Feuille de route', 'Contribuer', 'Preuves de cohérence', 'RPO / RTO'],
      },
    ],
    copyright: '© {year} Contributeurs FalconDB. Licence Apache-2.0.',
    builtWith: 'Construit avec React · Vite · Tailwind CSS',
  },
}
