import { ArrowRight, Github, Terminal, Zap, Shield, Database } from 'lucide-react'

export default function Hero() {
  return (
    <section className="relative min-h-screen flex items-center justify-center overflow-hidden">
      {/* Background effects */}
      <div className="absolute inset-0">
        <div className="absolute top-1/4 left-1/2 -translate-x-1/2 -translate-y-1/2 w-[800px] h-[600px] bg-falcon-600/10 rounded-full blur-[120px]" />
        <div className="absolute bottom-1/4 left-1/3 w-[400px] h-[400px] bg-cyan-500/8 rounded-full blur-[100px]" />
        <div className="absolute top-0 left-0 right-0 h-px bg-gradient-to-r from-transparent via-falcon-500/20 to-transparent" />
        {/* Grid */}
        <div
          className="absolute inset-0 opacity-[0.03]"
          style={{
            backgroundImage: `linear-gradient(rgba(255,255,255,0.1) 1px, transparent 1px),
                              linear-gradient(90deg, rgba(255,255,255,0.1) 1px, transparent 1px)`,
            backgroundSize: '60px 60px',
          }}
        />
      </div>

      <div className="relative max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 pt-32 pb-20 text-center">
        {/* Badge */}
        <div className="inline-flex items-center gap-2 px-4 py-1.5 rounded-full bg-falcon-500/10 border border-falcon-500/20 text-falcon-400 text-sm font-medium mb-8 animate-fade-in">
          <Zap className="w-3.5 h-3.5" />
          Open Source &middot; Rust-Powered &middot; PG-Compatible
        </div>

        {/* Heading */}
        <h1 className="text-5xl sm:text-6xl lg:text-7xl xl:text-8xl font-extrabold tracking-tight leading-[1.05] mb-6 animate-slide-up">
          <span className="text-white">The</span>{' '}
          <span className="gradient-text">Memory-First</span>
          <br />
          <span className="text-white">OLTP Database</span>
        </h1>

        {/* Subtitle */}
        <p className="max-w-2xl mx-auto text-lg sm:text-xl text-gray-400 leading-relaxed mb-10 animate-slide-up" style={{ animationDelay: '0.15s' }}>
          PG-compatible, distributed, deterministic transaction semantics.
          <br className="hidden sm:block" />
          Built in Rust for microsecond-level latency with provable consistency.
        </p>

        {/* Stats row */}
        <div className="flex flex-wrap items-center justify-center gap-6 sm:gap-10 mb-10 animate-slide-up" style={{ animationDelay: '0.25s' }}>
          <Stat icon={<Zap className="w-4 h-4 text-amber-400" />} value="< 1ms" label="p99 Latency" />
          <Stat icon={<Shield className="w-4 h-4 text-emerald-400" />} value="ACID" label="Guaranteed" />
          <Stat icon={<Database className="w-4 h-4 text-falcon-400" />} value="500+" label="SQL Functions" />
        </div>

        {/* CTA */}
        <div className="flex flex-wrap items-center justify-center gap-4 animate-slide-up" style={{ animationDelay: '0.35s' }}>
          <a
            href="#quickstart"
            className="group flex items-center gap-2 px-7 py-3.5 bg-gradient-to-r from-falcon-600 to-falcon-500 hover:from-falcon-500 hover:to-falcon-400 text-white font-semibold rounded-xl shadow-lg shadow-falcon-500/25 transition-all duration-300 hover:shadow-falcon-500/40"
          >
            <Terminal className="w-4.5 h-4.5" />
            Get Started
            <ArrowRight className="w-4 h-4 group-hover:translate-x-0.5 transition-transform" />
          </a>
          <a
            href="https://github.com/falcondb-lab/falcondb"
            target="_blank"
            rel="noopener noreferrer"
            className="flex items-center gap-2 px-7 py-3.5 bg-white/[0.06] hover:bg-white/[0.1] border border-white/[0.1] text-white font-semibold rounded-xl transition-all duration-300"
          >
            <Github className="w-4.5 h-4.5" />
            View on GitHub
          </a>
        </div>

        {/* Terminal preview */}
        <div className="mt-16 max-w-3xl mx-auto animate-slide-up" style={{ animationDelay: '0.5s' }}>
          <div className="glass-card glow-border overflow-hidden shadow-2xl shadow-falcon-950/50">
            <div className="flex items-center gap-2 px-4 py-3 border-b border-white/[0.06]">
              <div className="w-3 h-3 rounded-full bg-red-500/80" />
              <div className="w-3 h-3 rounded-full bg-yellow-500/80" />
              <div className="w-3 h-3 rounded-full bg-green-500/80" />
              <span className="ml-2 text-xs text-gray-500 font-mono">psql -h localhost -p 5433</span>
            </div>
            <div className="p-5 text-left font-mono text-sm leading-relaxed">
              <Line prompt="$" cmd="cargo build --release -p falcon_server" />
              <Line prompt="$" cmd="./target/release/falcon --config falcon.toml" />
              <p className="text-emerald-400 mt-1">
                FalconDB v1.2 — listening on 0.0.0.0:5433
              </p>
              <p className="text-gray-600 mt-3">---</p>
              <Line prompt="falcon=#" cmd="CREATE TABLE users (id INT PRIMARY KEY, name TEXT);" />
              <p className="text-gray-500">CREATE TABLE</p>
              <Line prompt="falcon=#" cmd="INSERT INTO users VALUES (1, 'alice'), (2, 'bob');" />
              <p className="text-gray-500">INSERT 0 2</p>
              <Line prompt="falcon=#" cmd="SELECT * FROM users WHERE id = 1;" />
              <p className="text-cyan-400"> id | name</p>
              <p className="text-cyan-400">----+-------</p>
              <p className="text-cyan-400">  1 | alice</p>
            </div>
          </div>
        </div>
      </div>
    </section>
  )
}

function Stat({ icon, value, label }) {
  return (
    <div className="flex items-center gap-2.5">
      {icon}
      <div className="text-left">
        <p className="text-white font-bold text-lg leading-tight">{value}</p>
        <p className="text-gray-500 text-xs">{label}</p>
      </div>
    </div>
  )
}

function Line({ prompt, cmd }) {
  return (
    <p className="mt-1">
      <span className="text-falcon-400">{prompt}</span>{' '}
      <span className="text-gray-300">{cmd}</span>
    </p>
  )
}
