import { useState } from 'react'
import { Copy, Check, Terminal } from 'lucide-react'

const tabs = [
  {
    id: 'linux',
    label: 'Linux / macOS',
    steps: [
      {
        title: 'Clone & Build',
        code: `git clone https://github.com/falcondb-lab/falcondb.git
cd falcondb
cargo build --release -p falcon_server`,
      },
      {
        title: 'Start FalconDB',
        code: `./target/release/falcon --config falcon.toml`,
      },
      {
        title: 'Connect with psql',
        code: `psql -h localhost -p 5433 -U falcon`,
      },
      {
        title: 'Or run the full demo',
        code: `chmod +x scripts/demo_standalone.sh
./scripts/demo_standalone.sh`,
      },
    ],
  },
  {
    id: 'windows',
    label: 'Windows',
    steps: [
      {
        title: 'Clone & Build',
        code: `git clone https://github.com/falcondb-lab/falcondb.git
cd falcondb
cargo build --release -p falcon_server`,
      },
      {
        title: 'Start FalconDB',
        code: `.\\target\\release\\falcon.exe --config falcon.toml`,
      },
      {
        title: 'Or run the full demo',
        code: `.\\scripts\\demo_standalone.ps1`,
      },
    ],
  },
  {
    id: 'replication',
    label: 'Replication Demo',
    steps: [
      {
        title: 'Start Primary + Replica',
        code: `chmod +x scripts/demo_replication.sh
./scripts/demo_replication.sh`,
      },
      {
        title: 'E2E Failover Test',
        code: `chmod +x scripts/e2e_two_node_failover.sh
./scripts/e2e_two_node_failover.sh`,
      },
    ],
  },
]

export default function QuickStart() {
  const [activeTab, setActiveTab] = useState('linux')
  const active = tabs.find((t) => t.id === activeTab)

  return (
    <section id="quickstart" className="relative py-24 lg:py-32">
      <div className="absolute top-0 left-0 right-0 h-px bg-gradient-to-r from-transparent via-white/[0.06] to-transparent" />
      <div className="absolute bottom-1/3 left-0 w-[400px] h-[400px] bg-cyan-500/5 rounded-full blur-[100px]" />

      <div className="relative max-w-4xl mx-auto px-4 sm:px-6 lg:px-8">
        <div className="text-center mb-12">
          <p className="text-falcon-400 font-semibold text-sm tracking-wider uppercase mb-3">
            Get Started
          </p>
          <h2 className="text-3xl sm:text-4xl lg:text-5xl font-bold text-white mb-4">
            Up and Running in <span className="gradient-text">Minutes</span>
          </h2>
          <p className="max-w-xl mx-auto text-gray-400 text-lg">
            Requires Rust 1.75+. Clone, build, connect — use any PostgreSQL client.
          </p>
        </div>

        {/* Tabs */}
        <div className="flex gap-1 p-1 bg-white/[0.04] rounded-xl mb-6 max-w-fit mx-auto">
          {tabs.map((tab) => (
            <button
              key={tab.id}
              onClick={() => setActiveTab(tab.id)}
              className={`px-4 py-2 rounded-lg text-sm font-medium transition-all ${
                activeTab === tab.id
                  ? 'bg-falcon-600 text-white shadow-lg shadow-falcon-600/25'
                  : 'text-gray-400 hover:text-white hover:bg-white/[0.06]'
              }`}
            >
              {tab.label}
            </button>
          ))}
        </div>

        {/* Steps */}
        <div className="space-y-4">
          {active.steps.map((step, i) => (
            <div key={i} className="glass-card overflow-hidden">
              <div className="flex items-center justify-between px-4 py-2.5 border-b border-white/[0.06]">
                <div className="flex items-center gap-2">
                  <span className="flex items-center justify-center w-5 h-5 rounded-full bg-falcon-500/20 text-falcon-400 text-xs font-bold">
                    {i + 1}
                  </span>
                  <span className="text-gray-300 text-sm font-medium">{step.title}</span>
                </div>
                <CopyButton text={step.code} />
              </div>
              <pre className="p-4 text-sm font-mono text-gray-300 overflow-x-auto leading-relaxed">
                {step.code}
              </pre>
            </div>
          ))}
        </div>

        {/* Prerequisites note */}
        <div className="mt-8 p-4 rounded-xl bg-amber-500/5 border border-amber-500/20">
          <div className="flex items-start gap-3">
            <Terminal className="w-4 h-4 text-amber-400 mt-0.5 shrink-0" />
            <div className="text-sm">
              <p className="text-amber-200 font-medium">Prerequisites</p>
              <p className="text-gray-400 mt-1">
                Rust 1.75+ (<code className="text-amber-400/80">rustup update</code>), 
                Git, and optionally <code className="text-amber-400/80">psql</code> for interactive sessions.
                FalconDB listens on port <code className="text-amber-400/80">5433</code> by default.
              </p>
            </div>
          </div>
        </div>
      </div>
    </section>
  )
}

function CopyButton({ text }) {
  const [copied, setCopied] = useState(false)

  const handleCopy = () => {
    navigator.clipboard.writeText(text)
    setCopied(true)
    setTimeout(() => setCopied(false), 2000)
  }

  return (
    <button
      onClick={handleCopy}
      className="flex items-center gap-1 px-2 py-1 rounded-md text-xs text-gray-500 hover:text-gray-300 hover:bg-white/[0.06] transition-all"
    >
      {copied ? (
        <>
          <Check className="w-3 h-3 text-emerald-400" />
          <span className="text-emerald-400">Copied</span>
        </>
      ) : (
        <>
          <Copy className="w-3 h-3" />
          Copy
        </>
      )}
    </button>
  )
}
