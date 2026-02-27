import { useState, useEffect } from 'react'
import { Menu, X, Github, ExternalLink } from 'lucide-react'

const navLinks = [
  { label: 'Features', href: '#features' },
  { label: 'Architecture', href: '#architecture' },
  { label: 'Performance', href: '#performance' },
  { label: 'Quick Start', href: '#quickstart' },
  { label: 'Community', href: '#community' },
]

export default function Navbar() {
  const [scrolled, setScrolled] = useState(false)
  const [mobileOpen, setMobileOpen] = useState(false)

  useEffect(() => {
    const onScroll = () => setScrolled(window.scrollY > 20)
    window.addEventListener('scroll', onScroll)
    return () => window.removeEventListener('scroll', onScroll)
  }, [])

  return (
    <header
      className={`fixed top-0 left-0 right-0 z-50 transition-all duration-300 ${
        scrolled
          ? 'bg-gray-950/80 backdrop-blur-xl border-b border-white/[0.06]'
          : 'bg-transparent'
      }`}
    >
      <nav className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8">
        <div className="flex items-center justify-between h-16 lg:h-20">
          {/* Logo */}
          <a href="#" className="flex items-center gap-2.5 group">
            <div className="w-8 h-8 rounded-lg bg-gradient-to-br from-falcon-500 to-cyan-400 flex items-center justify-center text-white font-bold text-sm">
              F
            </div>
            <span className="text-xl font-bold text-white group-hover:text-falcon-400 transition-colors">
              FalconDB
            </span>
          </a>

          {/* Desktop links */}
          <div className="hidden lg:flex items-center gap-1">
            {navLinks.map((link) => (
              <a
                key={link.href}
                href={link.href}
                className="px-3.5 py-2 text-sm text-gray-400 hover:text-white rounded-lg hover:bg-white/[0.06] transition-all"
              >
                {link.label}
              </a>
            ))}
          </div>

          {/* Desktop actions */}
          <div className="hidden lg:flex items-center gap-3">
            <a
              href="https://github.com/falcondb-lab/falcondb/blob/main/docs/README.md"
              target="_blank"
              rel="noopener noreferrer"
              className="flex items-center gap-1.5 px-3.5 py-2 text-sm text-gray-400 hover:text-white transition-colors"
            >
              Docs
              <ExternalLink className="w-3.5 h-3.5" />
            </a>
            <a
              href="https://github.com/falcondb-lab/falcondb"
              target="_blank"
              rel="noopener noreferrer"
              className="flex items-center gap-2 px-4 py-2 bg-white/[0.08] hover:bg-white/[0.14] border border-white/[0.1] text-white text-sm font-medium rounded-lg transition-all"
            >
              <Github className="w-4 h-4" />
              GitHub
            </a>
          </div>

          {/* Mobile toggle */}
          <button
            onClick={() => setMobileOpen(!mobileOpen)}
            className="lg:hidden p-2 text-gray-400 hover:text-white"
          >
            {mobileOpen ? <X className="w-5 h-5" /> : <Menu className="w-5 h-5" />}
          </button>
        </div>

        {/* Mobile menu */}
        {mobileOpen && (
          <div className="lg:hidden pb-4 border-t border-white/[0.06] mt-2 pt-4 space-y-1">
            {navLinks.map((link) => (
              <a
                key={link.href}
                href={link.href}
                onClick={() => setMobileOpen(false)}
                className="block px-3 py-2.5 text-sm text-gray-400 hover:text-white hover:bg-white/[0.06] rounded-lg transition-all"
              >
                {link.label}
              </a>
            ))}
            <div className="flex gap-2 pt-3 px-3">
              <a
                href="https://github.com/falcondb-lab/falcondb"
                target="_blank"
                rel="noopener noreferrer"
                className="flex items-center gap-2 px-4 py-2 bg-white/[0.08] border border-white/[0.1] text-white text-sm font-medium rounded-lg"
              >
                <Github className="w-4 h-4" />
                GitHub
              </a>
            </div>
          </div>
        )}
      </nav>
    </header>
  )
}
