import { useState, useEffect, useRef } from 'react'
import { Menu, X, Github, ExternalLink, Globe, ChevronDown, Sun, Moon } from 'lucide-react'
import { useI18n } from '../i18n'
import { useTheme } from '../theme'

export default function Navbar() {
  const { t, lang, setLanguage, langMeta } = useI18n()
  const { theme, toggleTheme } = useTheme()
  const [scrolled, setScrolled] = useState(false)
  const [mobileOpen, setMobileOpen] = useState(false)
  const [langOpen, setLangOpen] = useState(false)
  const langRef = useRef(null)

  const currentLang = langMeta.find((l) => l.code === lang)

  const navLinks = [
    { label: t.nav.features, href: '#features' },
    { label: t.nav.architecture, href: '#architecture' },
    { label: t.nav.performance, href: '#performance' },
    { label: t.nav.quickstart, href: '#quickstart' },
    { label: t.nav.community, href: '#community' },
  ]

  useEffect(() => {
    const onScroll = () => setScrolled(window.scrollY > 20)
    window.addEventListener('scroll', onScroll)
    return () => window.removeEventListener('scroll', onScroll)
  }, [])

  useEffect(() => {
    const onClick = (e) => {
      if (langRef.current && !langRef.current.contains(e.target)) {
        setLangOpen(false)
      }
    }
    document.addEventListener('mousedown', onClick)
    return () => document.removeEventListener('mousedown', onClick)
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
            <img src="/logo.png" alt="FalconDB" className="w-8 h-8 rounded-lg object-contain" />
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
            {/* Theme toggle */}
            <button
              onClick={toggleTheme}
              className="flex items-center justify-center w-9 h-9 text-gray-400 hover:text-white hover:bg-white/[0.06] rounded-lg transition-all"
              title={theme === 'dark' ? 'Light mode' : 'Dark mode'}
            >
              {theme === 'dark' ? <Sun className="w-4 h-4" /> : <Moon className="w-4 h-4" />}
            </button>
            {/* Language dropdown */}
            <div className="relative" ref={langRef}>
              <button
                onClick={() => setLangOpen(!langOpen)}
                className="flex items-center gap-1.5 px-3 py-2 text-sm text-gray-400 hover:text-white hover:bg-white/[0.06] rounded-lg transition-all"
              >
                <Globe className="w-3.5 h-3.5" />
                <span>{currentLang?.label}</span>
                <ChevronDown className={`w-3 h-3 transition-transform ${langOpen ? 'rotate-180' : ''}`} />
              </button>
              {langOpen && (
                <div className="absolute right-0 mt-2 w-44 py-1.5 bg-gray-900/95 backdrop-blur-xl border border-white/[0.1] rounded-xl shadow-2xl shadow-black/40 overflow-hidden">
                  {langMeta.map((l) => (
                    <button
                      key={l.code}
                      onClick={() => { setLanguage(l.code); setLangOpen(false) }}
                      className={`flex items-center gap-2.5 w-full px-4 py-2 text-sm transition-all ${
                        lang === l.code
                          ? 'text-falcon-400 bg-falcon-500/10'
                          : 'text-gray-400 hover:text-white hover:bg-white/[0.06]'
                      }`}
                    >
                      <span className="text-base">{l.flag}</span>
                      <span>{l.label}</span>
                    </button>
                  ))}
                </div>
              )}
            </div>
            <a
              href="https://github.com/falcondb-lab/falcondb/blob/main/docs/README.md"
              target="_blank"
              rel="noopener noreferrer"
              className="flex items-center gap-1.5 px-3.5 py-2 text-sm text-gray-400 hover:text-white transition-colors"
            >
              {t.nav.docs}
              <ExternalLink className="w-3.5 h-3.5" />
            </a>
            <a
              href="https://github.com/falcondb-lab/falcondb"
              target="_blank"
              rel="noopener noreferrer"
              className="flex items-center gap-2 px-4 py-2 bg-white/[0.08] hover:bg-white/[0.14] border border-white/[0.1] text-white text-sm font-medium rounded-lg transition-all"
            >
              <Github className="w-4 h-4" />
              {t.nav.github}
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
            {/* Mobile language list */}
            <div className="px-3 pt-3 pb-1">
              <p className="text-xs text-gray-500 uppercase tracking-wider mb-2 flex items-center gap-1.5">
                <Globe className="w-3 h-3" />
                Language
              </p>
              <div className="grid grid-cols-2 gap-1.5">
                {langMeta.map((l) => (
                  <button
                    key={l.code}
                    onClick={() => { setLanguage(l.code); setMobileOpen(false) }}
                    className={`flex items-center gap-2 px-3 py-2 rounded-lg text-sm transition-all ${
                      lang === l.code
                        ? 'text-falcon-400 bg-falcon-500/10 border border-falcon-500/20'
                        : 'text-gray-400 hover:text-white hover:bg-white/[0.06] border border-transparent'
                    }`}
                  >
                    <span>{l.flag}</span>
                    <span>{l.label}</span>
                  </button>
                ))}
              </div>
            </div>
            <div className="flex gap-2 pt-3 px-3">
              <button
                onClick={toggleTheme}
                className="flex items-center gap-2 px-4 py-2 text-sm text-gray-400 hover:text-white hover:bg-white/[0.06] rounded-lg transition-all"
              >
                {theme === 'dark' ? <Sun className="w-4 h-4" /> : <Moon className="w-4 h-4" />}
                {theme === 'dark' ? 'Light' : 'Dark'}
              </button>
              <a
                href="https://github.com/falcondb-lab/falcondb"
                target="_blank"
                rel="noopener noreferrer"
                className="flex items-center gap-2 px-4 py-2 bg-white/[0.08] border border-white/[0.1] text-white text-sm font-medium rounded-lg"
              >
                <Github className="w-4 h-4" />
                {t.nav.github}
              </a>
            </div>
          </div>
        )}
      </nav>
    </header>
  )
}
