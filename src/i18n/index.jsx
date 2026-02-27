import { createContext, useContext, useState, useCallback } from 'react'
import en from './en'
import zh from './zh'
import fr from './fr'
import de from './de'
import es from './es'
import ja from './ja'
import ko from './ko'

const languages = { en, zh, fr, de, es, ja, ko }

export const langMeta = [
  { code: 'en', label: 'English', flag: '🇺🇸' },
  { code: 'zh', label: '中文', flag: '🇨🇳' },
  { code: 'fr', label: 'Français', flag: '🇫🇷' },
  { code: 'de', label: 'Deutsch', flag: '🇩🇪' },
  { code: 'es', label: 'Español', flag: '🇪🇸' },
  { code: 'ja', label: '日本語', flag: '🇯🇵' },
  { code: 'ko', label: '한국어', flag: '🇰🇷' },
]

function detectLang() {
  if (typeof window === 'undefined') return 'en'
  const saved = localStorage.getItem('falcondb-lang')
  if (saved && languages[saved]) return saved
  const nav = navigator.language.toLowerCase()
  if (nav.startsWith('zh')) return 'zh'
  if (nav.startsWith('fr')) return 'fr'
  if (nav.startsWith('de')) return 'de'
  if (nav.startsWith('es')) return 'es'
  if (nav.startsWith('ja')) return 'ja'
  if (nav.startsWith('ko')) return 'ko'
  return 'en'
}

const I18nContext = createContext()

export function I18nProvider({ children }) {
  const [lang, setLang] = useState(detectLang)

  const setLanguage = useCallback((l) => {
    if (languages[l]) {
      setLang(l)
      localStorage.setItem('falcondb-lang', l)
    }
  }, [])

  const t = languages[lang]

  return (
    <I18nContext.Provider value={{ lang, t, setLanguage, langMeta }}>
      {children}
    </I18nContext.Provider>
  )
}

export function useI18n() {
  const ctx = useContext(I18nContext)
  if (!ctx) throw new Error('useI18n must be used within I18nProvider')
  return ctx
}
