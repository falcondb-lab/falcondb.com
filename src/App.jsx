import { I18nProvider } from './i18n'
import Navbar from './components/Navbar'
import Hero from './components/Hero'
import Features from './components/Features'
import Architecture from './components/Architecture'
import Performance from './components/Performance'
import QuickStart from './components/QuickStart'
import Community from './components/Community'
import Footer from './components/Footer'

function App() {
  return (
    <I18nProvider>
      <div className="min-h-screen">
        <Navbar />
        <main>
          <Hero />
          <Features />
          <Architecture />
          <Performance />
          <QuickStart />
          <Community />
        </main>
        <Footer />
      </div>
    </I18nProvider>
  )
}

export default App
