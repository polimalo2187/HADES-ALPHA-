import { useState, useEffect } from 'react'
import { motion } from 'framer-motion'
import { useAuthStore } from '../context/authStore'
import { LogOut, User, Settings, TrendingUp, Activity, Wallet, Bell } from 'lucide-react'

export default function Dashboard() {
  const { user, logout } = useAuthStore()
  const [activeTab, setActiveTab] = useState('home')

  const menuItems = [
    { id: 'home', icon: Activity, label: 'Inicio' },
    { id: 'signals', icon: TrendingUp, label: 'Señales' },
    { id: 'market', icon: TrendingUp, label: 'Mercado' },
    { id: 'account', icon: User, label: 'Cuenta' },
    { id: 'settings', icon: Settings, label: 'Configuración' },
  ]

  return (
    <div className="min-h-screen bg-background">
      {/* Header */}
      <header className="sticky top-0 z-50 glass-card border-b">
        <div className="container mx-auto px-4 py-4 flex items-center justify-between">
          <div>
            <h1 className="text-2xl font-bold gradient-text">HADES</h1>
            <p className="text-xs text-muted-foreground">Trading Platform</p>
          </div>
          <div className="flex items-center gap-3">
            <div className="text-right">
              <p className="text-sm font-semibold">{user?.username}</p>
              <p className="text-xs text-muted-foreground capitalize">{user?.plan}</p>
            </div>
            <button
              onClick={logout}
              className="p-2 hover:bg-secondary rounded-lg transition-colors"
            >
              <LogOut className="w-5 h-5" />
            </button>
          </div>
        </div>
      </header>

      {/* Main Content */}
      <main className="container mx-auto px-4 py-6 pb-24">
        <motion.div
          key={activeTab}
          initial={{ opacity: 0, y: 20 }}
          animate={{ opacity: 1, y: 0 }}
          transition={{ duration: 0.3 }}
        >
          {activeTab === 'home' && <HomeContent />}
          {activeTab === 'signals' && <SignalsContent />}
          {activeTab === 'market' && <MarketContent />}
          {activeTab === 'account' && <AccountContent />}
          {activeTab === 'settings' && <SettingsContent />}
        </motion.div>
      </main>

      {/* Bottom Navigation */}
      <nav className="fixed bottom-0 left-0 right-0 glass-card border-t">
        <div className="container mx-auto px-4 py-2 flex justify-around">
          {menuItems.map((item) => {
            const Icon = item.icon
            return (
              <button
                key={item.id}
                onClick={() => setActiveTab(item.id)}
                className={`flex flex-col items-center p-2 rounded-xl transition-all ${
                  activeTab === item.id
                    ? 'text-primary bg-secondary'
                    : 'text-muted-foreground hover:text-foreground'
                }`}
              >
                <Icon className="w-5 h-5 mb-1" />
                <span className="text-xs">{item.label}</span>
              </button>
            )
          })}
        </div>
      </nav>
    </div>
  )
}

function HomeContent() {
  return (
    <div className="space-y-6">
      {/* Stats Cards */}
      <div className="grid grid-cols-2 gap-4">
        <motion.div
          initial={{ scale: 0.9 }}
          animate={{ scale: 1 }}
          className="glass-card rounded-2xl p-4"
        >
          <p className="text-sm text-muted-foreground mb-1">Plan Actual</p>
          <p className="text-2xl font-bold gradient-text">FREE</p>
          <p className="text-xs text-muted-foreground mt-1">Trial de 5 días</p>
        </motion.div>
        <motion.div
          initial={{ scale: 0.9 }}
          animate={{ scale: 1 }}
          transition={{ delay: 0.1 }}
          className="glass-card rounded-2xl p-4"
        >
          <p className="text-sm text-muted-foreground mb-1">Señales Hoy</p>
          <p className="text-2xl font-bold">0</p>
          <p className="text-xs text-muted-foreground mt-1">Límite: 3/día</p>
        </motion.div>
      </div>

      {/* Quick Actions */}
      <div className="glass-card rounded-2xl p-4">
        <h3 className="text-lg font-semibold mb-4">Acciones Rápidas</h3>
        <div className="grid grid-cols-2 gap-3">
          <button className="p-3 bg-secondary rounded-xl hover:bg-secondary/80 transition-colors">
            <Bell className="w-6 h-6 mx-auto mb-2" />
            <p className="text-xs">Notificaciones</p>
          </button>
          <button className="p-3 bg-secondary rounded-xl hover:bg-secondary/80 transition-colors">
            <Wallet className="w-6 h-6 mx-auto mb-2" />
            <p className="text-xs">Pagos</p>
          </button>
        </div>
      </div>
    </div>
  )
}

function SignalsContent() {
  return (
    <div className="space-y-4">
      <h2 className="text-2xl font-bold">Señales en Vivo</h2>
      <div className="glass-card rounded-2xl p-8 text-center">
        <Activity className="w-12 h-12 mx-auto mb-4 text-muted-foreground" />
        <p className="text-muted-foreground">No hay señales activas en este momento</p>
      </div>
    </div>
  )
}

function MarketContent() {
  return (
    <div className="space-y-4">
      <h2 className="text-2xl font-bold">Mercado</h2>
      <div className="glass-card rounded-2xl p-8 text-center">
        <TrendingUp className="w-12 h-12 mx-auto mb-4 text-muted-foreground" />
        <p className="text-muted-foreground">Datos de mercado cargando...</p>
      </div>
    </div>
  )
}

function AccountContent() {
  const { user } = useAuthStore()
  
  return (
    <div className="space-y-4">
      <h2 className="text-2xl font-bold">Mi Cuenta</h2>
      <div className="glass-card rounded-2xl p-6 space-y-4">
        <div>
          <p className="text-sm text-muted-foreground">Usuario</p>
          <p className="text-lg font-semibold">{user?.username}</p>
        </div>
        <div>
          <p className="text-sm text-muted-foreground">Teléfono</p>
          <p className="text-lg font-semibold">{user?.phone_number}</p>
        </div>
        <div>
          <p className="text-sm text-muted-foreground">Plan</p>
          <p className="text-lg font-semibold capitalize">{user?.plan}</p>
        </div>
        <div>
          <p className="text-sm text-muted-foreground">Estado</p>
          <p className="text-lg font-semibold capitalize">{user?.subscription_status}</p>
        </div>
      </div>
    </div>
  )
}

function SettingsContent() {
  return (
    <div className="space-y-4">
      <h2 className="text-2xl font-bold">Configuración</h2>
      <div className="glass-card rounded-2xl p-6 space-y-4">
        <div className="flex items-center justify-between">
          <div>
            <p className="font-medium">Notificaciones Push</p>
            <p className="text-sm text-muted-foreground">Recibir alertas de señales</p>
          </div>
          <button className="w-12 h-6 bg-primary rounded-full relative">
            <div className="absolute right-1 top-1 w-4 h-4 bg-background rounded-full" />
          </button>
        </div>
      </div>
    </div>
  )
}
