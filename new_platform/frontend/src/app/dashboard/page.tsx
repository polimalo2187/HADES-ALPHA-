'use client';

import { useEffect, useState } from 'react';
import { motion } from 'framer-motion';
import { 
  Zap, Bell, TrendingUp, DollarSign, Clock, 
  ArrowUpRight, ArrowDownRight, Menu, X, LogOut,
  User, Settings, Wallet, Activity
} from 'lucide-react';
import Link from 'next/link';
import { useRouter } from 'next/navigation';
import { toast, Toaster } from 'sonner';
import { useAuthStore, useSignalStore, useWebSocketStore } from '@/hooks/useStore';
import { LineChart, Line, XAxis, YAxis, Tooltip, ResponsiveContainer } from 'recharts';

const mockChartData = [
  { time: '00:00', value: 45000 },
  { time: '04:00', value: 47000 },
  { time: '08:00', value: 46500 },
  { time: '12:00', value: 48000 },
  { time: '16:00', value: 49500 },
  { time: '20:00', value: 51000 },
];

export default function DashboardPage() {
  const router = useRouter();
  const { user, isAuthenticated, logout } = useAuthStore();
  const { signals, unreadCount } = useSignalStore();
  const { isConnected, connect, disconnect } = useWebSocketStore();
  const [isMobileMenuOpen, setIsMobileMenuOpen] = useState(false);
  const [activeTab, setActiveTab] = useState('overview');

  useEffect(() => {
    if (!isAuthenticated) {
      router.push('/login');
      return;
    }

    // Connect to WebSocket
    const wsUrl = process.env.NEXT_PUBLIC_WS_URL || 'ws://localhost:8000/api/ws';
    const token = useAuthStore.getState().token;
    if (token) {
      connect(wsUrl, token);
    }

    return () => {
      disconnect();
    };
  }, [isAuthenticated, router, connect, disconnect]);

  const handleLogout = () => {
    logout();
    toast.success('Sesión cerrada correctamente');
    router.push('/');
  };

  const navItems = [
    { id: 'overview', label: 'Resumen', icon: Activity },
    { id: 'signals', label: 'Señales', icon: Zap },
    { id: 'market', label: 'Mercado', icon: TrendingUp },
    { id: 'payments', label: 'Pagos', icon: Wallet },
    { id: 'profile', label: 'Perfil', icon: User },
  ];

  return (
    <div className="min-h-screen bg-dark-950">
      <Toaster position="top-right" theme="dark" />
      
      {/* Background Effects */}
      <div className="fixed inset-0 overflow-hidden pointer-events-none">
        <div className="absolute top-0 -right-1/4 w-96 h-96 bg-primary-900/20 rounded-full blur-3xl" />
        <div className="absolute bottom-0 -left-1/4 w-96 h-96 bg-accent-900/20 rounded-full blur-3xl" />
      </div>

      {/* Header */}
      <header className="sticky top-0 z-50 glass border-b border-white/5">
        <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8">
          <div className="flex items-center justify-between h-16">
            {/* Logo */}
            <Link href="/dashboard" className="flex items-center gap-2">
              <div className="w-10 h-10 bg-gradient-to-br from-primary-500 to-accent-500 rounded-xl flex items-center justify-center glow-primary">
                <Zap className="w-6 h-6 text-white" />
              </div>
              <span className="text-xl font-bold gradient-text hidden sm:block">HADES</span>
            </Link>

            {/* Desktop Navigation */}
            <nav className="hidden md:flex items-center gap-1">
              {navItems.map((item) => (
                <button
                  key={item.id}
                  onClick={() => setActiveTab(item.id)}
                  className={`px-4 py-2 rounded-lg text-sm font-medium transition-all duration-200 flex items-center gap-2 ${
                    activeTab === item.id
                      ? 'bg-primary-500/20 text-primary-400'
                      : 'text-dark-400 hover:text-white hover:bg-white/5'
                  }`}
                >
                  <item.icon className="w-4 h-4" />
                  {item.label}
                </button>
              ))}
            </nav>

            {/* Right Side */}
            <div className="flex items-center gap-4">
              {/* Connection Status */}
              <div className="hidden sm:flex items-center gap-2 px-3 py-1.5 glass-light rounded-full text-xs">
                <div className={`w-2 h-2 rounded-full ${isConnected ? 'bg-green-500 animate-pulse' : 'bg-red-500'}`} />
                <span className="text-dark-300">{isConnected ? 'Conectado' : 'Desconectado'}</span>
              </div>

              {/* Notifications */}
              <button className="relative p-2 text-dark-400 hover:text-white transition-colors">
                <Bell className="w-5 h-5" />
                {unreadCount > 0 && (
                  <span className="absolute top-1 right-1 w-4 h-4 bg-primary-500 rounded-full text-xs text-white flex items-center justify-center">
                    {unreadCount}
                  </span>
                )}
              </button>

              {/* User Menu */}
              <div className="hidden md:flex items-center gap-3">
                <div className="text-right">
                  <div className="text-sm font-medium text-white">{user?.name || 'Usuario'}</div>
                  <div className="text-xs text-dark-400 capitalize">{user?.plan || 'free'} Plan</div>
                </div>
                <button onClick={handleLogout} className="p-2 text-dark-400 hover:text-red-400 transition-colors">
                  <LogOut className="w-5 h-5" />
                </button>
              </div>

              {/* Mobile Menu Button */}
              <button
                onClick={() => setIsMobileMenuOpen(!isMobileMenuOpen)}
                className="md:hidden p-2 text-dark-400 hover:text-white"
              >
                {isMobileMenuOpen ? <X className="w-6 h-6" /> : <Menu className="w-6 h-6" />}
              </button>
            </div>
          </div>
        </div>

        {/* Mobile Menu */}
        {isMobileMenuOpen && (
          <motion.div
            initial={{ opacity: 0, height: 0 }}
            animate={{ opacity: 1, height: 'auto' }}
            exit={{ opacity: 0, height: 0 }}
            className="md:hidden border-t border-white/5 glass"
          >
            <nav className="p-4 space-y-2">
              {navItems.map((item) => (
                <button
                  key={item.id}
                  onClick={() => {
                    setActiveTab(item.id);
                    setIsMobileMenuOpen(false);
                  }}
                  className={`w-full px-4 py-3 rounded-lg text-sm font-medium transition-all duration-200 flex items-center gap-3 ${
                    activeTab === item.id
                      ? 'bg-primary-500/20 text-primary-400'
                      : 'text-dark-400 hover:text-white hover:bg-white/5'
                  }`}
                >
                  <item.icon className="w-5 h-5" />
                  {item.label}
                </button>
              ))}
              <button
                onClick={handleLogout}
                className="w-full px-4 py-3 rounded-lg text-sm font-medium text-red-400 hover:bg-red-500/10 flex items-center gap-3"
              >
                <LogOut className="w-5 h-5" />
                Cerrar Sesión
              </button>
            </nav>
          </motion.div>
        )}
      </header>

      {/* Main Content */}
      <main className="relative z-10 max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 py-8">
        {/* Stats Grid */}
        <div className="grid grid-cols-2 md:grid-cols-4 gap-4 mb-8">
          {[
            { label: 'Señales Hoy', value: '12', icon: Zap, change: '+3', color: 'from-yellow-400 to-orange-500' },
            { label: 'Ganancia Est.', value: '$1,234', icon: DollarSign, change: '+12%', color: 'from-green-400 to-emerald-500' },
            { label: 'Arbitrajes', value: '8', icon: ArrowUpRight, change: '+2', color: 'from-blue-400 to-cyan-500' },
            { label: 'Volumen', value: '$45K', icon: Activity, change: '+18%', color: 'from-purple-400 to-pink-500' },
          ].map((stat, index) => (
            <motion.div
              key={index}
              initial={{ opacity: 0, y: 20 }}
              animate={{ opacity: 1, y: 0 }}
              transition={{ delay: index * 0.1 }}
              className="card"
            >
              <div className="flex items-center justify-between mb-3">
                <div className={`w-10 h-10 bg-gradient-to-br ${stat.color} rounded-lg flex items-center justify-center`}>
                  <stat.icon className="w-5 h-5 text-white" />
                </div>
                <span className="text-xs text-green-400 flex items-center gap-1">
                  <ArrowUpRight className="w-3 h-3" />
                  {stat.change}
                </span>
              </div>
              <div className="text-2xl font-bold text-white mb-1">{stat.value}</div>
              <div className="text-sm text-dark-400">{stat.label}</div>
            </motion.div>
          ))}
        </div>

        {/* Chart & Recent Signals */}
        <div className="grid lg:grid-cols-3 gap-6 mb-8">
          {/* Chart */}
          <motion.div
            initial={{ opacity: 0, x: -20 }}
            animate={{ opacity: 1, x: 0 }}
            className="lg:col-span-2 card"
          >
            <h3 className="text-lg font-semibold text-white mb-4">Rendimiento del Portafolio</h3>
            <div className="h-64">
              <ResponsiveContainer width="100%" height="100%">
                <LineChart data={mockChartData}>
                  <XAxis dataKey="time" stroke="#475569" fontSize={12} />
                  <YAxis stroke="#475569" fontSize={12} />
                  <Tooltip
                    contentStyle={{ 
                      backgroundColor: '#0F172A', 
                      border: '1px solid #1E293B',
                      borderRadius: '8px'
                    }}
                  />
                  <Line
                    type="monotone"
                    dataKey="value"
                    stroke="#8B5CF6"
                    strokeWidth={2}
                    dot={{ fill: '#8B5CF6', strokeWidth: 2 }}
                  />
                </LineChart>
              </ResponsiveContainer>
            </div>
          </motion.div>

          {/* Recent Signals */}
          <motion.div
            initial={{ opacity: 0, x: 20 }}
            animate={{ opacity: 1, x: 0 }}
            className="card"
          >
            <div className="flex items-center justify-between mb-4">
              <h3 className="text-lg font-semibold text-white">Señales Recientes</h3>
              <Link href="/signals" className="text-sm text-primary-400 hover:text-primary-300">
                Ver todas
              </Link>
            </div>
            <div className="space-y-3">
              {[1, 2, 3, 4].map((i) => (
                <div key={i} className="flex items-center gap-3 p-3 glass-light rounded-lg hover:bg-white/5 transition-colors cursor-pointer">
                  <div className={`w-8 h-8 rounded-lg flex items-center justify-center ${
                    i % 2 === 0 ? 'bg-green-500/20 text-green-400' : 'bg-red-500/20 text-red-400'
                  }`}>
                    {i % 2 === 0 ? <ArrowUpRight className="w-4 h-4" /> : <ArrowDownRight className="w-4 h-4" />}
                  </div>
                  <div className="flex-1 min-w-0">
                    <div className="text-sm font-medium text-white truncate">BTC/USDT</div>
                    <div className="text-xs text-dark-400">Binance → Bybit</div>
                  </div>
                  <div className="text-right">
                    <div className="text-sm font-semibold text-green-400">+{i * 0.5}%</div>
                    <div className="text-xs text-dark-400">Hace {i * 5}m</div>
                  </div>
                </div>
              ))}
            </div>
          </motion.div>
        </div>

        {/* Quick Actions */}
        <motion.div
          initial={{ opacity: 0, y: 20 }}
          animate={{ opacity: 1, y: 0 }}
          className="grid sm:grid-cols-2 lg:grid-cols-4 gap-4"
        >
          {[
            { label: 'Nueva Señal', icon: Zap, color: 'from-yellow-400 to-orange-500', href: '/signals' },
            { label: 'Ver Mercado', icon: TrendingUp, color: 'from-green-400 to-emerald-500', href: '/market' },
            { label: 'Recargar', icon: Wallet, color: 'from-blue-400 to-cyan-500', href: '/payments' },
            { label: 'Configuración', icon: Settings, color: 'from-purple-400 to-pink-500', href: '/profile' },
          ].map((action, index) => (
            <Link
              key={index}
              href={action.href}
              className="group card hover:border-primary-500/50 transition-all duration-300"
            >
              <div className="flex items-center gap-4">
                <div className={`w-12 h-12 bg-gradient-to-br ${action.color} rounded-xl flex items-center justify-center group-hover:scale-110 transition-transform duration-300`}>
                  <action.icon className="w-6 h-6 text-white" />
                </div>
                <div>
                  <div className="font-semibold text-white">{action.label}</div>
                  <div className="text-sm text-dark-400">Click para continuar</div>
                </div>
              </div>
            </Link>
          ))}
        </motion.div>
      </main>
    </div>
  );
}
