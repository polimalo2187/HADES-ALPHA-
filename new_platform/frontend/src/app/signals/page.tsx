'use client';

import { useState, useEffect } from 'react';
import { motion, AnimatePresence } from 'framer-motion';
import { 
  TrendingUp, 
  TrendingDown, 
  Clock, 
  DollarSign, 
  Target, 
  AlertTriangle,
  RefreshCw,
  Filter,
  ChevronDown,
  CheckCircle,
  XCircle
} from 'lucide-react';
import { useAuth } from '@/hooks/useAuth';
import { api } from '@/lib/api';

interface Signal {
  _id: string;
  type: 'LONG' | 'SHORT';
  symbol: string;
  entry_price: number;
  take_profit: number[];
  stop_loss: number;
  leverage: number;
  status: 'ACTIVE' | 'CLOSED' | 'PENDING';
  profit?: number;
  created_at: string;
  closed_at?: string;
  exchange: string;
  confidence: number;
  notes?: string;
}

const EXCHANGE_COLORS: Record<string, string> = {
  binance: 'bg-yellow-500',
  bybit: 'bg-orange-500',
  okx: 'bg-black',
  kucoin: 'bg-blue-500'
};

export default function SignalsPage() {
  const { user, token } = useAuth();
  const [signals, setSignals] = useState<Signal[]>([]);
  const [loading, setLoading] = useState(true);
  const [filter, setFilter] = useState<'ALL' | 'ACTIVE' | 'CLOSED'>('ALL');
  const [typeFilter, setTypeFilter] = useState<'ALL' | 'LONG' | 'SHORT'>('ALL');
  const [refreshing, setRefreshing] = useState(false);

  const fetchSignals = async () => {
    try {
      const response = await api.get('/api/signals', {
        headers: { Authorization: `Bearer ${token}` }
      });
      setSignals(response.data.signals || []);
    } catch (error) {
      console.error('Error fetching signals:', error);
    } finally {
      setLoading(false);
      setRefreshing(false);
    }
  };

  useEffect(() => {
    fetchSignals();
    const interval = setInterval(fetchSignals, 30000); // Refresh every 30s
    return () => clearInterval(interval);
  }, [token]);

  const filteredSignals = signals.filter(signal => {
    const statusMatch = filter === 'ALL' || signal.status === filter;
    const typeMatch = typeFilter === 'ALL' || signal.type === typeFilter;
    return statusMatch && typeMatch;
  });

  const stats = {
    total: signals.length,
    active: signals.filter(s => s.status === 'ACTIVE').length,
    closed: signals.filter(s => s.status === 'CLOSED').length,
    profitable: signals.filter(s => s.status === 'CLOSED' && (s.profit || 0) > 0).length,
    winRate: signals.filter(s => s.status === 'CLOSED').length > 0
      ? ((signals.filter(s => s.status === 'CLOSED' && (s.profit || 0) > 0).length / signals.filter(s => s.status === 'CLOSED').length) * 100).toFixed(1)
      : '0'
  };

  return (
    <div className="min-h-screen bg-gradient-to-br from-gray-950 via-purple-950 to-gray-950 p-4 md:p-8">
      {/* Header */}
      <motion.div 
        initial={{ opacity: 0, y: -20 }}
        animate={{ opacity: 1, y: 0 }}
        className="mb-8"
      >
        <div className="flex flex-col md:flex-row md:items-center md:justify-between gap-4">
          <div>
            <h1 className="text-3xl md:text-4xl font-bold text-white mb-2">
              Señales de Trading
            </h1>
            <p className="text-gray-400">
              Oportunidades en tiempo real de múltiples exchanges
            </p>
          </div>
          <button
            onClick={fetchSignals}
            disabled={refreshing}
            className="flex items-center gap-2 px-6 py-3 bg-gradient-to-r from-purple-600 to-cyan-600 rounded-xl font-semibold text-white hover:from-purple-700 hover:to-cyan-700 transition-all disabled:opacity-50"
          >
            <RefreshCw className={`w-5 h-5 ${refreshing ? 'animate-spin' : ''}`} />
            {refreshing ? 'Actualizando...' : 'Actualizar'}
          </button>
        </div>
      </motion.div>

      {/* Stats Cards */}
      <motion.div 
        initial={{ opacity: 0, y: 20 }}
        animate={{ opacity: 1, y: 0 }}
        transition={{ delay: 0.1 }}
        className="grid grid-cols-2 md:grid-cols-5 gap-4 mb-8"
      >
        {[
          { label: 'Total', value: stats.total, icon: TrendingUp, color: 'from-purple-500 to-pink-500' },
          { label: 'Activas', value: stats.active, icon: Clock, color: 'from-cyan-500 to-blue-500' },
          { label: 'Cerradas', value: stats.closed, icon: CheckCircle, color: 'from-gray-500 to-gray-700' },
          { label: 'Rentables', value: stats.profitable, icon: DollarSign, color: 'from-green-500 to-emerald-500' },
          { label: 'Win Rate', value: `${stats.winRate}%`, icon: Target, color: 'from-yellow-500 to-orange-500' }
        ].map((stat, index) => (
          <motion.div
            key={stat.label}
            initial={{ opacity: 0, scale: 0.9 }}
            animate={{ opacity: 1, scale: 1 }}
            transition={{ delay: 0.1 + index * 0.05 }}
            className="relative overflow-hidden backdrop-blur-xl bg-white/5 border border-white/10 rounded-2xl p-4 md:p-6"
          >
            <div className={`absolute top-0 right-0 w-20 h-20 bg-gradient-to-br ${stat.color} opacity-20 blur-2xl`} />
            <div className="relative">
              <stat.icon className="w-6 h-6 md:w-8 md:h-8 text-gray-400 mb-2" />
              <div className="text-2xl md:text-3xl font-bold text-white">{stat.value}</div>
              <div className="text-sm text-gray-400">{stat.label}</div>
            </div>
          </motion.div>
        ))}
      </motion.div>

      {/* Filters */}
      <motion.div 
        initial={{ opacity: 0, y: 20 }}
        animate={{ opacity: 1, y: 0 }}
        transition={{ delay: 0.2 }}
        className="flex flex-wrap gap-4 mb-6"
      >
        <div className="flex items-center gap-2 backdrop-blur-xl bg-white/5 border border-white/10 rounded-xl px-4 py-2">
          <Filter className="w-5 h-5 text-gray-400" />
          <span className="text-gray-400">Estado:</span>
          <select
            value={filter}
            onChange={(e) => setFilter(e.target.value as any)}
            className="bg-transparent text-white font-medium focus:outline-none cursor-pointer"
          >
            <option value="ALL" className="bg-gray-900">Todas</option>
            <option value="ACTIVE" className="bg-gray-900">Activas</option>
            <option value="CLOSED" className="bg-gray-900">Cerradas</option>
          </select>
        </div>

        <div className="flex items-center gap-2 backdrop-blur-xl bg-white/5 border border-white/10 rounded-xl px-4 py-2">
          <TrendingUp className="w-5 h-5 text-gray-400" />
          <span className="text-gray-400">Tipo:</span>
          <select
            value={typeFilter}
            onChange={(e) => setTypeFilter(e.target.value as any)}
            className="bg-transparent text-white font-medium focus:outline-none cursor-pointer"
          >
            <option value="ALL" className="bg-gray-900">Todos</option>
            <option value="LONG" className="bg-gray-900">Long</option>
            <option value="SHORT" className="bg-gray-900">Short</option>
          </select>
        </div>
      </motion.div>

      {/* Signals List */}
      {loading ? (
        <div className="flex items-center justify-center py-20">
          <RefreshCw className="w-12 h-12 text-purple-500 animate-spin" />
        </div>
      ) : filteredSignals.length === 0 ? (
        <motion.div 
          initial={{ opacity: 0 }}
          animate={{ opacity: 1 }}
          className="text-center py-20 backdrop-blur-xl bg-white/5 border border-white/10 rounded-2xl"
        >
          <AlertTriangle className="w-16 h-16 text-gray-600 mx-auto mb-4" />
          <h3 className="text-xl font-semibold text-white mb-2">No hay señales</h3>
          <p className="text-gray-400">No se encontraron señales con los filtros seleccionados</p>
        </motion.div>
      ) : (
        <div className="grid gap-4">
          <AnimatePresence>
            {filteredSignals.map((signal, index) => (
              <motion.div
                key={signal._id}
                initial={{ opacity: 0, y: 20 }}
                animate={{ opacity: 1, y: 0 }}
                exit={{ opacity: 0, y: -20 }}
                transition={{ delay: index * 0.05 }}
                className="backdrop-blur-xl bg-white/5 border border-white/10 rounded-2xl p-6 hover:bg-white/10 transition-all cursor-pointer"
              >
                <div className="flex flex-col lg:flex-row lg:items-center justify-between gap-4">
                  {/* Left: Type & Symbol */}
                  <div className="flex items-center gap-4">
                    <div className={`flex items-center justify-center w-16 h-16 rounded-2xl ${
                      signal.type === 'LONG' 
                        ? 'bg-gradient-to-br from-green-500 to-emerald-600' 
                        : 'bg-gradient-to-br from-red-500 to-rose-600'
                    }`}>
                      {signal.type === 'LONG' ? (
                        <TrendingUp className="w-8 h-8 text-white" />
                      ) : (
                        <TrendingDown className="w-8 h-8 text-white" />
                      )}
                    </div>
                    <div>
                      <h3 className="text-xl font-bold text-white">{signal.symbol}</h3>
                      <div className="flex items-center gap-2 mt-1">
                        <span className={`px-2 py-0.5 rounded text-xs font-semibold text-white ${EXCHANGE_COLORS[signal.exchange.toLowerCase()] || 'bg-gray-600'}`}>
                          {signal.exchange}
                        </span>
                        <span className={`px-2 py-0.5 rounded text-xs font-semibold ${
                          signal.status === 'ACTIVE' 
                            ? 'bg-green-500/20 text-green-400' 
                            : signal.status === 'CLOSED'
                            ? 'bg-gray-500/20 text-gray-400'
                            : 'bg-yellow-500/20 text-yellow-400'
                        }`}>
                          {signal.status}
                        </span>
                      </div>
                    </div>
                  </div>

                  {/* Center: Prices */}
                  <div className="grid grid-cols-3 gap-4 flex-1">
                    <div className="text-center">
                      <div className="text-sm text-gray-400 mb-1">Entrada</div>
                      <div className="text-lg font-bold text-white">${signal.entry_price.toLocaleString()}</div>
                    </div>
                    <div className="text-center">
                      <div className="text-sm text-gray-400 mb-1">Take Profit</div>
                      <div className="text-lg font-bold text-green-400">
                        ${signal.take_profit[0]?.toLocaleString()}
                      </div>
                    </div>
                    <div className="text-center">
                      <div className="text-sm text-gray-400 mb-1">Stop Loss</div>
                      <div className="text-lg font-bold text-red-400">
                        ${signal.stop_loss.toLocaleString()}
                      </div>
                    </div>
                  </div>

                  {/* Right: Leverage & Profit */}
                  <div className="flex items-center gap-6">
                    <div className="text-center">
                      <div className="text-sm text-gray-400 mb-1">Apalancamiento</div>
                      <div className="text-xl font-bold text-purple-400">{signal.leverage}x</div>
                    </div>
                    <div className="text-center">
                      <div className="text-sm text-gray-400 mb-1">Confianza</div>
                      <div className="text-xl font-bold text-cyan-400">{signal.confidence}%</div>
                    </div>
                    {signal.profit !== undefined && (
                      <div className="text-center">
                        <div className="text-sm text-gray-400 mb-1">Profit</div>
                        <div className={`text-xl font-bold ${signal.profit >= 0 ? 'text-green-400' : 'text-red-400'}`}>
                          {signal.profit >= 0 ? '+' : ''}{signal.profit.toFixed(2)}%
                        </div>
                      </div>
                    )}
                  </div>
                </div>

                {/* Expandable Details */}
                {signal.notes && (
                  <motion.div 
                    initial={{ height: 0, opacity: 0 }}
                    animate={{ height: 'auto', opacity: 1 }}
                    className="mt-4 pt-4 border-t border-white/10"
                  >
                    <p className="text-gray-400 text-sm">{signal.notes}</p>
                  </motion.div>
                )}

                {/* Timestamp */}
                <div className="mt-4 flex items-center gap-2 text-sm text-gray-500">
                  <Clock className="w-4 h-4" />
                  {new Date(signal.created_at).toLocaleString()}
                  {signal.closed_at && (
                    <>
                      <span>•</span>
                      <span>Cerrada: {new Date(signal.closed_at).toLocaleString()}</span>
                    </>
                  )}
                </div>
              </motion.div>
            ))}
          </AnimatePresence>
        </div>
      )}
    </div>
  );
}
