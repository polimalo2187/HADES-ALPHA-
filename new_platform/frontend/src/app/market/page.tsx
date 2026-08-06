'use client';

import { useState, useEffect } from 'react';
import { motion, AnimatePresence } from 'framer-motion';
import { 
  TrendingUp, 
  TrendingDown, 
  RefreshCw, 
  Search,
  ArrowUpDown,
  BarChart3,
  Clock,
  DollarSign,
  Activity
} from 'lucide-react';
import { useAuth } from '@/hooks/useAuth';
import { api } from '@/lib/api';
import { LineChart, Line, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer, AreaChart, Area } from 'recharts';

interface MarketData {
  symbol: string;
  price: number;
  change24h: number;
  volume24h: number;
  high24h: number;
  low24h: number;
}

interface OrderBookEntry {
  price: number;
  amount: number;
  total: number;
}

interface KlineData {
  time: number;
  open: number;
  high: number;
  low: number;
  close: number;
  volume: number;
}

export default function MarketPage() {
  const { token } = useAuth();
  const [marketData, setMarketData] = useState<MarketData[]>([]);
  const [selectedSymbol, setSelectedSymbol] = useState<string>('BTC/USDT');
  const [selectedExchange, setSelectedExchange] = useState<string>('binance');
  const [orderBook, setOrderBook] = useState<{ bids: OrderBookEntry[], asks: OrderBookEntry[] }>({ bids: [], asks: [] });
  const [klines, setKlines] = useState<KlineData[]>([]);
  const [loading, setLoading] = useState(true);
  const [searchTerm, setSearchTerm] = useState('');
  const [timeframe, setTimeframe] = useState('1h');

  const fetchMarketData = async () => {
    try {
      const response = await api.get('/api/market/prices', {
        headers: { Authorization: `Bearer ${token}` }
      });
      setMarketData(response.data.prices || []);
    } catch (error) {
      console.error('Error fetching market data:', error);
    } finally {
      setLoading(false);
    }
  };

  const fetchOrderBook = async () => {
    try {
      const response = await api.get(`/api/market/orderbook?symbol=${selectedSymbol}&exchange=${selectedExchange}`, {
        headers: { Authorization: `Bearer ${token}` }
      });
      setOrderBook(response.data.orderbook || { bids: [], asks: [] });
    } catch (error) {
      console.error('Error fetching orderbook:', error);
    }
  };

  const fetchKlines = async () => {
    try {
      const response = await api.get(`/api/market/klines?symbol=${selectedSymbol}&exchange=${selectedExchange}&interval=${timeframe}`, {
        headers: { Authorization: `Bearer ${token}` }
      });
      const rawKlines = response.data.klines || [];
      const formattedKlines = rawKlines.map((k: any) => ({
        time: new Date(k.time).toLocaleTimeString(),
        open: k.open,
        high: k.high,
        low: k.low,
        close: k.close,
        volume: k.volume
      }));
      setKlines(formattedKlines);
    } catch (error) {
      console.error('Error fetching klines:', error);
    }
  };

  useEffect(() => {
    fetchMarketData();
    const interval = setInterval(fetchMarketData, 10000);
    return () => clearInterval(interval);
  }, [token]);

  useEffect(() => {
    if (selectedSymbol) {
      fetchOrderBook();
      fetchKlines();
    }
  }, [selectedSymbol, selectedExchange, timeframe, token]);

  const filteredSymbols = marketData.filter(data => 
    data.symbol.toLowerCase().includes(searchTerm.toLowerCase())
  );

  const selectedData = marketData.find(d => d.symbol === selectedSymbol);

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
              Mercado en Tiempo Real
            </h1>
            <p className="text-gray-400">
              Datos de múltiples exchanges al instante
            </p>
          </div>
          <button
            onClick={fetchMarketData}
            className="flex items-center gap-2 px-6 py-3 bg-gradient-to-r from-purple-600 to-cyan-600 rounded-xl font-semibold text-white hover:from-purple-700 hover:to-cyan-700 transition-all"
          >
            <RefreshCw className="w-5 h-5" />
            Actualizar
          </button>
        </div>
      </motion.div>

      {/* Search & Filter */}
      <motion.div 
        initial={{ opacity: 0, y: 20 }}
        animate={{ opacity: 1, y: 0 }}
        transition={{ delay: 0.1 }}
        className="mb-6 flex flex-wrap gap-4"
      >
        <div className="flex-1 min-w-[280px] relative">
          <Search className="absolute left-4 top-1/2 transform -translate-y-1/2 w-5 h-5 text-gray-400" />
          <input
            type="text"
            placeholder="Buscar símbolo..."
            value={searchTerm}
            onChange={(e) => setSearchTerm(e.target.value)}
            className="w-full pl-12 pr-4 py-3 backdrop-blur-xl bg-white/5 border border-white/10 rounded-xl text-white placeholder-gray-400 focus:outline-none focus:border-purple-500 transition-all"
          />
        </div>
        <select
          value={selectedExchange}
          onChange={(e) => setSelectedExchange(e.target.value)}
          className="px-6 py-3 backdrop-blur-xl bg-white/5 border border-white/10 rounded-xl text-white font-medium focus:outline-none focus:border-purple-500 cursor-pointer"
        >
          <option value="binance" className="bg-gray-900">Binance</option>
          <option value="bybit" className="bg-gray-900">Bybit</option>
          <option value="okx" className="bg-gray-900">OKX</option>
          <option value="kucoin" className="bg-gray-900">KuCoin</option>
        </select>
      </motion.div>

      <div className="grid lg:grid-cols-3 gap-6">
        {/* Symbols List */}
        <motion.div 
          initial={{ opacity: 0, x: -20 }}
          animate={{ opacity: 1, x: 0 }}
          transition={{ delay: 0.2 }}
          className="lg:col-span-1 backdrop-blur-xl bg-white/5 border border-white/10 rounded-2xl p-4 max-h-[600px] overflow-y-auto"
        >
          <h3 className="text-lg font-bold text-white mb-4 flex items-center gap-2">
            <Activity className="w-5 h-5 text-purple-400" />
            Símbolos
          </h3>
          {loading ? (
            <div className="flex justify-center py-8">
              <RefreshCw className="w-8 h-8 text-purple-500 animate-spin" />
            </div>
          ) : (
            <div className="space-y-2">
              {filteredSymbols.slice(0, 20).map((data) => (
                <motion.div
                  key={data.symbol}
                  whileHover={{ scale: 1.02 }}
                  onClick={() => setSelectedSymbol(data.symbol)}
                  className={`p-4 rounded-xl cursor-pointer transition-all ${
                    selectedSymbol === data.symbol
                      ? 'bg-gradient-to-r from-purple-600/20 to-cyan-600/20 border border-purple-500/50'
                      : 'bg-white/5 hover:bg-white/10 border border-transparent'
                  }`}
                >
                  <div className="flex items-center justify-between">
                    <div>
                      <div className="font-bold text-white">{data.symbol}</div>
                      <div className="text-sm text-gray-400">Vol: ${(data.volume24h / 1000000).toFixed(2)}M</div>
                    </div>
                    <div className="text-right">
                      <div className="font-bold text-white">${data.price.toLocaleString()}</div>
                      <div className={`text-sm font-semibold flex items-center gap-1 ${
                        data.change24h >= 0 ? 'text-green-400' : 'text-red-400'
                      }`}>
                        {data.change24h >= 0 ? <TrendingUp className="w-3 h-3" /> : <TrendingDown className="w-3 h-3" />}
                        {data.change24h >= 0 ? '+' : ''}{data.change24h.toFixed(2)}%
                      </div>
                    </div>
                  </div>
                </motion.div>
              ))}
            </div>
          )}
        </motion.div>

        {/* Price Chart & Details */}
        <motion.div 
          initial={{ opacity: 0, y: 20 }}
          animate={{ opacity: 1, y: 0 }}
          transition={{ delay: 0.3 }}
          className="lg:col-span-2 space-y-6"
        >
          {/* Selected Symbol Info */}
          {selectedData && (
            <div className="backdrop-blur-xl bg-white/5 border border-white/10 rounded-2xl p-6">
              <div className="flex flex-col md:flex-row md:items-center justify-between gap-4 mb-6">
                <div>
                  <h2 className="text-2xl font-bold text-white">{selectedData.symbol}</h2>
                  <p className="text-gray-400">{selectedExchange}</p>
                </div>
                <div className="text-right">
                  <div className="text-3xl font-bold text-white">${selectedData.price.toLocaleString()}</div>
                  <div className={`text-lg font-semibold flex items-center gap-2 justify-end ${
                    selectedData.change24h >= 0 ? 'text-green-400' : 'text-red-400'
                  }`}>
                    {selectedData.change24h >= 0 ? <TrendingUp className="w-5 h-5" /> : <TrendingDown className="w-5 h-5" />}
                    {selectedData.change24h >= 0 ? '+' : ''}{selectedData.change24h.toFixed(2)}% (24h)
                  </div>
                </div>
              </div>

              <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
                {[
                  { label: 'High 24h', value: `$${selectedData.high24h.toLocaleString()}`, icon: TrendingUp },
                  { label: 'Low 24h', value: `$${selectedData.low24h.toLocaleString()}`, icon: TrendingDown },
                  { label: 'Volumen 24h', value: `$${(selectedData.volume24h / 1000000).toFixed(2)}M`, icon: BarChart3 },
                  { label: 'Exchange', value: selectedExchange.charAt(0).toUpperCase() + selectedExchange.slice(1), icon: Activity }
                ].map((stat) => (
                  <div key={stat.label} className="bg-white/5 rounded-xl p-4">
                    <stat.icon className="w-5 h-5 text-gray-400 mb-2" />
                    <div className="text-sm text-gray-400">{stat.label}</div>
                    <div className="text-lg font-bold text-white">{stat.value}</div>
                  </div>
                ))}
              </div>
            </div>
          )}

          {/* Candlestick Chart */}
          <div className="backdrop-blur-xl bg-white/5 border border-white/10 rounded-2xl p-6">
            <div className="flex items-center justify-between mb-4">
              <h3 className="text-lg font-bold text-white flex items-center gap-2">
                <BarChart3 className="w-5 h-5 text-purple-400" />
                Gráfico de Precio
              </h3>
              <select
                value={timeframe}
                onChange={(e) => setTimeframe(e.target.value)}
                className="px-4 py-2 bg-white/5 border border-white/10 rounded-lg text-white text-sm focus:outline-none cursor-pointer"
              >
                <option value="1m" className="bg-gray-900">1m</option>
                <option value="5m" className="bg-gray-900">5m</option>
                <option value="15m" className="bg-gray-900">15m</option>
                <option value="1h" className="bg-gray-900">1h</option>
                <option value="4h" className="bg-gray-900">4h</option>
                <option value="1d" className="bg-gray-900">1d</option>
              </select>
            </div>
            <div className="h-[300px]">
              {klines.length > 0 ? (
                <ResponsiveContainer width="100%" height="100%">
                  <AreaChart data={klines}>
                    <defs>
                      <linearGradient id="colorPrice" x1="0" y1="0" x2="0" y2="1">
                        <stop offset="5%" stopColor="#8b5cf6" stopOpacity={0.3}/>
                        <stop offset="95%" stopColor="#8b5cf6" stopOpacity={0}/>
                      </linearGradient>
                    </defs>
                    <CartesianGrid strokeDasharray="3 3" stroke="#374151" opacity={0.5} />
                    <XAxis dataKey="time" stroke="#9ca3af" fontSize={12} />
                    <YAxis stroke="#9ca3af" fontSize={12} domain={['auto', 'auto']} />
                    <Tooltip 
                      contentStyle={{ 
                        backgroundColor: 'rgba(17, 24, 39, 0.95)', 
                        border: '1px solid rgba(255,255,255,0.1)',
                        borderRadius: '8px'
                      }} 
                    />
                    <Area type="monotone" dataKey="close" stroke="#8b5cf6" fillOpacity={1} fill="url(#colorPrice)" />
                  </AreaChart>
                </ResponsiveContainer>
              ) : (
                <div className="flex items-center justify-center h-full text-gray-400">
                  Cargando datos del gráfico...
                </div>
              )}
            </div>
          </div>
        </motion.div>
      </div>

      {/* Order Book */}
      <motion.div 
        initial={{ opacity: 0, y: 20 }}
        animate={{ opacity: 1, y: 0 }}
        transition={{ delay: 0.4 }}
        className="mt-6 backdrop-blur-xl bg-white/5 border border-white/10 rounded-2xl p-6"
      >
        <h3 className="text-lg font-bold text-white mb-4 flex items-center gap-2">
          <ArrowUpDown className="w-5 h-5 text-cyan-400" />
          Libro de Órdenes - {selectedSymbol}
        </h3>
        <div className="grid md:grid-cols-2 gap-6">
          {/* Bids */}
          <div>
            <h4 className="text-green-400 font-semibold mb-2 flex items-center gap-2">
              <TrendingUp className="w-4 h-4" />
              Compras (Bids)
            </h4>
            <div className="bg-black/20 rounded-xl overflow-hidden">
              <table className="w-full">
                <thead>
                  <tr className="text-xs text-gray-400 border-b border-white/10">
                    <th className="text-left p-2">Precio</th>
                    <th className="text-right p-2">Cantidad</th>
                    <th className="text-right p-2">Total</th>
                  </tr>
                </thead>
                <tbody>
                  {orderBook.bids.slice(0, 10).map((bid, index) => (
                    <tr key={index} className="text-sm border-b border-white/5 last:border-0">
                      <td className="p-2 text-green-400 font-mono">${bid.price.toLocaleString()}</td>
                      <td className="p-2 text-right text-white">{bid.amount.toFixed(4)}</td>
                      <td className="p-2 text-right text-gray-400">{bid.total.toFixed(2)}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </div>

          {/* Asks */}
          <div>
            <h4 className="text-red-400 font-semibold mb-2 flex items-center gap-2">
              <TrendingDown className="w-4 h-4" />
              Ventas (Asks)
            </h4>
            <div className="bg-black/20 rounded-xl overflow-hidden">
              <table className="w-full">
                <thead>
                  <tr className="text-xs text-gray-400 border-b border-white/10">
                    <th className="text-left p-2">Precio</th>
                    <th className="text-right p-2">Cantidad</th>
                    <th className="text-right p-2">Total</th>
                  </tr>
                </thead>
                <tbody>
                  {orderBook.asks.slice(0, 10).map((ask, index) => (
                    <tr key={index} className="text-sm border-b border-white/5 last:border-0">
                      <td className="p-2 text-red-400 font-mono">${ask.price.toLocaleString()}</td>
                      <td className="p-2 text-right text-white">{ask.amount.toFixed(4)}</td>
                      <td className="p-2 text-right text-gray-400">{ask.total.toFixed(2)}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          </div>
        </div>
      </motion.div>
    </div>
  );
}
