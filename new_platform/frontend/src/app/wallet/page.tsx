'use client';

import { useState, useEffect } from 'react';
import { motion, AnimatePresence } from 'framer-motion';
import {
  Wallet,
  ArrowUpRight,
  ArrowDownLeft,
  RefreshCw,
  Copy,
  ExternalLink,
  Clock,
  CheckCircle,
  AlertCircle,
  DollarSign,
  TrendingUp,
  History,
  QrCode
} from 'lucide-react';
import { useAuth } from '@/hooks/useAuth';
import { api } from '@/lib/api';

interface Transaction {
  _id: string;
  type: 'DEPOSIT' | 'WITHDRAWAL';
  amount: number;
  token: string;
  network: string;
  status: 'PENDING' | 'COMPLETED' | 'FAILED';
  tx_hash?: string;
  created_at: string;
}

interface Balance {
  token: string;
  balance: number;
  locked: number;
  available: number;
}

export default function WalletPage() {
  const { user, token } = useAuth();
  const [balances, setBalances] = useState<Balance[]>([]);
  const [transactions, setTransactions] = useState<Transaction[]>([]);
  const [loading, setLoading] = useState(true);
  const [depositAddress, setDepositAddress] = useState<string>('');
  const [showQR, setShowQR] = useState(false);

  const fetchWalletData = async () => {
    try {
      const [balanceRes, txRes] = await Promise.all([
        api.get('/api/users/balance', { headers: { Authorization: `Bearer ${token}` } }),
        api.get('/api/users/transactions', { headers: { Authorization: `Bearer ${token}` } })
      ]);
      
      setBalances(balanceRes.data.balances || []);
      setTransactions(txRes.data.transactions || []);
      
      // Get deposit address for USDT BEP20
      if (balanceRes.data.deposit_address) {
        setDepositAddress(balanceRes.data.deposit_address);
      }
    } catch (error) {
      console.error('Error fetching wallet data:', error);
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    fetchWalletData();
    const interval = setInterval(fetchWalletData, 30000);
    return () => clearInterval(interval);
  }, [token]);

  const copyToClipboard = (text: string) => {
    navigator.clipboard.writeText(text);
    alert('Dirección copiada al portapapeles');
  };

  const totalBalance = balances.reduce((sum, b) => sum + b.available, 0);

  return (
    <div className="min-h-screen bg-gradient-to-br from-gray-950 via-purple-950 to-gray-950 p-4 md:p-8">
      {/* Header */}
      <motion.div
        initial={{ opacity: 0, y: -20 }}
        animate={{ opacity: 1, y: 0 }}
        className="mb-8"
      >
        <h1 className="text-3xl md:text-4xl font-bold text-white mb-2">
          Mi Billetera
        </h1>
        <p className="text-gray-400">Gestiona tus fondos y transacciones</p>
      </motion.div>

      {/* Total Balance Card */}
      <motion.div
        initial={{ opacity: 0, scale: 0.95 }}
        animate={{ opacity: 1, scale: 1 }}
        className="relative overflow-hidden backdrop-blur-xl bg-gradient-to-br from-purple-600/20 to-cyan-600/20 border border-white/10 rounded-2xl p-6 md:p-8 mb-8"
      >
        <div className="absolute top-0 right-0 w-64 h-64 bg-purple-500/20 rounded-full blur-3xl" />
        <div className="absolute bottom-0 left-0 w-64 h-64 bg-cyan-500/20 rounded-full blur-3xl" />
        
        <div className="relative">
          <div className="flex items-center gap-3 mb-4">
            <Wallet className="w-8 h-8 text-purple-400" />
            <span className="text-gray-400 text-lg">Balance Total</span>
          </div>
          <div className="text-4xl md:text-5xl font-bold text-white mb-2">
            ${totalBalance.toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 })}
            <span className="text-xl text-gray-400 ml-2">USDT</span>
          </div>
          <div className="flex items-center gap-2 text-green-400">
            <TrendingUp className="w-5 h-5" />
            <span>+2.5% en las últimas 24h</span>
          </div>
        </div>
      </motion.div>

      {/* Action Buttons */}
      <motion.div
        initial={{ opacity: 0, y: 20 }}
        animate={{ opacity: 1, y: 0 }}
        transition={{ delay: 0.1 }}
        className="grid grid-cols-2 md:grid-cols-4 gap-4 mb-8"
      >
        {[
          { label: 'Depositar', icon: ArrowDownLeft, color: 'from-green-500 to-emerald-600', action: () => setShowQR(true) },
          { label: 'Retirar', icon: ArrowUpRight, color: 'from-red-500 to-rose-600', action: () => {} },
          { label: 'Comprar', icon: DollarSign, color: 'from-blue-500 to-cyan-600', action: () => {} },
          { label: 'Historial', icon: History, color: 'from-purple-500 to-pink-600', action: () => {} },
        ].map((action, index) => (
          <button
            key={action.label}
            onClick={action.action}
            className="backdrop-blur-xl bg-white/5 border border-white/10 rounded-2xl p-4 hover:bg-white/10 transition-all group"
          >
            <div className={`w-12 h-12 bg-gradient-to-br ${action.color} rounded-xl flex items-center justify-center mb-3 group-hover:scale-110 transition-transform`}>
              <action.icon className="w-6 h-6 text-white" />
            </div>
            <span className="text-white font-semibold">{action.label}</span>
          </button>
        ))}
      </motion.div>

      {/* Balances & Transactions */}
      <div className="grid lg:grid-cols-2 gap-6">
        {/* Balances */}
        <motion.div
          initial={{ opacity: 0, x: -20 }}
          animate={{ opacity: 1, x: 0 }}
          transition={{ delay: 0.2 }}
          className="backdrop-blur-xl bg-white/5 border border-white/10 rounded-2xl p-6"
        >
          <div className="flex items-center justify-between mb-6">
            <h2 className="text-xl font-bold text-white flex items-center gap-2">
              <DollarSign className="w-6 h-6 text-green-400" />
              Activos
            </h2>
            <button onClick={fetchWalletData} disabled={loading} className="p-2 hover:bg-white/10 rounded-lg transition-colors">
              <RefreshCw className={`w-5 h-5 text-gray-400 ${loading ? 'animate-spin' : ''}`} />
            </button>
          </div>

          {loading && balances.length === 0 ? (
            <div className="flex items-center justify-center py-12">
              <RefreshCw className="w-8 h-8 text-purple-500 animate-spin" />
            </div>
          ) : balances.length === 0 ? (
            <div className="text-center py-12">
              <AlertCircle className="w-12 h-12 text-gray-600 mx-auto mb-4" />
              <p className="text-gray-400">No hay activos disponibles</p>
            </div>
          ) : (
            <div className="space-y-4">
              {balances.map((balance, index) => (
                <motion.div
                  key={balance.token}
                  initial={{ opacity: 0, y: 10 }}
                  animate={{ opacity: 1, y: 0 }}
                  transition={{ delay: index * 0.05 }}
                  className="flex items-center justify-between p-4 bg-white/5 rounded-xl"
                >
                  <div className="flex items-center gap-4">
                    <div className="w-10 h-10 bg-gradient-to-br from-yellow-400 to-orange-500 rounded-full flex items-center justify-center">
                      <DollarSign className="w-5 h-5 text-white" />
                    </div>
                    <div>
                      <div className="font-semibold text-white">{balance.token}</div>
                      <div className="text-sm text-gray-400">Disponible: {balance.available.toLocaleString()}</div>
                    </div>
                  </div>
                  <div className="text-right">
                    <div className="font-bold text-white">${(balance.available).toLocaleString()}</div>
                    <div className="text-sm text-gray-400">En órdenes: {balance.locked.toLocaleString()}</div>
                  </div>
                </motion.div>
              ))}
            </div>
          )}
        </motion.div>

        {/* Recent Transactions */}
        <motion.div
          initial={{ opacity: 0, x: 20 }}
          animate={{ opacity: 1, x: 0 }}
          transition={{ delay: 0.2 }}
          className="backdrop-blur-xl bg-white/5 border border-white/10 rounded-2xl p-6"
        >
          <div className="flex items-center justify-between mb-6">
            <h2 className="text-xl font-bold text-white flex items-center gap-2">
              <History className="w-6 h-6 text-blue-400" />
              Transacciones Recientes
            </h2>
            <button className="text-sm text-purple-400 hover:text-purple-300">Ver todas</button>
          </div>

          {loading && transactions.length === 0 ? (
            <div className="flex items-center justify-center py-12">
              <RefreshCw className="w-8 h-8 text-purple-500 animate-spin" />
            </div>
          ) : transactions.length === 0 ? (
            <div className="text-center py-12">
              <Clock className="w-12 h-12 text-gray-600 mx-auto mb-4" />
              <p className="text-gray-400">No hay transacciones recientes</p>
            </div>
          ) : (
            <div className="space-y-3">
              {transactions.slice(0, 10).map((tx, index) => (
                <motion.div
                  key={tx._id}
                  initial={{ opacity: 0, x: 10 }}
                  animate={{ opacity: 1, x: 0 }}
                  transition={{ delay: index * 0.05 }}
                  className="flex items-center justify-between p-4 bg-white/5 rounded-xl hover:bg-white/10 transition-colors cursor-pointer"
                >
                  <div className="flex items-center gap-4">
                    <div className={`w-10 h-10 rounded-full flex items-center justify-center ${
                      tx.type === 'DEPOSIT' 
                        ? 'bg-green-500/20 text-green-400' 
                        : 'bg-red-500/20 text-red-400'
                    }`}>
                      {tx.type === 'DEPOSIT' ? <ArrowDownLeft className="w-5 h-5" /> : <ArrowUpRight className="w-5 h-5" />}
                    </div>
                    <div>
                      <div className="font-semibold text-white">
                        {tx.type === 'DEPOSIT' ? 'Depósito' : 'Retiro'}
                      </div>
                      <div className="text-sm text-gray-400">
                        {new Date(tx.created_at).toLocaleDateString()}
                      </div>
                    </div>
                  </div>
                  <div className="text-right">
                    <div className={`font-bold ${
                      tx.type === 'DEPOSIT' ? 'text-green-400' : 'text-red-400'
                    }`}>
                      {tx.type === 'DEPOSIT' ? '+' : '-'}{tx.amount.toLocaleString()} {tx.token}
                    </div>
                    <div className={`text-sm flex items-center justify-end gap-1 ${
                      tx.status === 'COMPLETED' ? 'text-green-400' :
                      tx.status === 'PENDING' ? 'text-yellow-400' : 'text-red-400'
                    }`}>
                      {tx.status === 'COMPLETED' && <CheckCircle className="w-3 h-3" />}
                      {tx.status === 'PENDING' && <Clock className="w-3 h-3" />}
                      {tx.status === 'FAILED' && <AlertCircle className="w-3 h-3" />}
                      {tx.status}
                    </div>
                  </div>
                </motion.div>
              ))}
            </div>
          )}
        </motion.div>
      </div>

      {/* Deposit Modal */}
      <AnimatePresence>
        {showQR && depositAddress && (
          <motion.div
            initial={{ opacity: 0 }}
            animate={{ opacity: 1 }}
            exit={{ opacity: 0 }}
            className="fixed inset-0 z-50 flex items-center justify-center p-4 bg-black/80 backdrop-blur-sm"
            onClick={() => setShowQR(false)}
          >
            <motion.div
              initial={{ scale: 0.9, opacity: 0 }}
              animate={{ scale: 1, opacity: 1 }}
              exit={{ scale: 0.9, opacity: 0 }}
              className="backdrop-blur-xl bg-gray-900/90 border border-white/10 rounded-2xl p-8 max-w-md w-full"
              onClick={(e) => e.stopPropagation()}
            >
              <div className="flex items-center justify-between mb-6">
                <h3 className="text-2xl font-bold text-white">Depositar USDT</h3>
                <button onClick={() => setShowQR(false)} className="p-2 hover:bg-white/10 rounded-lg">
                  <X className="w-6 h-6 text-gray-400" />
                </button>
              </div>

              <div className="text-center mb-6">
                <div className="inline-block p-4 bg-white rounded-2xl mb-4">
                  {/* Simple QR Code Placeholder - In production use a library like qrcode.react */}
                  <div className="w-48 h-48 bg-gradient-to-br from-purple-500 to-cyan-500 rounded-xl flex items-center justify-center">
                    <QrCode className="w-32 h-32 text-white" />
                  </div>
                </div>
                <p className="text-gray-400 text-sm mb-2">Escanea el código QR o copia la dirección</p>
                <p className="text-yellow-400 text-xs">Solo envía USDT en red BEP-20 (BSC)</p>
              </div>

              <div className="flex items-center gap-2 p-4 bg-white/5 rounded-xl mb-6">
                <code className="flex-1 text-sm text-white break-all font-mono">
                  {depositAddress}
                </code>
                <button
                  onClick={() => copyToClipboard(depositAddress)}
                  className="p-2 bg-purple-600 hover:bg-purple-700 rounded-lg transition-colors"
                >
                  <Copy className="w-5 h-5 text-white" />
                </button>
              </div>

              <div className="flex items-start gap-3 p-4 bg-yellow-500/10 border border-yellow-500/20 rounded-xl">
                <AlertCircle className="w-5 h-5 text-yellow-400 flex-shrink-0 mt-0.5" />
                <p className="text-sm text-yellow-400">
                  Asegúrate de enviar solo USDT en la red Binance Smart Chain (BEP-20). Enviar otros tokens puede resultar en pérdida permanente.
                </p>
              </div>
            </motion.div>
          </motion.div>
        )}
      </AnimatePresence>
    </div>
  );
}
