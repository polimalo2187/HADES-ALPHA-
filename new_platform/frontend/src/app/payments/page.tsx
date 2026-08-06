'use client';

import { useState, useEffect } from 'react';
import { motion, AnimatePresence } from 'framer-motion';
import { 
  CreditCard, 
  DollarSign, 
  Clock, 
  CheckCircle, 
  AlertCircle,
  RefreshCw,
  Copy,
  ExternalLink,
  Wallet,
  TrendingUp,
  History
} from 'lucide-react';
import { useAuth } from '@/hooks/useAuth';
import { api } from '@/lib/api';

interface PaymentOrder {
  _id: string;
  user_id: string;
  amount: number;
  token: string;
  network: string;
  address: string;
  status: 'PENDING' | 'COMPLETED' | 'FAILED' | 'EXPIRED';
  plan?: string;
  created_at: string;
  expires_at: string;
  tx_hash?: string;
}

const PLANS = [
  { id: 'basic', name: 'Basic', price: 50, features: ['10 señales/día', 'Soporte básico', '1 exchange'] },
  { id: 'pro', name: 'Pro', price: 100, features: ['30 señales/día', 'Soporte prioritario', '3 exchanges', 'Alertas push'] },
  { id: 'premium', name: 'Premium', price: 200, features: ['Señales ilimitadas', 'Soporte 24/7', 'Todos los exchanges', 'API access', 'Análisis avanzado'] }
];

export default function PaymentsPage() {
  const { user, token } = useAuth();
  const [orders, setOrders] = useState<PaymentOrder[]>([]);
  const [loading, setLoading] = useState(true);
  const [creatingOrder, setCreatingOrder] = useState(false);
  const [selectedPlan, setSelectedPlan] = useState<string>('pro');
  const [currentOrder, setCurrentOrder] = useState<PaymentOrder | null>(null);

  const fetchOrders = async () => {
    try {
      const response = await api.get('/api/payments/orders', {
        headers: { Authorization: `Bearer ${token}` }
      });
      setOrders(response.data.orders || []);
    } catch (error) {
      console.error('Error fetching orders:', error);
    } finally {
      setLoading(false);
    }
  };

  const createOrder = async () => {
    setCreatingOrder(true);
    try {
      const plan = PLANS.find(p => p.id === selectedPlan);
      if (!plan) return;

      const response = await api.post('/api/payments/create-order', {
        amount: plan.price,
        plan: plan.id,
        token: 'USDT',
        network: 'BEP20'
      }, {
        headers: { Authorization: `Bearer ${token}` }
      });

      setCurrentOrder(response.data.order);
      fetchOrders();
    } catch (error) {
      console.error('Error creating order:', error);
    } finally {
      setCreatingOrder(false);
    }
  };

  const copyToClipboard = (text: string) => {
    navigator.clipboard.writeText(text);
    // Could add toast notification here
  };

  useEffect(() => {
    fetchOrders();
    const interval = setInterval(fetchOrders, 30000);
    return () => clearInterval(interval);
  }, [token]);

  const pendingOrder = orders.find(o => o.status === 'PENDING');

  return (
    <div className="min-h-screen bg-gradient-to-br from-gray-950 via-purple-950 to-gray-950 p-4 md:p-8">
      {/* Header */}
      <motion.div 
        initial={{ opacity: 0, y: -20 }}
        animate={{ opacity: 1, y: 0 }}
        className="mb-8"
      >
        <h1 className="text-3xl md:text-4xl font-bold text-white mb-2">
          Pagos y Suscripciones
        </h1>
        <p className="text-gray-400">
          Gestiona tus pagos con USDT en la red BEP20
        </p>
      </motion.div>

      {/* Current Plan Info */}
      <motion.div 
        initial={{ opacity: 0, y: 20 }}
        animate={{ opacity: 1, y: 0 }}
        transition={{ delay: 0.1 }}
        className="mb-8 backdrop-blur-xl bg-gradient-to-r from-purple-600/20 to-cyan-600/20 border border-purple-500/30 rounded-2xl p-6"
      >
        <div className="flex items-center gap-4 mb-4">
          <Wallet className="w-8 h-8 text-purple-400" />
          <div>
            <h2 className="text-xl font-bold text-white">Tu Plan Actual</h2>
            <p className="text-gray-400">{user?.plan || 'Free'} {user?.plan_expires ? `• Vence: ${new Date(user.plan_expires).toLocaleDateString()}` : ''}</p>
          </div>
        </div>
        <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
          <div className="bg-black/20 rounded-xl p-4">
            <div className="text-sm text-gray-400 mb-1">Estado</div>
            <div className={`font-bold ${user?.is_premium ? 'text-green-400' : 'text-yellow-400'}`}>
              {user?.is_premium ? 'Premium Activo' : 'Plan Gratuito'}
            </div>
          </div>
          <div className="bg-black/20 rounded-xl p-4">
            <div className="text-sm text-gray-400 mb-1">Próximo Pago</div>
            <div className="font-bold text-white">
              {user?.plan_expires ? new Date(user.plan_expires).toLocaleDateString() : 'N/A'}
            </div>
          </div>
          <div className="bg-black/20 rounded-xl p-4">
            <div className="text-sm text-gray-400 mb-1">Total Pagado</div>
            <div className="font-bold text-white">
              ${orders.filter(o => o.status === 'COMPLETED').reduce((sum, o) => sum + o.amount, 0)}
            </div>
          </div>
          <div className="bg-black/20 rounded-xl p-4">
            <div className="text-sm text-gray-400 mb-1">Órdenes</div>
            <div className="font-bold text-white">{orders.length}</div>
          </div>
        </div>
      </motion.div>

      <div className="grid lg:grid-cols-2 gap-6">
        {/* Plans */}
        <motion.div 
          initial={{ opacity: 0, x: -20 }}
          animate={{ opacity: 1, x: 0 }}
          transition={{ delay: 0.2 }}
          className="space-y-4"
        >
          <h3 className="text-xl font-bold text-white flex items-center gap-2">
            <TrendingUp className="w-6 h-6 text-purple-400" />
            Selecciona tu Plan
          </h3>
          
          {PLANS.map((plan, index) => (
            <motion.div
              key={plan.id}
              initial={{ opacity: 0, y: 20 }}
              animate={{ opacity: 1, y: 0 }}
              transition={{ delay: 0.1 * index }}
              onClick={() => setSelectedPlan(plan.id)}
              className={`relative overflow-hidden backdrop-blur-xl border rounded-2xl p-6 cursor-pointer transition-all ${
                selectedPlan === plan.id
                  ? 'bg-gradient-to-r from-purple-600/20 to-cyan-600/20 border-purple-500/50'
                  : 'bg-white/5 border-white/10 hover:bg-white/10'
              }`}
            >
              {plan.id === 'pro' && (
                <div className="absolute top-2 right-2 px-3 py-1 bg-gradient-to-r from-purple-500 to-cyan-500 rounded-full text-xs font-bold text-white">
                  MÁS POPULAR
                </div>
              )}
              <div className="flex items-center justify-between mb-4">
                <div>
                  <h4 className="text-2xl font-bold text-white">{plan.name}</h4>
                  <p className="text-gray-400 text-sm">{plan.features.length} características incluidas</p>
                </div>
                <div className="text-right">
                  <div className="text-3xl font-bold text-white">${plan.price}</div>
                  <div className="text-gray-400 text-sm">/mes</div>
                </div>
              </div>
              <ul className="space-y-2">
                {plan.features.map((feature, i) => (
                  <li key={i} className="flex items-center gap-2 text-gray-300">
                    <CheckCircle className="w-4 h-4 text-green-400" />
                    {feature}
                  </li>
                ))}
              </ul>
            </motion.div>
          ))}

          <button
            onClick={createOrder}
            disabled={creatingOrder || !!pendingOrder}
            className="w-full py-4 bg-gradient-to-r from-purple-600 to-cyan-600 rounded-xl font-bold text-white text-lg hover:from-purple-700 hover:to-cyan-700 transition-all disabled:opacity-50 disabled:cursor-not-allowed flex items-center justify-center gap-2"
          >
            {creatingOrder ? (
              <>
                <RefreshCw className="w-5 h-5 animate-spin" />
                Procesando...
              </>
            ) : pendingOrder ? (
              <>
                <Clock className="w-5 h-5" />
                Ya tienes una orden pendiente
              </>
            ) : (
              <>
                <CreditCard className="w-5 h-5" />
                Crear Orden de Pago
              </>
            )}
          </button>
        </motion.div>

        {/* Payment Order */}
        <motion.div 
          initial={{ opacity: 0, x: 20 }}
          animate={{ opacity: 1, x: 0 }}
          transition={{ delay: 0.3 }}
        >
          {currentOrder || pendingOrder ? (
            <div className="backdrop-blur-xl bg-white/5 border border-white/10 rounded-2xl p-6">
              <h3 className="text-xl font-bold text-white mb-6 flex items-center gap-2">
                <DollarSign className="w-6 h-6 text-green-400" />
                Realizar Pago
              </h3>

              {(currentOrder || pendingOrder) && (
                <>
                  <div className="mb-6 p-4 bg-gradient-to-r from-purple-600/20 to-cyan-600/20 border border-purple-500/30 rounded-xl">
                    <div className="flex items-center justify-between mb-2">
                      <span className="text-gray-400">Monto a pagar</span>
                      <span className="text-2xl font-bold text-white">${(currentOrder || pendingOrder)?.amount}</span>
                    </div>
                    <div className="flex items-center justify-between">
                      <span className="text-gray-400">Token</span>
                      <span className="text-white font-semibold">{(currentOrder || pendingOrder)?.token} ({(currentOrder || pendingOrder)?.network})</span>
                    </div>
                  </div>

                  <div className="mb-6">
                    <label className="block text-sm text-gray-400 mb-2">
                      Dirección de depósito
                    </label>
                    <div className="flex items-center gap-2 p-4 bg-black/30 border border-white/10 rounded-xl">
                      <code className="flex-1 text-sm text-white break-all font-mono">
                        {(currentOrder || pendingOrder)?.address}
                      </code>
                      <button
                        onClick={() => copyToClipboard((currentOrder || pendingOrder)?.address || '')}
                        className="p-2 hover:bg-white/10 rounded-lg transition-all"
                        title="Copiar"
                      >
                        <Copy className="w-5 h-5 text-gray-400" />
                      </button>
                    </div>
                  </div>

                  <div className="mb-6 p-4 bg-yellow-500/10 border border-yellow-500/30 rounded-xl">
                    <div className="flex items-start gap-3">
                      <AlertCircle className="w-5 h-5 text-yellow-400 mt-0.5" />
                      <div className="text-sm text-yellow-200">
                        <p className="font-semibold mb-1">Importante:</p>
                        <ul className="list-disc list-inside space-y-1 text-yellow-300/80">
                          <li>Envía exactamente el monto mostrado</li>
                          <li>Usa la red BEP20 (BSC)</li>
                          <li>La orden expira en 30 minutos</li>
                          <li>La activación es automática tras confirmación</li>
                        </ul>
                      </div>
                    </div>
                  </div>

                  <div className="flex items-center justify-between p-4 bg-black/30 border border-white/10 rounded-xl mb-4">
                    <span className="text-gray-400">Estado</span>
                    <span className={`px-4 py-2 rounded-full text-sm font-bold ${
                      (currentOrder || pendingOrder)?.status === 'PENDING'
                        ? 'bg-yellow-500/20 text-yellow-400'
                        : (currentOrder || pendingOrder)?.status === 'COMPLETED'
                        ? 'bg-green-500/20 text-green-400'
                        : 'bg-red-500/20 text-red-400'
                    }`}>
                      {(currentOrder || pendingOrder)?.status}
                    </span>
                  </div>

                  {(currentOrder || pendingOrder)?.tx_hash && (
                    <a
                      href={`https://bscscan.com/tx/${(currentOrder || pendingOrder)?.tx_hash}`}
                      target="_blank"
                      rel="noopener noreferrer"
                      className="flex items-center justify-center gap-2 w-full py-3 bg-blue-600/20 border border-blue-500/30 rounded-xl text-blue-400 hover:bg-blue-600/30 transition-all"
                    >
                      <ExternalLink className="w-4 h-4" />
                      Ver en BSCScan
                    </a>
                  )}
                </>
              )}
            </div>
          ) : (
            <div className="backdrop-blur-xl bg-white/5 border border-white/10 rounded-2xl p-6 h-full flex items-center justify-center">
              <div className="text-center">
                <Wallet className="w-16 h-16 text-gray-600 mx-auto mb-4" />
                <h3 className="text-xl font-semibold text-white mb-2">Sin órdenes activas</h3>
                <p className="text-gray-400">Selecciona un plan y crea una orden para comenzar</p>
              </div>
            </div>
          )}
        </motion.div>
      </div>

      {/* Payment History */}
      <motion.div 
        initial={{ opacity: 0, y: 20 }}
        animate={{ opacity: 1, y: 0 }}
        transition={{ delay: 0.4 }}
        className="mt-8 backdrop-blur-xl bg-white/5 border border-white/10 rounded-2xl p-6"
      >
        <h3 className="text-xl font-bold text-white mb-6 flex items-center gap-2">
          <History className="w-6 h-6 text-cyan-400" />
          Historial de Pagos
        </h3>

        {loading ? (
          <div className="flex justify-center py-8">
            <RefreshCw className="w-8 h-8 text-purple-500 animate-spin" />
          </div>
        ) : orders.length === 0 ? (
          <div className="text-center py-8 text-gray-400">
            No hay historial de pagos
          </div>
        ) : (
          <div className="overflow-x-auto">
            <table className="w-full">
              <thead>
                <tr className="text-left text-sm text-gray-400 border-b border-white/10">
                  <th className="pb-3">Fecha</th>
                  <th className="pb-3">Plan</th>
                  <th className="pb-3">Monto</th>
                  <th className="pb-3">Token</th>
                  <th className="pb-3">Estado</th>
                  <th className="pb-3">TX Hash</th>
                </tr>
              </thead>
              <tbody>
                {orders.slice(0, 10).map((order) => (
                  <tr key={order._id} className="border-b border-white/5 last:border-0">
                    <td className="py-4 text-white">
                      {new Date(order.created_at).toLocaleDateString()}
                    </td>
                    <td className="py-4 text-white capitalize">{order.plan || '-'}</td>
                    <td className="py-4 font-bold text-white">${order.amount}</td>
                    <td className="py-4 text-gray-400">{order.token}</td>
                    <td className="py-4">
                      <span className={`px-3 py-1 rounded-full text-xs font-bold ${
                        order.status === 'COMPLETED'
                          ? 'bg-green-500/20 text-green-400'
                          : order.status === 'PENDING'
                          ? 'bg-yellow-500/20 text-yellow-400'
                          : 'bg-red-500/20 text-red-400'
                      }`}>
                        {order.status}
                      </span>
                    </td>
                    <td className="py-4">
                      {order.tx_hash ? (
                        <a
                          href={`https://bscscan.com/tx/${order.tx_hash}`}
                          target="_blank"
                          rel="noopener noreferrer"
                          className="text-purple-400 hover:text-purple-300 flex items-center gap-1"
                        >
                          <ExternalLink className="w-3 h-3" />
                          Ver
                        </a>
                      ) : (
                        <span className="text-gray-600">-</span>
                      )}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        )}
      </motion.div>
    </div>
  );
}
