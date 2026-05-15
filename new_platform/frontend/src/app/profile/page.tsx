'use client';

import { useState, useEffect } from 'react';
import { motion } from 'framer-motion';
import { 
  User, 
  Mail, 
  Phone, 
  Shield, 
  CreditCard, 
  Clock, 
  Edit2, 
  Save, 
  X,
  LogOut,
  Key,
  Bell,
  CheckCircle,
  AlertCircle
} from 'lucide-react';
import { useAuth } from '@/hooks/useAuth';
import { api } from '@/lib/api';
import { useRouter } from 'next/navigation';

export default function ProfilePage() {
  const { user, token, logout } = useAuth();
  const router = useRouter();
  const [editing, setEditing] = useState(false);
  const [formData, setFormData] = useState({
    phone: '',
    email: '',
    current_password: '',
    new_password: '',
    confirm_password: ''
  });
  const [loading, setLoading] = useState(false);
  const [message, setMessage] = useState<{ type: 'success' | 'error', text: string } | null>(null);

  useEffect(() => {
    if (user) {
      setFormData(prev => ({
        ...prev,
        phone: user.phone || '',
        email: user.email || ''
      }));
    }
  }, [user]);

  const handleUpdateProfile = async () => {
    setLoading(true);
    setMessage(null);
    try {
      const updateData: any = {};
      if (formData.phone !== user?.phone) updateData.phone = formData.phone;
      if (formData.email !== user?.email) updateData.email = formData.email;

      if (Object.keys(updateData).length > 0) {
        await api.put('/api/users/profile', updateData, {
          headers: { Authorization: `Bearer ${token}` }
        });
      }

      setMessage({ type: 'success', text: 'Perfil actualizado correctamente' });
      setEditing(false);
    } catch (error: any) {
      setMessage({ type: 'error', text: error.response?.data?.detail || 'Error al actualizar perfil' });
    } finally {
      setLoading(false);
    }
  };

  const handleChangePassword = async () => {
    if (formData.new_password !== formData.confirm_password) {
      setMessage({ type: 'error', text: 'Las contraseñas no coinciden' });
      return;
    }

    if (formData.new_password.length < 6) {
      setMessage({ type: 'error', text: 'La contraseña debe tener al menos 6 caracteres' });
      return;
    }

    setLoading(true);
    setMessage(null);
    try {
      await api.put('/api/users/change-password', {
        current_password: formData.current_password,
        new_password: formData.new_password
      }, {
        headers: { Authorization: `Bearer ${token}` }
      });

      setMessage({ type: 'success', text: 'Contraseña cambiada exitosamente' });
      setFormData(prev => ({
        ...prev,
        current_password: '',
        new_password: '',
        confirm_password: ''
      }));
    } catch (error: any) {
      setMessage({ type: 'error', text: error.response?.data?.detail || 'Error al cambiar contraseña' });
    } finally {
      setLoading(false);
    }
  };

  const handleLogout = () => {
    logout();
    router.push('/login');
  };

  return (
    <div className="min-h-screen bg-gradient-to-br from-gray-950 via-purple-950 to-gray-950 p-4 md:p-8">
      {/* Header */}
      <motion.div 
        initial={{ opacity: 0, y: -20 }}
        animate={{ opacity: 1, y: 0 }}
        className="mb-8"
      >
        <h1 className="text-3xl md:text-4xl font-bold text-white mb-2">
          Mi Perfil
        </h1>
        <p className="text-gray-400">
          Gestiona tu cuenta y configuración
        </p>
      </motion.div>

      {message && (
        <motion.div 
          initial={{ opacity: 0, y: -10 }}
          animate={{ opacity: 1, y: 0 }}
          className={`mb-6 p-4 rounded-xl flex items-center gap-3 ${
            message.type === 'success' 
              ? 'bg-green-500/20 border border-green-500/30 text-green-400' 
              : 'bg-red-500/20 border border-red-500/30 text-red-400'
          }`}
        >
          {message.type === 'success' ? <CheckCircle className="w-5 h-5" /> : <AlertCircle className="w-5 h-5" />}
          {message.text}
        </motion.div>
      )}

      <div className="grid lg:grid-cols-3 gap-6">
        {/* Profile Info Card */}
        <motion.div 
          initial={{ opacity: 0, x: -20 }}
          animate={{ opacity: 1, x: 0 }}
          transition={{ delay: 0.1 }}
          className="lg:col-span-1"
        >
          <div className="backdrop-blur-xl bg-gradient-to-br from-purple-600/20 to-cyan-600/20 border border-purple-500/30 rounded-2xl p-6 text-center">
            <div className="w-24 h-24 mx-auto mb-4 rounded-full bg-gradient-to-br from-purple-500 to-cyan-500 flex items-center justify-center">
              <User className="w-12 h-12 text-white" />
            </div>
            <h2 className="text-2xl font-bold text-white mb-1">
              {user?.phone || 'Usuario'}
            </h2>
            <p className="text-gray-400 mb-4">
              {user?.email || 'Sin email'}
            </p>
            <div className="inline-flex items-center gap-2 px-4 py-2 bg-gradient-to-r from-purple-500 to-cyan-500 rounded-full text-sm font-bold text-white">
              <CreditCard className="w-4 h-4" />
              Plan {user?.plan || 'Free'}
            </div>
            
            {user?.plan_expires && (
              <div className="mt-4 pt-4 border-t border-white/10">
                <div className="flex items-center justify-center gap-2 text-sm text-gray-400">
                  <Clock className="w-4 h-4" />
                  Vence: {new Date(user.plan_expires).toLocaleDateString()}
                </div>
              </div>
            )}

            <button
              onClick={handleLogout}
              className="mt-6 w-full py-3 bg-red-500/20 border border-red-500/30 rounded-xl text-red-400 hover:bg-red-500/30 transition-all flex items-center justify-center gap-2 font-semibold"
            >
              <LogOut className="w-5 h-5" />
              Cerrar Sesión
            </button>
          </div>

          {/* Stats */}
          <div className="mt-6 grid grid-cols-2 gap-4">
            <div className="backdrop-blur-xl bg-white/5 border border-white/10 rounded-2xl p-4 text-center">
              <Bell className="w-6 h-6 text-purple-400 mx-auto mb-2" />
              <div className="text-2xl font-bold text-white">0</div>
              <div className="text-xs text-gray-400">Notificaciones</div>
            </div>
            <div className="backdrop-blur-xl bg-white/5 border border-white/10 rounded-2xl p-4 text-center">
              <Shield className="w-6 h-6 text-cyan-400 mx-auto mb-2" />
              <div className="text-2xl font-bold text-white">{user?.is_premium ? 'Premium' : 'Free'}</div>
              <div className="text-xs text-gray-400">Estado</div>
            </div>
          </div>
        </motion.div>

        {/* Profile Settings */}
        <motion.div 
          initial={{ opacity: 0, y: 20 }}
          animate={{ opacity: 1, y: 0 }}
          transition={{ delay: 0.2 }}
          className="lg:col-span-2 space-y-6"
        >
          {/* Personal Information */}
          <div className="backdrop-blur-xl bg-white/5 border border-white/10 rounded-2xl p-6">
            <div className="flex items-center justify-between mb-6">
              <h3 className="text-xl font-bold text-white flex items-center gap-2">
                <User className="w-6 h-6 text-purple-400" />
                Información Personal
              </h3>
              {!editing ? (
                <button
                  onClick={() => setEditing(true)}
                  className="flex items-center gap-2 px-4 py-2 bg-purple-600/20 border border-purple-500/30 rounded-xl text-purple-400 hover:bg-purple-600/30 transition-all"
                >
                  <Edit2 className="w-4 h-4" />
                  Editar
                </button>
              ) : (
                <div className="flex gap-2">
                  <button
                    onClick={() => {
                      setEditing(false);
                      setFormData(prev => ({ ...prev, phone: user?.phone || '', email: user?.email || '' }));
                    }}
                    className="flex items-center gap-2 px-4 py-2 bg-gray-600/20 border border-gray-500/30 rounded-xl text-gray-400 hover:bg-gray-600/30 transition-all"
                  >
                    <X className="w-4 h-4" />
                    Cancelar
                  </button>
                  <button
                    onClick={handleUpdateProfile}
                    disabled={loading}
                    className="flex items-center gap-2 px-4 py-2 bg-gradient-to-r from-purple-600 to-cyan-600 rounded-xl text-white hover:from-purple-700 hover:to-cyan-700 transition-all disabled:opacity-50"
                  >
                    <Save className="w-4 h-4" />
                    Guardar
                  </button>
                </div>
              )}
            </div>

            <div className="space-y-4">
              <div>
                <label className="block text-sm text-gray-400 mb-2">
                  <Phone className="w-4 h-4 inline mr-1" />
                  Teléfono
                </label>
                <input
                  type="tel"
                  value={formData.phone}
                  onChange={(e) => setFormData(prev => ({ ...prev, phone: e.target.value }))}
                  disabled={!editing}
                  className="w-full px-4 py-3 bg-black/30 border border-white/10 rounded-xl text-white disabled:text-gray-500 disabled:cursor-not-allowed focus:outline-none focus:border-purple-500 transition-all"
                />
              </div>

              <div>
                <label className="block text-sm text-gray-400 mb-2">
                  <Mail className="w-4 h-4 inline mr-1" />
                  Email (opcional)
                </label>
                <input
                  type="email"
                  value={formData.email}
                  onChange={(e) => setFormData(prev => ({ ...prev, email: e.target.value }))}
                  disabled={!editing}
                  className="w-full px-4 py-3 bg-black/30 border border-white/10 rounded-xl text-white disabled:text-gray-500 disabled:cursor-not-allowed focus:outline-none focus:border-purple-500 transition-all"
                />
              </div>

              <div className="pt-4 border-t border-white/10">
                <div className="flex items-center gap-2 text-sm text-gray-400">
                  <Clock className="w-4 h-4" />
                  Miembro desde: {user?.created_at ? new Date(user.created_at).toLocaleDateString() : 'N/A'}
                </div>
              </div>
            </div>
          </div>

          {/* Change Password */}
          <div className="backdrop-blur-xl bg-white/5 border border-white/10 rounded-2xl p-6">
            <h3 className="text-xl font-bold text-white mb-6 flex items-center gap-2">
              <Key className="w-6 h-6 text-cyan-400" />
              Cambiar Contraseña
            </h3>

            <div className="space-y-4">
              <div>
                <label className="block text-sm text-gray-400 mb-2">
                  Contraseña Actual
                </label>
                <input
                  type="password"
                  value={formData.current_password}
                  onChange={(e) => setFormData(prev => ({ ...prev, current_password: e.target.value }))}
                  placeholder="••••••"
                  className="w-full px-4 py-3 bg-black/30 border border-white/10 rounded-xl text-white placeholder-gray-600 focus:outline-none focus:border-purple-500 transition-all"
                />
              </div>

              <div>
                <label className="block text-sm text-gray-400 mb-2">
                  Nueva Contraseña
                </label>
                <input
                  type="password"
                  value={formData.new_password}
                  onChange={(e) => setFormData(prev => ({ ...prev, new_password: e.target.value }))}
                  placeholder="Mínimo 6 caracteres"
                  className="w-full px-4 py-3 bg-black/30 border border-white/10 rounded-xl text-white placeholder-gray-600 focus:outline-none focus:border-purple-500 transition-all"
                />
              </div>

              <div>
                <label className="block text-sm text-gray-400 mb-2">
                  Confirmar Nueva Contraseña
                </label>
                <input
                  type="password"
                  value={formData.confirm_password}
                  onChange={(e) => setFormData(prev => ({ ...prev, confirm_password: e.target.value }))}
                  placeholder="Repite la nueva contraseña"
                  className="w-full px-4 py-3 bg-black/30 border border-white/10 rounded-xl text-white placeholder-gray-600 focus:outline-none focus:border-purple-500 transition-all"
                />
              </div>

              <button
                onClick={handleChangePassword}
                disabled={loading || !formData.current_password || !formData.new_password || !formData.confirm_password}
                className="w-full py-3 bg-gradient-to-r from-purple-600 to-cyan-600 rounded-xl font-bold text-white hover:from-purple-700 hover:to-cyan-700 transition-all disabled:opacity-50 disabled:cursor-not-allowed"
              >
                {loading ? 'Procesando...' : 'Actualizar Contraseña'}
              </button>
            </div>
          </div>

          {/* Account Info */}
          <div className="backdrop-blur-xl bg-white/5 border border-white/10 rounded-2xl p-6">
            <h3 className="text-xl font-bold text-white mb-6 flex items-center gap-2">
              <Shield className="w-6 h-6 text-purple-400" />
              Información de Cuenta
            </h3>

            <div className="grid md:grid-cols-2 gap-4">
              {[
                { label: 'ID Usuario', value: user?._id || 'N/A' },
                { label: 'Teléfono', value: user?.phone || 'No verificado' },
                { label: 'Email', value: user?.email || 'No agregado' },
                { label: 'Plan', value: user?.plan || 'Free', highlight: true },
                { label: 'Estado Premium', value: user?.is_premium ? 'Activo' : 'Inactivo', highlight: user?.is_premium },
                { label: 'Vencimiento', value: user?.plan_expires ? new Date(user.plan_expires).toLocaleDateString() : 'N/A' }
              ].map((item) => (
                <div key={item.label} className="p-4 bg-black/20 rounded-xl">
                  <div className="text-sm text-gray-400 mb-1">{item.label}</div>
                  <div className={`font-bold ${item.highlight ? 'text-purple-400' : 'text-white'}`}>
                    {item.value}
                  </div>
                </div>
              ))}
            </div>
          </div>
        </motion.div>
      </div>
    </div>
  );
}
