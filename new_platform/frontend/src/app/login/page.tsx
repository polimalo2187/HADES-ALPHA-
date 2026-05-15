'use client';

import { useState } from 'react';
import { motion } from 'framer-motion';
import { Zap, Smartphone, Lock, Eye, EyeOff, ChevronRight, CheckCircle } from 'lucide-react';
import Link from 'next/link';
import { useRouter } from 'next/navigation';
import { toast, Toaster } from 'sonner';
import { useAuthStore } from '@/hooks/useStore';

export default function LoginPage() {
  const router = useRouter();
  const login = useAuthStore((state) => state.login);
  const [phone, setPhone] = useState('');
  const [password, setPassword] = useState('');
  const [showPassword, setShowPassword] = useState(false);
  const [isLoading, setIsLoading] = useState(false);

  const handleSubmit = async (e: React.FormEvent) => {
    e.preventDefault();
    setIsLoading(true);

    try {
      const response = await fetch(`${process.env.NEXT_PUBLIC_API_URL}/api/auth/login`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ phone, password }),
      });

      const data = await response.json();

      if (!response.ok) {
        throw new Error(data.detail || 'Error al iniciar sesión');
      }

      login(data.access_token, data.user);
      toast.success('¡Bienvenido de nuevo!');
      router.push('/dashboard');
    } catch (error: any) {
      toast.error(error.message || 'Error al iniciar sesión');
    } finally {
      setIsLoading(false);
    }
  };

  return (
    <div className="min-h-screen bg-dark-950 flex items-center justify-center p-4 relative overflow-hidden">
      <Toaster position="top-center" theme="dark" />
      
      {/* Background Effects */}
      <div className="fixed inset-0 overflow-hidden pointer-events-none">
        <div className="absolute top-1/4 -left-1/4 w-96 h-96 bg-primary-900/30 rounded-full blur-3xl animate-float" />
        <div className="absolute bottom-1/4 -right-1/4 w-96 h-96 bg-accent-900/30 rounded-full blur-3xl animate-float" style={{ animationDelay: '2s' }} />
      </div>

      <motion.div
        initial={{ opacity: 0, y: 20 }}
        animate={{ opacity: 1, y: 0 }}
        transition={{ duration: 0.5 }}
        className="w-full max-w-md relative z-10"
      >
        {/* Logo */}
        <div className="text-center mb-8">
          <Link href="/" className="inline-flex items-center gap-2 mb-2">
            <div className="w-12 h-12 bg-gradient-to-br from-primary-500 to-accent-500 rounded-xl flex items-center justify-center glow-primary">
              <Zap className="w-7 h-7 text-white" />
            </div>
            <span className="text-3xl font-bold gradient-text">HADES</span>
          </Link>
          <p className="text-dark-400">Inicia sesión para continuar</p>
        </div>

        {/* Login Card */}
        <div className="card">
          <form onSubmit={handleSubmit} className="space-y-6">
            {/* Phone Input */}
            <div>
              <label className="block text-sm font-medium text-dark-300 mb-2">
                Teléfono
              </label>
              <div className="relative">
                <Smartphone className="absolute left-4 top-1/2 -translate-y-1/2 w-5 h-5 text-dark-400" />
                <input
                  type="tel"
                  value={phone}
                  onChange={(e) => setPhone(e.target.value)}
                  placeholder="+1234567890"
                  className="input pl-12"
                  required
                  pattern="^\+?[1-9]\d{1,14}$"
                />
              </div>
            </div>

            {/* Password Input */}
            <div>
              <label className="block text-sm font-medium text-dark-300 mb-2">
                Contraseña
              </label>
              <div className="relative">
                <Lock className="absolute left-4 top-1/2 -translate-y-1/2 w-5 h-5 text-dark-400" />
                <input
                  type={showPassword ? 'text' : 'password'}
                  value={password}
                  onChange={(e) => setPassword(e.target.value)}
                  placeholder="••••••••"
                  className="input pl-12 pr-12"
                  required
                  minLength={6}
                />
                <button
                  type="button"
                  onClick={() => setShowPassword(!showPassword)}
                  className="absolute right-4 top-1/2 -translate-y-1/2 text-dark-400 hover:text-dark-300"
                >
                  {showPassword ? <EyeOff className="w-5 h-5" /> : <Eye className="w-5 h-5" />}
                </button>
              </div>
            </div>

            {/* Submit Button */}
            <button
              type="submit"
              disabled={isLoading}
              className="btn-primary w-full flex items-center justify-center gap-2 disabled:opacity-50 disabled:cursor-not-allowed"
            >
              {isLoading ? (
                <div className="w-5 h-5 border-2 border-white/30 border-t-white rounded-full animate-spin" />
              ) : (
                <>
                  Iniciar Sesión
                  <ChevronRight className="w-5 h-5" />
                </>
              )}
            </button>
          </form>

          {/* Divider */}
          <div className="relative my-6">
            <div className="absolute inset-0 flex items-center">
              <div className="w-full border-t border-dark-600" />
            </div>
            <div className="relative flex justify-center text-sm">
              <span className="px-4 bg-dark-900 text-dark-400">¿No tienes cuenta?</span>
            </div>
          </div>

          {/* Register Link */}
          <Link href="/register" className="btn-secondary w-full block text-center">
            Crear Cuenta Gratis
          </Link>
        </div>

        {/* Features */}
        <div className="mt-8 space-y-3">
          {[
            'Sin verificación SMS',
            'Acceso inmediato a señales',
            'Seguridad de grado militar',
          ].map((feature, index) => (
            <motion.div
              key={index}
              initial={{ opacity: 0, x: -20 }}
              animate={{ opacity: 1, x: 0 }}
              transition={{ delay: index * 0.1 }}
              className="flex items-center gap-3 text-dark-400 text-sm"
            >
              <CheckCircle className="w-4 h-4 text-primary-500 flex-shrink-0" />
              {feature}
            </motion.div>
          ))}
        </div>
      </motion.div>
    </div>
  );
}
