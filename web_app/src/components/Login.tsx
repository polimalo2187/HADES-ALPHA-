import { useState } from 'react'
import { motion, AnimatePresence } from 'framer-motion'
import { authService } from '../services/api'
import { useAuthStore } from '../context/authStore'
import { Phone, ArrowRight, Shield } from 'lucide-react'

interface LoginProps {
  onLoginSuccess: () => void
}

export default function Login({ onLoginSuccess }: LoginProps) {
  const [phoneNumber, setPhoneNumber] = useState('')
  const [verificationCode, setVerificationCode] = useState('')
  const [step, setStep] = useState<'phone' | 'code'>('phone')
  const [loading, setLoading] = useState(false)
  const [error, setError] = useState('')
  const login = useAuthStore((state) => state.login)

  const handleSendCode = async () => {
    if (!phoneNumber.trim()) {
      setError('Ingresa tu número de teléfono')
      return
    }

    setLoading(true)
    setError('')

    try {
      await authService.sendVerificationCode(phoneNumber)
      setStep('code')
    } catch (err: any) {
      setError(err.response?.data?.detail || 'Error enviando código')
    } finally {
      setLoading(false)
    }
  }

  const handleVerifyCode = async () => {
    if (!verificationCode.trim()) {
      setError('Ingresa el código de verificación')
      return
    }

    setLoading(true)
    setError('')

    try {
      const response = await authService.verifyCode(phoneNumber, verificationCode)
      if (response.ok) {
        login(response.session_token, response.user)
        onLoginSuccess()
      }
    } catch (err: any) {
      setError(err.response?.data?.detail || 'Código inválido')
    } finally {
      setLoading(false)
    }
  }

  return (
    <div className="min-h-screen flex items-center justify-center p-4">
      <motion.div
        initial={{ opacity: 0, y: 20 }}
        animate={{ opacity: 1, y: 0 }}
        className="w-full max-w-md"
      >
        <div className="glass-card rounded-3xl p-8">
          <div className="text-center mb-8">
            <motion.div
              initial={{ scale: 0 }}
              animate={{ scale: 1 }}
              transition={{ type: 'spring', stiffness: 260, damping: 20 }}
              className="w-20 h-20 mx-auto mb-4 rounded-2xl animated-gradient flex items-center justify-center"
            >
              <Shield className="w-10 h-10 text-white" />
            </motion.div>
            <h1 className="text-3xl font-bold gradient-text mb-2">HADES</h1>
            <p className="text-muted-foreground">Plataforma de Trading Profesional</p>
          </div>

          <AnimatePresence mode="wait">
            {step === 'phone' ? (
              <motion.div
                key="phone"
                initial={{ opacity: 0, x: -20 }}
                animate={{ opacity: 1, x: 0 }}
                exit={{ opacity: 0, x: 20 }}
              >
                <label className="block text-sm font-medium mb-2">
                  Número de Teléfono
                </label>
                <div className="relative mb-4">
                  <Phone className="absolute left-3 top-1/2 -translate-y-1/2 w-5 h-5 text-muted-foreground" />
                  <input
                    type="tel"
                    value={phoneNumber}
                    onChange={(e) => setPhoneNumber(e.target.value)}
                    placeholder="+54 9 11 2345 6789"
                    className="w-full pl-12 pr-4 py-3 bg-secondary border border-border rounded-xl focus:outline-none focus:ring-2 focus:ring-primary"
                    onKeyDown={(e) => e.key === 'Enter' && handleSendCode()}
                  />
                </div>

                {error && (
                  <motion.p
                    initial={{ opacity: 0, y: -10 }}
                    animate={{ opacity: 1, y: 0 }}
                    className="text-destructive text-sm mb-4"
                  >
                    {error}
                  </motion.p>
                )}

                <button
                  onClick={handleSendCode}
                  disabled={loading}
                  className="w-full py-3 px-4 bg-gradient-to-r from-pink-500 to-purple-600 text-white font-semibold rounded-xl hover:opacity-90 transition-opacity disabled:opacity-50 flex items-center justify-center gap-2"
                >
                  {loading ? 'Enviando...' : 'Enviar Código'}
                  {!loading && <ArrowRight className="w-5 h-5" />}
                </button>
              </motion.div>
            ) : (
              <motion.div
                key="code"
                initial={{ opacity: 0, x: 20 }}
                animate={{ opacity: 1, x: 0 }}
                exit={{ opacity: 0, x: -20 }}
              >
                <label className="block text-sm font-medium mb-2">
                  Código de Verificación
                </label>
                <p className="text-sm text-muted-foreground mb-4">
                  Ingresa el código enviado a {phoneNumber}
                </p>
                <input
                  type="text"
                  value={verificationCode}
                  onChange={(e) => setVerificationCode(e.target.value)}
                  placeholder="123456"
                  maxLength={6}
                  className="w-full px-4 py-3 bg-secondary border border-border rounded-xl focus:outline-none focus:ring-2 focus:ring-primary text-center text-2xl tracking-widest mb-4"
                  onKeyDown={(e) => e.key === 'Enter' && handleVerifyCode()}
                />

                {error && (
                  <motion.p
                    initial={{ opacity: 0, y: -10 }}
                    animate={{ opacity: 1, y: 0 }}
                    className="text-destructive text-sm mb-4"
                  >
                    {error}
                  </motion.p>
                )}

                <button
                  onClick={handleVerifyCode}
                  disabled={loading}
                  className="w-full py-3 px-4 bg-gradient-to-r from-pink-500 to-purple-600 text-white font-semibold rounded-xl hover:opacity-90 transition-opacity disabled:opacity-50"
                >
                  {loading ? 'Verificando...' : 'Verificar'}
                </button>

                <button
                  onClick={() => setStep('phone')}
                  className="w-full mt-3 py-2 text-sm text-muted-foreground hover:text-foreground transition-colors"
                >
                  Cambiar número
                </button>
              </motion.div>
            )}
          </AnimatePresence>
        </div>
      </motion.div>
    </div>
  )
}
