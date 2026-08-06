'use client';

import { motion } from 'framer-motion';
import { Zap, TrendingUp, Shield, Globe, ChevronRight, Star } from 'lucide-react';
import Link from 'next/link';

const features = [
  {
    icon: Zap,
    title: 'Señales en Tiempo Real',
    description: 'Detección instantánea de oportunidades de arbitraje y picos de volumen.',
    color: 'from-yellow-400 to-orange-500',
  },
  {
    icon: TrendingUp,
    title: 'Multi-Exchange',
    description: 'Datos simultáneos de Binance, Bybit, OKX y KuCoin.',
    color: 'from-green-400 to-emerald-500',
  },
  {
    icon: Shield,
    title: '100% Seguro',
    description: 'Autenticación nativa sin dependencias de terceros.',
    color: 'from-blue-400 to-cyan-500',
  },
  {
    icon: Globe,
    title: 'Acceso Global',
    description: 'Disponible en web y app móvil sin restricciones.',
    color: 'from-purple-400 to-pink-500',
  },
];

const plans = [
  {
    name: 'Free',
    price: '$0',
    period: '/mes',
    features: ['3 señales/día', '1 exchange', 'Soporte básico'],
    cta: 'Comenzar Gratis',
    popular: false,
  },
  {
    name: 'Pro',
    price: '$49',
    period: '/mes',
    features: ['Señales ilimitadas', '4 exchanges', 'Arbitraje avanzado', 'Soporte prioritario'],
    cta: 'Obtener Pro',
    popular: true,
  },
  {
    name: 'Enterprise',
    price: '$199',
    period: '/mes',
    features: ['Todo en Pro', 'API dedicada', 'Personalización', 'Soporte 24/7'],
    cta: 'Contactar',
    popular: false,
  },
];

const stats = [
  { value: '10K+', label: 'Usuarios Activos' },
  { value: '$2M+', label: 'Volumen Diario' },
  { value: '99.9%', label: 'Uptime' },
  { value: '<10ms', label: 'Latencia' },
];

export default function LandingPage() {
  return (
    <div className="min-h-screen bg-dark-950">
      {/* Animated Background */}
      <div className="fixed inset-0 overflow-hidden pointer-events-none">
        <div className="absolute -top-1/2 -left-1/2 w-full h-full bg-gradient-to-br from-primary-900/20 via-transparent to-transparent rounded-full blur-3xl animate-float" />
        <div className="absolute -bottom-1/2 -right-1/2 w-full h-full bg-gradient-to-tl from-accent-900/20 via-transparent to-transparent rounded-full blur-3xl animate-float" style={{ animationDelay: '2s' }} />
        <div className="absolute inset-0 bg-hero-pattern opacity-50" />
      </div>

      {/* Navigation */}
      <nav className="relative z-50 border-b border-white/5 glass">
        <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8">
          <div className="flex items-center justify-between h-16">
            <motion.div 
              initial={{ opacity: 0, x: -20 }}
              animate={{ opacity: 1, x: 0 }}
              className="flex items-center gap-2"
            >
              <div className="w-10 h-10 bg-gradient-to-br from-primary-500 to-accent-500 rounded-xl flex items-center justify-center glow-primary">
                <Zap className="w-6 h-6 text-white" />
              </div>
              <span className="text-2xl font-bold gradient-text">HADES</span>
            </motion.div>
            
            <motion.div 
              initial={{ opacity: 0, x: 20 }}
              animate={{ opacity: 1, x: 0 }}
              className="flex items-center gap-4"
            >
              <Link href="/login" className="btn-secondary">
                Iniciar Sesión
              </Link>
              <Link href="/register" className="btn-primary">
                Registrarse
              </Link>
            </motion.div>
          </div>
        </div>
      </nav>

      {/* Hero Section */}
      <section className="relative z-10 py-20 lg:py-32">
        <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8 text-center">
          <motion.div
            initial={{ opacity: 0, y: 30 }}
            animate={{ opacity: 1, y: 0 }}
            transition={{ duration: 0.8 }}
          >
            <div className="inline-flex items-center gap-2 px-4 py-2 glass-light rounded-full mb-6">
              <Star className="w-4 h-4 text-yellow-400" />
              <span className="text-sm text-dark-300">La plataforma de trading más avanzada</span>
            </div>
            
            <h1 className="text-5xl lg:text-7xl font-bold mb-6">
              <span className="text-white">Descubre Oportunidades</span>
              <br />
              <span className="gradient-text">Antes que Nadie</span>
            </h1>
            
            <p className="text-xl text-dark-400 max-w-3xl mx-auto mb-10">
              Sistema inteligente de detección de arbitraje y anomalías de mercado. 
              Conecta con 4 exchanges principales y recibe señales en tiempo real.
            </p>
            
            <div className="flex flex-col sm:flex-row gap-4 justify-center">
              <Link href="/register" className="btn-primary text-lg px-8 py-4">
                Comenzar Ahora <ChevronRight className="inline-block ml-2 w-5 h-5" />
              </Link>
              <Link href="#features" className="btn-secondary text-lg px-8 py-4">
                Saber Más
              </Link>
            </div>
          </motion.div>

          {/* Stats */}
          <motion.div
            initial={{ opacity: 0, y: 40 }}
            animate={{ opacity: 1, y: 0 }}
            transition={{ duration: 0.8, delay: 0.2 }}
            className="grid grid-cols-2 md:grid-cols-4 gap-8 mt-20"
          >
            {stats.map((stat, index) => (
              <div key={index} className="card text-center">
                <div className="text-3xl lg:text-4xl font-bold gradient-text mb-2">{stat.value}</div>
                <div className="text-dark-400">{stat.label}</div>
              </div>
            ))}
          </motion.div>
        </div>
      </section>

      {/* Features Section */}
      <section id="features" className="relative z-10 py-20">
        <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8">
          <motion.div
            initial={{ opacity: 0, y: 20 }}
            whileInView={{ opacity: 1, y: 0 }}
            viewport={{ once: true }}
            className="text-center mb-16"
          >
            <h2 className="text-4xl font-bold text-white mb-4">
              Características <span className="gradient-text">Premium</span>
            </h2>
            <p className="text-dark-400 text-lg max-w-2xl mx-auto">
              Todo lo que necesitas para operar como un profesional
            </p>
          </motion.div>

          <div className="grid md:grid-cols-2 lg:grid-cols-4 gap-6">
            {features.map((feature, index) => (
              <motion.div
                key={index}
                initial={{ opacity: 0, y: 20 }}
                whileInView={{ opacity: 1, y: 0 }}
                viewport={{ once: true }}
                transition={{ delay: index * 0.1 }}
                className="card group"
              >
                <div className={`w-14 h-14 bg-gradient-to-br ${feature.color} rounded-xl flex items-center justify-center mb-4 group-hover:scale-110 transition-transform duration-300`}>
                  <feature.icon className="w-7 h-7 text-white" />
                </div>
                <h3 className="text-xl font-semibold text-white mb-2">{feature.title}</h3>
                <p className="text-dark-400">{feature.description}</p>
              </motion.div>
            ))}
          </div>
        </div>
      </section>

      {/* Pricing Section */}
      <section className="relative z-10 py-20">
        <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8">
          <motion.div
            initial={{ opacity: 0, y: 20 }}
            whileInView={{ opacity: 1, y: 0 }}
            viewport={{ once: true }}
            className="text-center mb-16"
          >
            <h2 className="text-4xl font-bold text-white mb-4">
              Planes <span className="gradient-text">Flexibles</span>
            </h2>
            <p className="text-dark-400 text-lg max-w-2xl mx-auto">
              Elige el plan perfecto para tus necesidades
            </p>
          </motion.div>

          <div className="grid md:grid-cols-3 gap-8">
            {plans.map((plan, index) => (
              <motion.div
                key={index}
                initial={{ opacity: 0, y: 20 }}
                whileInView={{ opacity: 1, y: 0 }}
                viewport={{ once: true }}
                transition={{ delay: index * 0.1 }}
                className={`card relative ${plan.popular ? 'border-primary-500 glow-primary' : ''}`}
              >
                {plan.popular && (
                  <div className="absolute -top-4 left-1/2 -translate-x-1/2 px-4 py-1 bg-gradient-to-r from-primary-600 to-primary-500 rounded-full text-sm font-semibold text-white">
                    Más Popular
                  </div>
                )}
                <div className="text-center mb-6">
                  <h3 className="text-2xl font-bold text-white mb-2">{plan.name}</h3>
                  <div className="flex items-baseline justify-center">
                    <span className="text-5xl font-bold gradient-text">{plan.price}</span>
                    <span className="text-dark-400 ml-2">{plan.period}</span>
                  </div>
                </div>
                <ul className="space-y-3 mb-8">
                  {plan.features.map((feature, i) => (
                    <li key={i} className="flex items-center gap-3 text-dark-300">
                      <div className="w-5 h-5 bg-gradient-to-br from-primary-500 to-accent-500 rounded-full flex items-center justify-center flex-shrink-0">
                        <Shield className="w-3 h-3 text-white" />
                      </div>
                      {feature}
                    </li>
                  ))}
                </ul>
                <Link href={`/register?plan=${plan.name.toLowerCase()}`} className={`block w-full text-center py-3 rounded-lg font-semibold transition-all duration-300 ${plan.popular ? 'btn-primary' : 'btn-secondary'}`}>
                  {plan.cta}
                </Link>
              </motion.div>
            ))}
          </div>
        </div>
      </section>

      {/* CTA Section */}
      <section className="relative z-10 py-20">
        <div className="max-w-4xl mx-auto px-4 sm:px-6 lg:px-8 text-center">
          <motion.div
            initial={{ opacity: 0, scale: 0.95 }}
            whileInView={{ opacity: 1, scale: 1 }}
            viewport={{ once: true }}
            className="card glow-primary"
          >
            <h2 className="text-4xl font-bold text-white mb-4">
              ¿Listo para Empezar?
            </h2>
            <p className="text-dark-400 text-lg mb-8">
              Únete a miles de traders que ya están aprovechando nuestras señales
            </p>
            <Link href="/register" className="btn-primary text-lg px-8 py-4">
              Crear Cuenta Gratis
            </Link>
          </motion.div>
        </div>
      </section>

      {/* Footer */}
      <footer className="relative z-10 border-t border-white/5 py-12">
        <div className="max-w-7xl mx-auto px-4 sm:px-6 lg:px-8">
          <div className="flex flex-col md:flex-row items-center justify-between gap-6">
            <div className="flex items-center gap-2">
              <div className="w-8 h-8 bg-gradient-to-br from-primary-500 to-accent-500 rounded-lg flex items-center justify-center">
                <Zap className="w-5 h-5 text-white" />
              </div>
              <span className="text-xl font-bold gradient-text">HADES</span>
            </div>
            <p className="text-dark-400 text-sm">
              © 2024 Hades Platform. Todos los derechos reservados.
            </p>
          </div>
        </div>
      </footer>
    </div>
  );
}
