'use client';

import { useState, useEffect } from 'react';
import Link from 'next/link';
import { usePathname } from 'next/navigation';
import { motion } from 'framer-motion';
import { 
  Menu, 
  X, 
  TrendingUp, 
  Bell, 
  User, 
  LogOut,
  Zap,
  BarChart3,
  CreditCard,
  Settings
} from 'lucide-react';
import { useAuth } from '@/hooks/useAuth';

const navItems = [
  { href: '/dashboard', label: 'Dashboard', icon: Zap },
  { href: '/signals', label: 'Señales', icon: TrendingUp },
  { href: '/market', label: 'Mercado', icon: BarChart3 },
  { href: '/payments', label: 'Pagos', icon: CreditCard },
  { href: '/profile', label: 'Perfil', icon: Settings },
];

export default function Navbar() {
  const pathname = usePathname();
  const { user, logout } = useAuth();
  const [mobileMenuOpen, setMobileMenuOpen] = useState(false);
  const [scrolled, setScrolled] = useState(false);

  useEffect(() => {
    const handleScroll = () => setScrolled(window.scrollY > 20);
    window.addEventListener('scroll', handleScroll);
    return () => window.removeEventListener('scroll', handleScroll);
  }, []);

  if (!user) return null;

  return (
    <>
      <motion.nav
        initial={{ y: -100 }}
        animate={{ y: 0 }}
        className={`fixed top-0 left-0 right-0 z-50 transition-all duration-300 ${
          scrolled 
            ? 'backdrop-blur-xl bg-gray-950/80 border-b border-white/10' 
            : 'bg-transparent'
        }`}
      >
        <div className="max-w-7xl mx-auto px-4 md:px-8">
          <div className="flex items-center justify-between h-16 md:h-20">
            {/* Logo */}
            <Link href="/dashboard" className="flex items-center gap-2">
              <div className="w-10 h-10 rounded-xl bg-gradient-to-br from-purple-600 to-cyan-600 flex items-center justify-center">
                <TrendingUp className="w-6 h-6 text-white" />
              </div>
              <span className="text-xl font-bold text-white hidden md:block">HADES</span>
            </Link>

            {/* Desktop Navigation */}
            <div className="hidden md:flex items-center gap-1">
              {navItems.map((item) => {
                const isActive = pathname === item.href;
                return (
                  <Link
                    key={item.href}
                    href={item.href}
                    className={`relative px-4 py-2 rounded-xl flex items-center gap-2 transition-all ${
                      isActive
                        ? 'text-white bg-white/10'
                        : 'text-gray-400 hover:text-white hover:bg-white/5'
                    }`}
                  >
                    <item.icon className="w-5 h-5" />
                    {item.label}
                    {isActive && (
                      <motion.div
                        layoutId="activeNav"
                        className="absolute inset-0 bg-gradient-to-r from-purple-600/20 to-cyan-600/20 rounded-xl -z-10"
                      />
                    )}
                  </Link>
                );
              })}
            </div>

            {/* Right Side */}
            <div className="flex items-center gap-4">
              {/* Notifications */}
              <button className="relative p-2 text-gray-400 hover:text-white transition-all">
                <Bell className="w-6 h-6" />
                <span className="absolute top-1 right-1 w-2 h-2 bg-red-500 rounded-full" />
              </button>

              {/* User Menu */}
              <div className="hidden md:flex items-center gap-3 pl-4 border-l border-white/10">
                <div className="text-right">
                  <div className="text-sm font-bold text-white">{user.phone}</div>
                  <div className="text-xs text-gray-400 capitalize">{user.plan || 'Free'} Plan</div>
                </div>
                <button
                  onClick={logout}
                  className="p-2 text-gray-400 hover:text-red-400 transition-all"
                  title="Cerrar sesión"
                >
                  <LogOut className="w-5 h-5" />
                </button>
              </div>

              {/* Mobile Menu Button */}
              <button
                onClick={() => setMobileMenuOpen(!mobileMenuOpen)}
                className="md:hidden p-2 text-gray-400 hover:text-white"
              >
                {mobileMenuOpen ? <X className="w-6 h-6" /> : <Menu className="w-6 h-6" />}
              </button>
            </div>
          </div>
        </div>
      </motion.nav>

      {/* Mobile Menu */}
      {mobileMenuOpen && (
        <motion.div
          initial={{ opacity: 0, y: -20 }}
          animate={{ opacity: 1, y: 0 }}
          exit={{ opacity: 0, y: -20 }}
          className="fixed top-16 left-0 right-0 z-40 backdrop-blur-xl bg-gray-950/95 border-b border-white/10 md:hidden"
        >
          <div className="p-4 space-y-2">
            {navItems.map((item) => {
              const isActive = pathname === item.href;
              return (
                <Link
                  key={item.href}
                  href={item.href}
                  onClick={() => setMobileMenuOpen(false)}
                  className={`flex items-center gap-3 px-4 py-3 rounded-xl transition-all ${
                    isActive
                      ? 'bg-gradient-to-r from-purple-600/20 to-cyan-600/20 text-white'
                      : 'text-gray-400 hover:bg-white/5 hover:text-white'
                  }`}
                >
                  <item.icon className="w-5 h-5" />
                  {item.label}
                </Link>
              );
            })}
            <div className="pt-4 mt-4 border-t border-white/10">
              <div className="flex items-center justify-between px-4 py-3">
                <div>
                  <div className="text-sm font-bold text-white">{user.phone}</div>
                  <div className="text-xs text-gray-400 capitalize">{user.plan || 'Free'} Plan</div>
                </div>
                <button
                  onClick={() => {
                    logout();
                    setMobileMenuOpen(false);
                  }}
                  className="flex items-center gap-2 px-4 py-2 bg-red-500/20 text-red-400 rounded-xl"
                >
                  <LogOut className="w-4 h-4" />
                  Salir
                </button>
              </div>
            </div>
          </div>
        </motion.div>
      )}
    </>
  );
}
