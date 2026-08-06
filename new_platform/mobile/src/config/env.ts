export const ENV = {
  API_URL: process.env.EXPO_PUBLIC_API_URL || 'http://localhost:8000',
  WS_URL: process.env.EXPO_PUBLIC_WS_URL || 'ws://localhost:8000/api/ws',
  APP_NAME: 'HADES Trading',
  APP_VERSION: '1.0.0',
};

export const THEME = {
  colors: {
    background: '#0a0a0f',
    surface: '#12121a',
    surfaceLight: '#1e1e2a',
    primary: '#7c3aed',
    primaryLight: '#9f67ff',
    secondary: '#06b6d4',
    secondaryLight: '#22d3ee',
    accent: '#f43f5e',
    success: '#10b981',
    warning: '#f59e0b',
    error: '#ef4444',
    text: '#ffffff',
    textSecondary: '#a1a1aa',
    textMuted: '#71717a',
    border: '#27273a',
    glass: 'rgba(30, 30, 42, 0.7)',
    glassBorder: 'rgba(255, 255, 255, 0.1)',
  },
  spacing: {
    xs: 4,
    sm: 8,
    md: 16,
    lg: 24,
    xl: 32,
    xxl: 48,
  },
  borderRadius: {
    sm: 8,
    md: 12,
    lg: 16,
    xl: 24,
    full: 9999,
  },
  fontSize: {
    xs: 12,
    sm: 14,
    md: 16,
    lg: 18,
    xl: 24,
    xxl: 32,
  },
  shadows: {
    sm: {
      shadowColor: '#000',
      shadowOffset: { width: 0, height: 2 },
      shadowOpacity: 0.25,
      shadowRadius: 3.84,
      elevation: 2,
    },
    md: {
      shadowColor: '#000',
      shadowOffset: { width: 0, height: 4 },
      shadowOpacity: 0.3,
      shadowRadius: 4.6,
      elevation: 4,
    },
    lg: {
      shadowColor: '#7c3aed',
      shadowOffset: { width: 0, height: 8 },
      shadowOpacity: 0.4,
      shadowRadius: 9.11,
      elevation: 8,
    },
  },
};

export const AUTH = {
  BIOMETRIC_ENABLED: true,
  TOKEN_REFRESH_THRESHOLD: 300, // 5 minutos antes de expirar
  MAX_LOGIN_ATTEMPTS: 5,
  LOCKOUT_DURATION: 900000, // 15 minutos
};

export const WS = {
  HEARTBEAT_INTERVAL: 30000, // 30 segundos
  RECONNECT_INTERVAL: 5000, // 5 segundos
  MAX_RECONNECT_ATTEMPTS: 10,
  PING_TIMEOUT: 10000, // 10 segundos
};

export const NOTIFICATIONS = {
  ENABLED: true,
  SOUND_ENABLED: true,
  VIBRATION_ENABLED: true,
  BADGE_ENABLED: true,
};

export const SCANNER = {
  MIN_ARBITRAGE_PERCENT: 0.5,
  MIN_VOLUME_MULTIPLIER: 2,
  REFRESH_INTERVAL: 5000, // 5 segundos
  MAX_SIGNALS_DISPLAY: 50,
};

export const MARKET = {
  DEFAULT_EXCHANGE: 'binance',
  DEFAULT_TIMEFRAME: '1h',
  SUPPORTED_TIMEFRAMES: ['1m', '5m', '15m', '30m', '1h', '4h', '1d', '1w'],
  SUPPORTED_EXCHANGES: ['binance', 'bybit', 'okx', 'kucoin'],
};

export const PAYMENT = {
  CONFIRMATIONS_REQUIRED: 12,
  MIN_AMOUNT: 10, // USDT
  MAX_AMOUNT: 100000, // USDT
};
