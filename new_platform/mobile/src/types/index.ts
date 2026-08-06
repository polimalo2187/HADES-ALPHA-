export interface User {
  id: string;
  phone: string;
  email?: string;
  firstName?: string;
  lastName?: string;
  avatar?: string;
  isActive: boolean;
  isBanned: boolean;
  plan?: {
    name: string;
    expiresAt: string;
    features: string[];
  };
  createdAt: string;
  lastLogin?: string;
}

export interface Signal {
  id: string;
  type: 'arbitrage' | 'volume' | 'pump';
  symbol: string;
  exchanges: {
    buy: {
      exchange: string;
      price: number;
      volume?: number;
    };
    sell: {
      exchange: string;
      price: number;
      volume?: number;
    };
  };
  profitPercent: number;
  volumeMultiplier?: number;
  confidence: number;
  status: 'active' | 'expired' | 'executed';
  createdAt: string;
  expiresAt: string;
}

export interface MarketData {
  symbol: string;
  exchange: string;
  price: number;
  change24h: number;
  changePercent24h: number;
  high24h: number;
  low24h: number;
  volume24h: number;
  bid: number;
  ask: number;
  timestamp: string;
}

export interface OrderBook {
  symbol: string;
  exchange: string;
  bids: [number, number][]; // [price, amount]
  asks: [number, number][];
  timestamp: string;
}

export interface Candle {
  timestamp: string;
  open: number;
  high: number;
  low: number;
  close: number;
  volume: number;
}

export interface Payment {
  id: string;
  userId: string;
  amount: number;
  currency: string;
  status: 'pending' | 'confirmed' | 'failed' | 'expired';
  txHash?: string;
  confirmations?: number;
  planName?: string;
  createdAt: string;
  confirmedAt?: string;
}

export interface PaymentOrder {
  id: string;
  address: string;
  amount: number;
  currency: string;
  chain: string;
  qrCode: string;
  expiresAt: string;
}

export interface Notification {
  id: string;
  type: 'signal' | 'payment' | 'system' | 'price';
  title: string;
  message: string;
  data?: any;
  read: boolean;
  createdAt: string;
}

export interface AuthTokens {
  accessToken: string;
  refreshToken: string;
  expiresAt: number;
}

export interface LoginCredentials {
  phone: string;
  password: string;
}

export interface RegisterData {
  phone: string;
  password: string;
  firstName?: string;
  lastName?: string;
}

export interface WebSocketMessage {
  type: 'signal' | 'market_update' | 'notification' | 'heartbeat' | 'auth_success' | 'auth_error';
  payload: any;
  timestamp: string;
}

export interface AppState {
  user: User | null;
  tokens: AuthTokens | null;
  isAuthenticated: boolean;
  isLoading: boolean;
  biometricEnabled: boolean;
  notifications: Notification[];
  signals: Signal[];
  marketData: Record<string, MarketData>;
}

export interface NavigationParams {
  Login: undefined;
  Register: undefined;
  Dashboard: undefined;
  Scanner: undefined;
  Market: { symbol?: string };
  Wallet: undefined;
  Profile: undefined;
  SignalDetail: { signalId: string };
  PaymentDetail: { paymentId: string };
  Settings: undefined;
}
