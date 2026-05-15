import { create } from 'zustand';
import { persist } from 'zustand/middleware';

interface User {
  id: string;
  phone: string;
  name?: string;
  email?: string;
  plan?: 'free' | 'basic' | 'pro' | 'enterprise';
  planExpires?: string;
  createdAt: string;
}

interface AuthState {
  user: User | null;
  token: string | null;
  isAuthenticated: boolean;
  isLoading: boolean;
  login: (token: string, user: User) => void;
  logout: () => void;
  updateUser: (user: Partial<User>) => void;
}

export const useAuthStore = create<AuthState>()(
  persist(
    (set) => ({
      user: null,
      token: null,
      isAuthenticated: false,
      isLoading: true,
      login: (token, user) => set({ 
        token, 
        user, 
        isAuthenticated: true,
        isLoading: false 
      }),
      logout: () => set({ 
        token: null, 
        user: null, 
        isAuthenticated: false,
        isLoading: false 
      }),
      updateUser: (userData) => set((state) => ({
        user: state.user ? { ...state.user, ...userData } : null
      })),
    }),
    {
      name: 'hades-auth-storage',
    }
  )
);

interface Signal {
  id: string;
  type: 'arbitrage' | 'volume' | 'price';
  symbol: string;
  exchange1: string;
  exchange2?: string;
  price1: number;
  price2?: number;
  profit?: number;
  volumeChange?: number;
  timestamp: string;
  status: 'active' | 'expired' | 'executed';
}

interface SignalState {
  signals: Signal[];
  unreadCount: number;
  addSignal: (signal: Signal) => void;
  markAsRead: (id: string) => void;
  clearSignals: () => void;
}

export const useSignalStore = create<SignalState>((set) => ({
  signals: [],
  unreadCount: 0,
  addSignal: (signal) => set((state) => ({
    signals: [signal, ...state.signals].slice(0, 100),
    unreadCount: state.unreadCount + 1
  })),
  markAsRead: (id) => set((state) => ({
    signals: state.signals.map(s => 
      s.id === id ? { ...s, status: 'executed' as const } : s
    ),
    unreadCount: Math.max(0, state.unreadCount - 1)
  })),
  clearSignals: () => set({ signals: [], unreadCount: 0 }),
}));

interface WebSocketState {
  isConnected: boolean;
  ws: WebSocket | null;
  connect: (url: string, token: string) => void;
  disconnect: () => void;
  sendMessage: (message: any) => void;
}

export const useWebSocketStore = create<WebSocketState>((set, get) => ({
  isConnected: false,
  ws: null,
  connect: (url, token) => {
    const ws = new WebSocket(`${url}?token=${token}`);
    
    ws.onopen = () => {
      console.log('WebSocket connected');
      set({ isConnected: true, ws });
    };
    
    ws.onclose = () => {
      console.log('WebSocket disconnected');
      set({ isConnected: false, ws: null });
    };
    
    ws.onerror = (error) => {
      console.error('WebSocket error:', error);
    };
    
    ws.onmessage = (event) => {
      const data = JSON.parse(event.data);
      console.log('Message received:', data);
      // Handle different message types
      if (data.type === 'signal') {
        useSignalStore.getState().addSignal(data.payload);
      }
    };
  },
  disconnect: () => {
    const { ws } = get();
    if (ws) {
      ws.close();
      set({ isConnected: false, ws: null });
    }
  },
  sendMessage: (message) => {
    const { ws } = get();
    if (ws && ws.readyState === WebSocket.OPEN) {
      ws.send(JSON.stringify(message));
    }
  },
}));
