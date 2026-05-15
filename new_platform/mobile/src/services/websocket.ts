import { WebSocketMessage, Signal, MarketData, Notification } from '@types/index';
import { ENV, WS } from '@config/env';

type MessageHandler = (message: WebSocketMessage) => void;
type ConnectionStatus = 'disconnected' | 'connecting' | 'connected' | 'reconnecting';

class WebSocketService {
  private ws: WebSocket | null = null;
  private heartbeatInterval: NodeJS.Timeout | null = null;
  private reconnectTimeout: NodeJS.Timeout | null = null;
  private reconnectAttempts = 0;
  private status: ConnectionStatus = 'disconnected';
  private messageHandlers: MessageHandler[] = [];
  private authToken: string | null = null;
  private shouldReconnect = true;

  constructor() {
    this.shouldReconnect = true;
  }

  connect(token: string): Promise<void> {
    return new Promise((resolve, reject) => {
      if (this.ws && this.status === 'connected') {
        resolve();
        return;
      }

      this.authToken = token;
      this.status = 'connecting';
      this.shouldReconnect = true;

      try {
        const wsUrl = `${ENV.WS_URL}?token=${token}`;
        this.ws = new WebSocket(wsUrl);

        this.ws.onopen = () => {
          console.log('[WS] Connected');
          this.status = 'connected';
          this.reconnectAttempts = 0;
          this.startHeartbeat();
          resolve();
        };

        this.ws.onmessage = (event) => {
          try {
            const message: WebSocketMessage = JSON.parse(event.data);
            this.handleMessage(message);
          } catch (error) {
            console.error('[WS] Error parsing message:', error);
          }
        };

        this.ws.onerror = (error) => {
          console.error('[WS] Error:', error);
          reject(error);
        };

        this.ws.onclose = (event) => {
          console.log(`[WS] Closed: ${event.code} ${event.reason}`);
          this.status = 'disconnected';
          this.stopHeartbeat();

          if (this.shouldReconnect && event.code !== 1000) {
            this.scheduleReconnect();
          }
        };
      } catch (error) {
        this.status = 'disconnected';
        reject(error);
      }
    });
  }

  disconnect(): void {
    this.shouldReconnect = false;
    this.stopHeartbeat();
    if (this.reconnectTimeout) {
      clearTimeout(this.reconnectTimeout);
      this.reconnectTimeout = null;
    }

    if (this.ws) {
      this.ws.close(1000, 'User disconnected');
      this.ws = null;
    }

    this.status = 'disconnected';
    this.authToken = null;
  }

  private startHeartbeat(): void {
    this.stopHeartbeat();

    this.heartbeatInterval = setInterval(() => {
      if (this.ws && this.status === 'connected') {
        this.send({ type: 'heartbeat', payload: {}, timestamp: new Date().toISOString() });
      }
    }, WS.HEARTBEAT_INTERVAL);
  }

  private stopHeartbeat(): void {
    if (this.heartbeatInterval) {
      clearInterval(this.heartbeatInterval);
      this.heartbeatInterval = null;
    }
  }

  private scheduleReconnect(): void {
    if (this.reconnectAttempts >= WS.MAX_RECONNECT_ATTEMPTS) {
      console.log('[WS] Max reconnect attempts reached');
      this.status = 'disconnected';
      return;
    }

    this.status = 'reconnecting';
    this.reconnectAttempts++;

    const delay = Math.min(
      WS.RECONNECT_INTERVAL * Math.pow(2, this.reconnectAttempts),
      60000
    );

    console.log(`[WS] Reconnecting in ${delay}ms (attempt ${this.reconnectAttempts})`);

    this.reconnectTimeout = setTimeout(() => {
      if (this.authToken && this.shouldReconnect) {
        this.connect(this.authToken).catch(console.error);
      }
    }, delay);
  }

  send(message: Omit<WebSocketMessage, 'timestamp'>): void {
    if (!this.ws || this.status !== 'connected') {
      console.warn('[WS] Cannot send message - not connected');
      return;
    }

    const fullMessage: WebSocketMessage = {
      ...message,
      timestamp: new Date().toISOString(),
    };

    try {
      this.ws.send(JSON.stringify(fullMessage));
    } catch (error) {
      console.error('[WS] Error sending message:', error);
    }
  }

  subscribe(handler: MessageHandler): () => void {
    this.messageHandlers.push(handler);

    return () => {
      const index = this.messageHandlers.indexOf(handler);
      if (index > -1) {
        this.messageHandlers.splice(index, 1);
      }
    };
  }

  private handleMessage(message: WebSocketMessage): void {
    this.messageHandlers.forEach((handler) => {
      try {
        handler(message);
      } catch (error) {
        console.error('[WS] Error in message handler:', error);
      }
    });
  }

  getStatus(): ConnectionStatus {
    return this.status;
  }

  isAuthenticated(): boolean {
    return this.status === 'connected' && this.authToken !== null;
  }

  // Convenience methods for specific message types
  subscribeToSignals(handler: (signal: Signal) => void): () => void {
    return this.subscribe((message) => {
      if (message.type === 'signal') {
        handler(message.payload);
      }
    });
  }

  subscribeToMarketUpdates(handler: (data: MarketData) => void): () => void {
    return this.subscribe((message) => {
      if (message.type === 'market_update') {
        handler(message.payload);
      }
    });
  }

  subscribeToNotifications(handler: (notification: Notification) => void): () => void {
    return this.subscribe((message) => {
      if (message.type === 'notification') {
        handler(message.payload);
      }
    });
  }
}

export const wsService = new WebSocketService();
export default wsService;
