import axios, { AxiosInstance, AxiosRequestConfig, AxiosError } from 'axios';
import * as SecureStore from 'expo-secure-store';
import { ENV } from '@config/env';
import type { AuthTokens } from '@types/index';

const TOKEN_KEY = 'auth_tokens';

class ApiService {
  private client: AxiosInstance;
  private isRefreshing: boolean = false;
  private refreshSubscribers: ((token: string) => void)[] = [];

  constructor() {
    this.client = axios.create({
      baseURL: ENV.API_URL,
      timeout: 30000,
      headers: {
        'Content-Type': 'application/json',
      },
    });

    this.setupInterceptors();
  }

  private setupInterceptors() {
    // Request interceptor - add auth token
    this.client.interceptors.request.use(
      async (config) => {
        const tokens = await this.getTokens();
        if (tokens && config.headers) {
          config.headers.Authorization = `Bearer ${tokens.accessToken}`;
        }
        return config;
      },
      (error) => Promise.reject(error)
    );

    // Response interceptor - handle token refresh
    this.client.interceptors.response.use(
      (response) => response,
      async (error: AxiosError) => {
        const originalRequest = error.config as any;

        if (error.response?.status === 401 && !originalRequest._retry) {
          if (this.isRefreshing) {
            return new Promise((resolve) => {
              this.refreshSubscribers.push((token: string) => {
                originalRequest.headers.Authorization = `Bearer ${token}`;
                resolve(this.client(originalRequest));
              });
            });
          }

          originalRequest._retry = true;
          this.isRefreshing = true;

          try {
            const newToken = await this.refreshToken();
            this.isRefreshing = false;
            this.onRefreshed(newToken);
            originalRequest.headers.Authorization = `Bearer ${newToken}`;
            return this.client(originalRequest);
          } catch (refreshError) {
            this.isRefreshing = false;
            await this.logout();
            return Promise.reject(refreshError);
          }
        }

        return Promise.reject(error);
      }
    );
  }

  private onRefreshed(token: string) {
    this.refreshSubscribers.forEach((callback) => callback(token));
    this.refreshSubscribers = [];
  }

  private async getTokens(): Promise<AuthTokens | null> {
    try {
      const tokensJson = await SecureStore.getItemAsync(TOKEN_KEY);
      if (!tokensJson) return null;
      return JSON.parse(tokensJson);
    } catch {
      return null;
    }
  }

  private async saveTokens(tokens: AuthTokens): Promise<void> {
    await SecureStore.setItemAsync(TOKEN_KEY, JSON.stringify(tokens));
  }

  async login(phone: string, password: string) {
    const response = await this.client.post('/api/auth/login', { phone, password });
    const tokens = response.data.tokens;
    await this.saveTokens(tokens);
    return response.data;
  }

  async register(phone: string, password: string, firstName?: string, lastName?: string) {
    const response = await this.client.post('/api/auth/register', {
      phone,
      password,
      firstName,
      lastName,
    });
    const tokens = response.data.tokens;
    await this.saveTokens(tokens);
    return response.data;
  }

  async refreshToken(): Promise<string> {
    const tokens = await this.getTokens();
    if (!tokens) throw new Error('No refresh token available');

    const response = await this.client.post('/api/auth/refresh', {
      refreshToken: tokens.refreshToken,
    });

    const newTokens = response.data.tokens;
    await this.saveTokens(newTokens);
    return newTokens.accessToken;
  }

  async logout(): Promise<void> {
    try {
      await this.client.post('/api/auth/logout');
    } catch {
      // Ignore errors on logout
    }
    await SecureStore.deleteItemAsync(TOKEN_KEY);
  }

  async isAuthenticated(): Promise<boolean> {
    const tokens = await this.getTokens();
    if (!tokens) return false;

    const now = Date.now();
    return tokens.expiresAt > now;
  }

  async getUserProfile() {
    const response = await this.client.get('/api/users/me');
    return response.data;
  }

  // Generic request methods
  async get<T>(url: string, config?: AxiosRequestConfig) {
    const response = await this.client.get<T>(url, config);
    return response.data;
  }

  async post<T>(url: string, data?: any, config?: AxiosRequestConfig) {
    const response = await this.client.post<T>(url, data, config);
    return response.data;
  }

  async put<T>(url: string, data?: any, config?: AxiosRequestConfig) {
    const response = await this.client.put<T>(url, data, config);
    return response.data;
  }

  async delete<T>(url: string, config?: AxiosRequestConfig) {
    const response = await this.client.delete<T>(url, config);
    return response.data;
  }
}

export const api = new ApiService();
export default api;
