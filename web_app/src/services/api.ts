import axios from 'axios'

const API_BASE_URL = import.meta.env.VITE_API_URL || 'http://localhost:8001'

export const apiClient = axios.create({
  baseURL: API_BASE_URL,
  headers: {
    'Content-Type': 'application/json',
  },
})

// Interceptor para agregar token
apiClient.interceptors.request.use(
  (config) => {
    const token = localStorage.getItem('auth-storage')
    if (token) {
      try {
        const parsed = JSON.parse(token)
        if (parsed.state?.token) {
          config.headers.Authorization = `Bearer ${parsed.state.token}`
        }
      } catch (e) {
        // Token inválido
      }
    }
    return config
  },
  (error) => Promise.reject(error)
)

// Auth Service
export const authService = {
  sendVerificationCode: async (phoneNumber: string) => {
    const response = await apiClient.post('/api/auth/send-code', { phone_number: phoneNumber })
    return response.data
  },

  verifyCode: async (phoneNumber: string, code: string) => {
    const response = await apiClient.post('/api/auth/verify-code', {
      phone_number: phoneNumber,
      verification_code: code,
    })
    return response.data
  },

  register: async (phoneNumber: string, username?: string, referralCode?: string) => {
    const response = await apiClient.post('/api/auth/register', {
      phone_number: phoneNumber,
      username,
      referral_code: referralCode,
    })
    return response.data
  },

  login: async (phoneNumber: string) => {
    const response = await apiClient.post('/api/auth/login', { phone_number: phoneNumber })
    return response.data
  },
}

// User Service
export const userService = {
  getMe: async () => {
    const response = await apiClient.get('/api/user/me')
    return response.data
  },

  savePushToken: async (pushToken: string, platform: string) => {
    const response = await apiClient.post('/api/user/push-token', {
      push_token: pushToken,
      platform,
    })
    return response.data
  },

  deletePushToken: async (token: string) => {
    const response = await apiClient.delete(`/api/user/push-token/${token}`)
    return response.data
  },

  updateSettings: async (settings: {
    language?: string
    push_alerts_enabled?: boolean
    push_tiers?: Record<string, boolean>
  }) => {
    const response = await apiClient.put('/api/user/settings', settings)
    return response.data
  },
}
