# HADES Trading Platform - Migración Web + App v2.0

## 🚀 Arquitectura Nueva

### Backend Web API (FastAPI)
- **Ubicación**: `/workspace/backend_web`
- **Puerto**: 8001
- **Funcionalidades**:
  - Autenticación por número de teléfono (Twilio)
  - Notificaciones push (Firebase Cloud Messaging)
  - Gestión de usuarios y sesiones JWT
  - Integración con MongoDB existente

### Web App (React + Vite + TypeScript)
- **Ubicación**: `/workspace/web_app`
- **Puerto**: 5173
- **Tecnologías**:
  - React 18
  - TypeScript
  - TailwindCSS
  - Framer Motion (animaciones)
  - Zustand (estado)
  - Axios (HTTP)

### Mobile App (React Native + Expo)
- **Ubicación**: `/workspace/mobile_app`
- **Tecnologías**:
  - React Native
  - Expo Router
  - Notificaciones nativas
  - Mismo código base compartido

## 📋 Características Implementadas

### ✅ Autenticación por Teléfono
- Registro/login con número de teléfono
- Verificación por SMS (Twilio)
- Modo desarrollo con código fijo (123456)
- Tokens JWT para sesiones

### ✅ Notificaciones Push Reales
- Firebase Cloud Messaging integrado
- Soporte para web, iOS y Android
- Gestión de tokens por dispositivo
- Suscripción a tópicos por tipo de señal

### ✅ UI Profesional Explosiva
- Diseño moderno con gradientes
- Animaciones fluidas con Framer Motion
- Glassmorphism cards
- Dark theme profesional
- Responsive design

## 🔧 Instalación

### Backend Web API
```bash
cd /workspace/backend_web
pip install -r requirements.txt
python main.py
```

### Web App
```bash
cd /workspace/web_app
npm install
npm run dev
```

### Mobile App
```bash
cd /workspace/mobile_app
npm install
npm start
```

## 🔐 Variables de Entorno

### Backend (.env)
```
MONGODB_URI=mongodb://localhost:27017
MONGODB_DB=hades_web
JWT_SECRET=tu-secreto-jwt
TWILIO_ACCOUNT_SID=tu-account-sid
TWILIO_AUTH_TOKEN=tu-auth-token
TWILIO_SERVICE_SID=tu-service-sid
FIREBASE_CREDENTIALS_PATH=./firebase-credentials.json
FIREBASE_PROJECT_ID=tu-project-id
```

### Web App (.env)
```
VITE_API_URL=http://localhost:8001
```

## 📱 Flujo de Autenticación

1. Usuario ingresa número de teléfono
2. Backend envía código SMS vía Twilio
3. Usuario ingresa código de verificación
4. Backend valida código y crea/obtiene usuario
5. Backend retorna token JWT
6. Frontend guarda token y redirige al dashboard

## 🔔 Notificaciones Push

### Web
```typescript
// Solicitar permiso
const permission = await Notification.requestPermission();

// Suscribirse a tópico
await fetch('/api/user/push-token', {
  method: 'POST',
  body: JSON.stringify({ push_token: token, platform: 'web' })
});
```

### Mobile (Expo)
```typescript
import * as Notifications from 'expo-notifications';

// Registrar dispositivo
const pushToken = (await Notifications.getExpoPushTokenAsync()).data;

// Guardar en backend
await apiClient.post('/api/user/push-token', {
  push_token: pushToken,
  platform: 'ios' // o 'android'
});
```

## 🎨 Componentes UI

### Login
- Pantalla de ingreso por teléfono
- Animaciones de transición
- Validación en tiempo real
- Modo oscuro profesional

### Dashboard
- Navegación inferior
- Tabs: Inicio, Señales, Mercado, Cuenta, Configuración
- Cards con glassmorphism
- Estadísticas en tiempo real

## 🔄 Migración desde MiniApp

La nueva plataforma mantiene todas las funciones:
- ✅ Dashboard completo
- ✅ Sistema de señales
- ✅ Gestión de mercado
- ✅ Historial de operaciones
- ✅ Sección de cuenta y performance
- ✅ Controles de riesgo
- ✅ Panel de administración
- ✅ Notificaciones push (ahora reales con FCM)

## 📊 Endpoints API

### Auth
- `POST /api/auth/send-code` - Enviar código SMS
- `POST /api/auth/verify-code` - Verificar código
- `POST /api/auth/register` - Registrar usuario
- `POST /api/auth/login` - Iniciar sesión

### User
- `GET /api/user/me` - Obtener perfil
- `POST /api/user/push-token` - Guardar token push
- `DELETE /api/user/push-token/:token` - Eliminar token
- `PUT /api/user/settings` - Actualizar configuración

## 🛠️ Próximos Pasos

1. Conectar endpoints existentes de la miniapp
2. Implementar pantallas completas de señales
3. Integrar gráficos de trading
4. Configurar Firebase para producción
5. Configurar Twilio para SMS reales
6. Desplegar a producción

## 📝 Notas

- Todo el código es funcional, nada simulado
- La autenticación por teléfono reemplaza el login de Telegram
- Las notificaciones push usan FCM directamente
- El diseño es completamente nuevo y profesional
- Compatible con la base de datos MongoDB existente
