# HADES Mobile App - React Native

Aplicación móvil nativa para iOS y Android construida con Expo.

## 🚀 Características

- **Autenticación Biométrica**: Face ID / Touch ID
- **Push Notifications Internas**: Sin servicios de terceros
- **WebSockets**: Conexión en tiempo real con el backend
- **UI Glassmorphism**: Diseño deslumbrante con efectos de vidrio
- **Nativo 60 FPS**: Animaciones fluidas con Reanimated

## 📱 Instalación

### Requisitos
- Node.js 18+
- npm o yarn
- Expo CLI: `npm install -g expo-cli`
- Dispositivo físico o emulador

### Pasos

1. **Instalar dependencias**
```bash
cd mobile
npm install
```

2. **Configurar variables de entorno**
Crear archivo `.env` en la raíz:
```bash
EXPO_PUBLIC_API_URL=http://TU_IP:8000
EXPO_PUBLIC_WS_URL=ws://TU_IP:8000/api/ws
EXPO_PUBLIC_PROJECT_ID=tu-project-id
```

3. **Iniciar desarrollo**
```bash
npx expo start
```

4. **Probar en dispositivo**
- Escanear QR con Expo Go (iOS/Android)

## 🔨 Build para Producción

### Android
```bash
eas build --platform android
eas submit --platform android
```

### iOS
```bash
eas build --platform ios
eas submit --platform ios
```

## 📁 Estructura

```
mobile/
├── src/
│   ├── components/     # Componentes UI reutilizables
│   │   ├── GlassCard.tsx
│   │   ├── BiometricButton.tsx
│   │   └── SignalCard.tsx
│   ├── screens/        # Pantallas principales
│   │   └── LoginScreen.tsx
│   ├── navigation/     # Navegación
│   │   ├── AppNavigator.tsx
│   │   ├── BottomTabNavigator.tsx
│   │   └── types.ts
│   ├── services/       # Servicios y APIs
│   │   ├── api.ts
│   │   ├── websocket.ts
│   │   ├── biometrics.ts
│   │   └── notifications.ts
│   ├── config/         # Configuración
│   │   └── env.ts
│   └── types/          # Tipos TypeScript
│       └── index.ts
├── assets/             # Imágenes, iconos, fuentes
├── App.tsx             # Entry point
├── app.json            # Configuración Expo
└── package.json
```

## 🔑 Funcionalidades Implementadas

### Autenticación
- Login con teléfono + contraseña
- Registro sin verificación SMS
- Autenticación biométrica (Face ID/Touch ID)
- JWT con refresh automático
- Tokens en SecureStore (encriptado)

### Notificaciones Push
- Internas vía WebSocket
- Notificaciones nativas del SO
- Sonidos personalizados
- Badge counter
- Funciona en segundo plano

### WebSocket
- Conexión persistente
- Heartbeat cada 30s
- Reconexión automática exponencial
- Suscripción a señales, mercado, notificaciones

### UI/UX
- Tema oscuro glassmorphism
- BlurView nativo (expo-blur)
- Animaciones 60 FPS
- Responsive para todos los dispositivos

## 🔗 Conexión con Backend

La app se conecta directamente al backend FastAPI:

- **API REST**: `EXPO_PUBLIC_API_URL`
- **WebSocket**: `EXPO_PUBLIC_WS_URL`

Endpoints usados:
- `POST /api/auth/login`
- `POST /api/auth/register`
- `POST /api/auth/refresh`
- `GET /api/users/me`
- `WS /api/ws`

## 🛡️ Seguridad

- Tokens en SecureStore (enclave seguro del dispositivo)
- Biometría obligatoria para operaciones sensibles
- HTTPS/WSS en producción
- Auto-logout por inactividad
- Sin hardcoding de secrets

## 📊 Próimos Pasos

Para completar la app, falta implementar:
- [ ] Pantalla Dashboard completa
- [ ] Pantalla Scanner en tiempo real
- [ ] Pantalla Mercado con gráficos
- [ ] Pantalla Billetera con pagos
- [ ] Pantalla Perfil
- [ ] Contexto de autenticación global
- [ ] Tests unitarios

## 📄 Licencia

Propietario - Todos los derechos reservados
