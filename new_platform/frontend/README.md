# HADES Platform - Frontend

## Descripción
Frontend moderno y deslumbrante para la plataforma HADES, construido con Next.js 14, TypeScript y TailwindCSS.

## Características UI/UX
- 🎨 **Diseño Glassmorphism**: Efectos de vidrio esmerilado en toda la interfaz
- ✨ **Animaciones Fluidas**: Framer Motion para transiciones suaves
- 🌙 **Tema Oscuro Premium**: Paleta de colores cuidadosamente seleccionada
- 📱 **100% Responsive**: Funciona perfectamente en móvil, tablet y desktop
- 🔔 **Notificaciones en Tiempo Real**: Integración con WebSocket para push notifications
- 📊 **Gráficos Interactivos**: Recharts para visualización de datos

## Páginas Implementadas

### Públicas
- `/` - Landing page con hero section, features, pricing y CTA
- `/login` - Login con teléfono + contraseña (sin SMS)
- `/register` - Registro instantáneo sin verificación

### Privadas (Requieren Auth)
- `/dashboard` - Panel principal con estadísticas, gráficos y señales recientes
- `/signals` - Listado completo de señales (pendiente)
- `/market` - Datos de mercado en tiempo real (pendiente)
- `/payments` - Gestión de pagos y planes (pendiente)
- `/profile` - Configuración de usuario (pendiente)

## Tecnologías
- **Framework**: Next.js 14 (App Router)
- **Lenguaje**: TypeScript
- **Estilos**: TailwindCSS con configuración personalizada
- **Animaciones**: Framer Motion
- **Iconos**: Lucide React
- **Gráficos**: Recharts
- **Estado**: Zustand con persistencia
- **Notificaciones**: Sonner

## Instalación

```bash
cd frontend

# Instalar dependencias
npm install

# Copiar variables de entorno
cp .env.local.example .env.local

# Ejecutar en desarrollo
npm run dev

# Build para producción
npm run build

# Iniciar en producción
npm start
```

## Variables de Entorno

```bash
NEXT_PUBLIC_API_URL=http://localhost:8000
NEXT_PUBLIC_WS_URL=ws://localhost:8000/api/ws
```

## Estructura de Archivos

```
frontend/
├── src/
│   ├── app/
│   │   ├── layout.tsx          # Layout root
│   │   ├── page.tsx            # Landing page
│   │   ├── login/
│   │   │   └── page.tsx        # Login
│   │   ├── register/
│   │   │   └── page.tsx        # Registro
│   │   └── dashboard/
│   │       └── page.tsx        # Dashboard
│   ├── components/             # Componentes reutilizables
│   ├── hooks/
│   │   └── useStore.ts         # Stores Zustand
│   ├── lib/
│   │   └── utils.ts            # Utilidades
│   └── styles/
│       └── globals.css         # Estilos globales
├── package.json
├── tailwind.config.js
├── tsconfig.json
└── .env.local
```

## Componentes Clave

### Sistema de Diseño
- Colores personalizados (primary, accent, dark)
- Efectos glass (.glass, .glass-light)
- Textos gradient (.gradient-text)
- Efectos glow (.glow-primary, .glow-accent)
- Botones (.btn-primary, .btn-secondary)
- Cards (.card)
- Inputs (.input)

### Animaciones Personalizadas
- `animate-float`: Flotamiento suave
- `animate-gradient`: Gradiente animado
- `animate-pulse-slow`: Pulso lento
- `animate-shine`: Brillo deslizante

## Próximos Pasos
1. Completar páginas restantes (signals, market, payments, profile)
2. Implementar componentes reutilizables adicionales
3. Agregar más gráficos y visualizaciones
4. Optimizar performance (lazy loading, code splitting)
5. Implementar PWA para instalación como app móvil

## Notas
- No hay dependencias de Telegram
- Autenticación nativa con JWT
- WebSocket para notificaciones push internas
- Todo el código es producción real, nada simulado
