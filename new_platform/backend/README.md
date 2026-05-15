# HADES Platform - Backend API

## 📊 Resumen del Backend

| Métrica | Valor |
|---------|-------|
| **Archivos Python** | 16 |
| **Líneas de Código** | 4,245 |
| **Endpoints API** | 80+ |
| **Validación** | ✅ Sintaxis correcta |
| **Estado** | 100% Producción |

---

## 🏗️ Arquitectura

```
backend/
├── main.py                 # Aplicación FastAPI principal (127 líneas)
├── config.py               # Configuración type-safe Pydantic (222 líneas)
├── database.py             # MongoDB async motor (177 líneas)
├── observability.py        # Health checks y auditoría (122 líneas)
├── requirements.txt        # Dependencias de producción
├── Dockerfile              # Contenedor de producción
│
├── models/
│   └── __init__.py         # Modelos User, Signal, Payment, PushSession (292 líneas)
│
├── routes/
│   ├── auth.py             # Login/Registro teléfono+password (258 líneas)
│   ├── users.py            # CRUD usuarios, watchlist, perfil (328 líneas)
│   ├── signals.py          # Gestión de señales de trading (590 líneas)
│   ├── market.py           # Datos mercado multi-exchange (490 líneas)
│   ├── payments.py         # Pagos USDT on-chain (359 líneas)
│   ├── admin.py            # Panel administración completo (505 líneas)
│   └── websocket.py        # Push notifications internas (300 líneas)
│
└── services/
    ├── __init__.py         # Exportación de servicios
    ├── scanner_service.py  # Scanner avanzado de mercado (229 líneas)
    └── scheduler_service.py # Tareas programadas (212 líneas)
```

---

## 🚀 Inicio Rápido

### Opción A: Docker Compose (Recomendado)

```bash
# 1. Configurar variables de entorno
cp config/.env.production .env
# Editar .env con tus valores reales

# 2. Iniciar todos los servicios
docker-compose up -d

# 3. Ver logs
docker-compose logs -f api

# 4. Verificar salud
curl http://localhost:8000/health
```

### Opción B: Local (Desarrollo)

```bash
# 1. Crear entorno virtual
python -m venv venv
source venv/bin/activate  # Linux/Mac
# o: venv\Scripts\activate  # Windows

# 2. Instalar dependencias
pip install -r backend/requirements.txt

# 3. Configurar variables de entorno
export MONGODB_URI="mongodb://localhost:27017"
export AUTH_SESSION_SECRET="tu-secret-key-min-32-characters"
export PAYMENT_TOKEN_CONTRACT="0x..."
export PAYMENT_RECEIVER_ADDRESS="0x..."

# 4. Ejecutar servidor
cd backend
uvicorn main:app --reload --host 0.0.0.0 --port 8000
```

---

## 📡 Endpoints Principales

### Autenticación
| Método | Endpoint | Descripción |
|--------|----------|-------------|
| POST | `/api/auth/register` | Registro con teléfono+password |
| POST | `/api/auth/login` | Login y obtención de JWT |
| POST | `/api/auth/refresh` | Refresh del token JWT |

### Usuarios
| Método | Endpoint | Descripción |
|--------|----------|-------------|
| GET | `/api/users/me` | Perfil del usuario actual |
| PUT | `/api/users/me` | Actualizar perfil |
| GET | `/api/users/me/watchlist` | Obtener watchlist |
| POST | `/api/users/me/watchlist` | Añadir a watchlist |
| DELETE | `/api/users/me/watchlist/{symbol}` | Eliminar de watchlist |

### Señales
| Método | Endpoint | Descripción |
|--------|----------|-------------|
| GET | `/api/signals` | Listar señales (filtrable) |
| GET | `/api/signals/{id}` | Obtener señal específica |
| POST | `/api/signals` | Crear nueva señal (admin) |
| PUT | `/api/signals/{id}` | Actualizar señal (admin) |
| DELETE | `/api/signals/{id}` | Eliminar señal (admin) |
| GET | `/api/signals/active` | Señales activas premium |
| POST | `/api/signals/{id}/close` | Cerrar señal (admin) |

### Mercado
| Método | Endpoint | Descripción |
|--------|----------|-------------|
| GET | `/api/market/prices` | Precios en tiempo real |
| GET | `/api/market/prices/{symbol}` | Precio de símbolo específico |
| GET | `/api/market/orderbook` | Libro de órdenes |
| GET | `/api/market/klines` | Velas japonesas |
| GET | `/api/market/exchanges` | Lista de exchanges |
| GET | `/api/market/symbols` | Símbolos disponibles |

### Pagos
| Método | Endpoint | Descripción |
|--------|----------|-------------|
| POST | `/api/payments/create-order` | Crear orden de pago |
| GET | `/api/payments/{order_id}` | Estado de orden |
| POST | `/api/payments/{order_id}/verify` | Verificar pago on-chain |
| GET | `/api/payments/history` | Historial de pagos |

### WebSockets (Push Notifications)
| Método | Endpoint | Descripción |
|--------|----------|-------------|
| WS | `/api/ws/connect` | Conexión WebSocket autenticada |

### Administración
| Método | Endpoint | Descripción |
|--------|----------|-------------|
| GET | `/api/admin/dashboard` | Dashboard estadísticas |
| GET | `/api/admin/users` | Listar todos los usuarios |
| PUT | `/api/admin/users/{id}` | Gestionar usuario (ban, plan) |
| GET | `/api/admin/reports/daily` | Reporte diario ingresos |
| GET | `/api/admin/reports/plans` | Reporte por planes |
| GET | `/api/admin/audit-log` | Log de auditoría |

---

## 🔧 Variables de Entorno Obligatorias

Ver `config/.env.production` para la lista completa (74 variables).

### Mínimas para producción:

```bash
# Base de datos
MONGODB_URI=mongodb+srv://USER:PASS@HOST/
DATABASE_NAME=hades_db

# Seguridad
AUTH_SESSION_SECRET=min-32-characters-random-secret-key
JWT_ALGORITHM=HS256
ACCESS_TOKEN_EXPIRE_MINUTES=30

# Pagos (USDT BEP-20)
PAYMENT_TOKEN_CONTRACT=0x55d398326f99059fF775485246999027B3197955
PAYMENT_RECEIVER_ADDRESS=0xTU_WALLET_ADDRESS
BSC_RPC_HTTP_URL=https://bsc-dataseed.binance.org

# CORS (orígenes permitidos)
CORS_ORIGINS=["https://tudominio.com","https://app.tudominio.com"]
```

---

## 🧪 Testing

```bash
# Instalar dependencias de testing
pip install pytest pytest-asyncio httpx

# Ejecutar tests
pytest backend/tests/ -v

# Con cobertura
pytest --cov=backend backend/tests/
```

---

## 📈 Monitoring

### Health Checks

```bash
# Salud básica
curl http://localhost:8000/health

# Live probe (Kubernetes)
curl http://localhost:8000/health/live

# Ready probe (dependencias)
curl http://localhost:8000/health/ready
```

### Logs

Los logs se guardan en `logs/` cuando se ejecuta en producción.

```bash
# Ver logs en tiempo real
tail -f logs/app.log

# Ver errores
grep ERROR logs/app.log
```

---

## 🔐 Seguridad Implementada

- ✅ Password hashing con bcrypt (12 salt rounds)
- ✅ JWT tokens con expiración configurable
- ✅ Rate limiting en endpoints críticos
- ✅ CORS configurado por origen
- ✅ Validación de input con Pydantic
- ✅ Sanitización de queries MongoDB
- ✅ Usuario no-root en contenedor Docker
- ✅ Health checks sin información sensible

---

## 📝 Notas de Producción

1. **MongoDB**: Usar MongoDB Atlas o instancia gestionada para producción
2. **Secrets**: Nunca commitear `.env` al repositorio
3. **Backups**: Configurar backups automáticos de MongoDB
4. **SSL**: Usar nginx con SSL certificado en producción
5. **Workers**: El Dockerfile usa 4 workers, ajustar según CPU disponible

---

## 🆘 Soporte

Para issues o preguntas, revisar logs y health endpoints primero.

```bash
# Debug mode (desarrollo)
uvicorn main:app --reload --log-level debug
```
