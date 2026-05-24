// HADES App — Service Worker
// Versión: 1.0.1
// Estrategia: Cache-first para assets estáticos, Network-first para API

const CACHE_NAME = 'hades-v_ecosystem_1'; // FIX: bump para desalojar caché vieja que guardó manifest roto
const STATIC_ASSETS = [
  '/miniapp/static/index.html',
  '/miniapp/static/app.css',
  '/miniapp/static/app.js',
  '/miniapp/static/logo.png',
  '/miniapp/static/icon-192x192.png',
  '/miniapp/static/icon-512x512.png',
  'https://telegram.org/js/telegram-web-app.js'
];

// ── Install: pre-cachear assets estáticos ──────────────────────────────────
self.addEventListener('install', event => {
  event.waitUntil(
    caches.open(CACHE_NAME).then(cache => {
      return cache.addAll(STATIC_ASSETS).catch(err => {
        // No bloquear la instalación si algún asset externo falla
        console.warn('[SW] Algunos assets no se pudieron cachear:', err);
      });
    }).then(() => self.skipWaiting())
  );
});

// ── Activate: limpiar caches viejas ───────────────────────────────────────
self.addEventListener('activate', event => {
  event.waitUntil(
    caches.keys().then(keys =>
      Promise.all(
        keys
          .filter(key => key !== CACHE_NAME)
          .map(key => caches.delete(key))
      )
    ).then(() => self.clients.claim())
  );
});

// ── Fetch: estrategia por tipo de recurso ────────────────────────────────
self.addEventListener('fetch', event => {
  const { request } = event;
  const url = new URL(request.url);

  // Ignorar requests no-GET y chrome-extension
  if (request.method !== 'GET') return;
  if (url.protocol === 'chrome-extension:') return;

  // API calls → Network-first (datos frescos), fallback a caché
  if (url.pathname.startsWith('/miniapp/api/') || url.pathname.startsWith('/api/miniapp/')) {
    event.respondWith(networkFirst(request));
    return;
  }

  // Assets estáticos → Cache-first (velocidad)
  if (
    url.pathname.startsWith('/miniapp/static/') ||
    url.hostname === 'telegram.org'
  ) {
    event.respondWith(cacheFirst(request));
    return;
  }

  // Todo lo demás → Network-first
  event.respondWith(networkFirst(request));
});

// ── Estrategias ────────────────────────────────────────────────────────────
async function cacheFirst(request) {
  const cached = await caches.match(request);
  if (cached) return cached;
  try {
    const response = await fetch(request);
    if (response.ok) {
      const cache = await caches.open(CACHE_NAME);
      cache.put(request, response.clone());
    }
    return response;
  } catch {
    return new Response('Recurso no disponible offline', { status: 503 });
  }
}

async function networkFirst(request) {
  try {
    const response = await fetch(request);
    if (response.ok) {
      const cache = await caches.open(CACHE_NAME);
      cache.put(request, response.clone());
    }
    return response;
  } catch {
    const cached = await caches.match(request);
    if (cached) return cached;
    // Si es navegación, devolver el index para SPA
    if (request.mode === 'navigate') {
      // FIX: intentar primero /miniapp (la ruta real de la app),
      // luego el fallback estático como último recurso
      return caches.match('/miniapp') || caches.match('/miniapp/static/index.html');
    }
    return new Response(JSON.stringify({ error: 'Sin conexión' }), {
      status: 503,
      headers: { 'Content-Type': 'application/json' }
    });
  }
}

// ── Push Notifications (preparado para el futuro) ────────────────────────
self.addEventListener('push', event => {
  if (!event.data) return;
  const data = event.data.json();
  event.waitUntil(
    self.registration.showNotification(data.title || 'HADES App', {
      body: data.body || '',
      icon: '/miniapp/static/icon-192x192.png',
      badge: '/miniapp/static/icon-96x96.png',
      data: data.url || '/',
      vibrate: [200, 100, 200]
    })
  );
});

self.addEventListener('notificationclick', event => {
  event.notification.close();
  event.waitUntil(
    clients.openWindow(event.notification.data || '/')
  );
});

// ecosystem-bridge-fetch-fix-v2
