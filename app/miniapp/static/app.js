const tg = window.Telegram?.WebApp;
if (tg) {
  tg.ready();
  tg.expand();
}

const DEFAULT_RADAR_VIEW = {
  search: '',
  direction: 'all',
  priority: 'all',
  proximity: 'all',
  signal: 'all',
  execution: 'all',
  alignment: 'all',
  sort: 'ranking',
  offset: 0,
};

const state = {
  token: null,
  payload: null,
  authMe: null,
  bootstrapRequestInFlight: false,
  currentView: 'home',
  signalDetail: null,
  radarDetail: null,
  radarView: { ...DEFAULT_RADAR_VIEW },
  historyFilter: 'all',
  accountNotice: null,
  adminPanel: {
    overview: null,
    loading: false,
    notice: null,
    confirmReset: false,
    lastResetSummary: null,
    manualActivation: {
      lookup: null,
      lookupLoading: false,
      activationLoading: false,
      draft: {
        userId: '',
        plan: 'plus',
        days: '30',
      },
    },
    moderation: {
      actionLoading: false,
      confirmAction: null,
      draft: {
        durationValue: '7',
        durationUnit: 'days',
      },
    },
  },
  riskCenter: {
    payload: null,
    loading: false,
    notice: null,
    query: {
      signalId: null,
      profile: null,
      leverage: null,
    },
  },
  performanceCenter: {
    payload: null,
    loading: false,
    notice: null,
    query: {
      days: 30,
    },
  },
  settingsCenter: {
    payload: null,
    loading: false,
    notice: null,
  },
  liveSignals: {
    timer: null,
    requestInFlight: false,
    feedVersion: null,
    lastSyncedAt: null,
  },
  lazy: {
    dashboard: { loaded: false, loading: false },
    signals: { loaded: false, loading: false },
    history: { loaded: false, loading: false },
    market: { loaded: false, loading: false, error: null },
    account: { loaded: false, loading: false },
  },
};

const LIVE_SIGNALS_HOME_POLL_INTERVAL_MS = 15000;
const LIVE_SIGNALS_VIEW_POLL_INTERVAL_MS = 8000;
const LIVE_SIGNALS_FOCUS_DEBOUNCE_MS = 2500;
const PAYLOAD_CACHE_TTL_MS = 10 * 60 * 1000;
const PAYLOAD_CACHE_PREFIX = 'hades-miniapp-payload-v3';

const els = {
  loading: document.getElementById('loading'),
  content: document.getElementById('content'),
  bottomNav: document.getElementById('bottomNav'),
  titleMain: document.getElementById('titleMain'),
  planBadge: document.getElementById('planBadge'),
  daysBadge: document.getElementById('daysBadge'),
  home: document.getElementById('view-home'),
  signals: document.getElementById('view-signals'),
  market: document.getElementById('view-market'),
  history: document.getElementById('view-history'),
  account: document.getElementById('view-account'),
  performance: document.getElementById('view-performance'),
  risk: document.getElementById('view-risk'),
  settings: document.getElementById('view-settings'),
  admin: document.getElementById('view-admin'),
  signalDetailModal: document.getElementById('signalDetailModal'),
  signalDetailTitle: document.getElementById('signalDetailTitle'),
  signalDetailBody: document.getElementById('signalDetailBody'),
  signalDetailClose: document.getElementById('signalDetailClose'),
};

const labels = {
  home: 'Dashboard',
  signals: 'Señales',
  market: 'Mercado',
  history: 'Historial',
  account: 'Cuenta',
  performance: 'Rendimiento',
  risk: 'Gestión de riesgo',
  settings: 'Ajustes',
  admin: 'Panel admin',
};

function getPayloadCacheKey(userId) {
  const normalized = Number(userId || 0);
  return `${PAYLOAD_CACHE_PREFIX}:${normalized}`;
}

function loadCachedPayload(userId) {
  const normalized = Number(userId || 0);
  if (!normalized) return null;
  try {
    const raw = window.localStorage.getItem(getPayloadCacheKey(normalized));
    if (!raw) return null;
    const parsed = JSON.parse(raw);
    const cachedAt = Number(parsed?.cached_at || 0);
    if (!cachedAt || (Date.now() - cachedAt) > PAYLOAD_CACHE_TTL_MS) {
      window.localStorage.removeItem(getPayloadCacheKey(normalized));
      return null;
    }
    const payload = parsed?.payload;
    return payload && typeof payload === 'object' ? payload : null;
  } catch {
    return null;
  }
}

function persistPayloadCache() {
  const userId = Number(state.payload?.me?.user_id || state.authMe?.user_id || 0);
  if (!userId || !state.payload || typeof state.payload !== 'object') return;
  try {
    window.localStorage.setItem(getPayloadCacheKey(userId), JSON.stringify({
      cached_at: Date.now(),
      payload: state.payload,
    }));
  } catch {}
}

function primePayloadShell(me = {}) {
  ensurePayloadShell();
  state.payload.bootstrap_mode = 'light';
  state.payload.me = { ...(state.payload.me || {}), ...(me || {}) };
  if (!state.payload.market || typeof state.payload.market !== 'object') state.payload.market = {};
  if (!state.payload.market.recommendation) state.payload.market.recommendation = 'Cargando lectura de mercado...';
  if (!state.payload.market.radar_context || typeof state.payload.market.radar_context !== 'object') {
    state.payload.market.radar_context = {
      bias: 'neutral',
      regime: 'neutral',
      environment: '—',
      recommendation: 'Cargando lectura de mercado...',
    };
  }
  markLazyStateFromBootstrap();
}

function applyBootstrapPayload(payload, { persist = true } = {}) {
  const incoming = payload && typeof payload === 'object' ? payload : {};
  const isLightBootstrap = String(incoming?.bootstrap_mode || '').toLowerCase() === 'light';
  const hasExistingPayload = state.payload && typeof state.payload === 'object';

  if (isLightBootstrap && hasExistingPayload) {
    state.payload = {
      ...state.payload,
      ...incoming,
      me: { ...(state.payload.me || {}), ...(incoming.me || {}) },
      dashboard: Object.keys(state.payload.dashboard || {}).length
        ? { ...(incoming.dashboard || {}), ...(state.payload.dashboard || {}) }
        : { ...(incoming.dashboard || {}) },
      market: Object.keys(state.payload.market || {}).length
        ? { ...(incoming.market || {}), ...(state.payload.market || {}) }
        : { ...(incoming.market || {}) },
      watchlist_meta: Object.keys(state.payload.watchlist_meta || {}).length
        ? { ...(incoming.watchlist_meta || {}), ...(state.payload.watchlist_meta || {}) }
        : { ...(incoming.watchlist_meta || {}) },
      account: Object.keys(incoming.account || {}).length ? incoming.account : (state.payload.account || {}),
      plans: Object.keys(incoming.plans || {}).length ? incoming.plans : (state.payload.plans || {}),
      signals: Array.isArray(incoming.signals) && incoming.signals.length ? incoming.signals : (Array.isArray(state.payload.signals) ? state.payload.signals : []),
      history: Array.isArray(incoming.history) && incoming.history.length ? incoming.history : (Array.isArray(state.payload.history) ? state.payload.history : []),
      watchlist: Array.isArray(incoming.watchlist) && incoming.watchlist.length ? incoming.watchlist : (Array.isArray(state.payload.watchlist) ? state.payload.watchlist : []),
    };
  } else {
    state.payload = incoming;
  }

  ensurePayloadShell();
  if (state.authMe && typeof state.authMe === 'object') {
    state.payload.me = { ...(state.payload.me || {}), ...state.authMe };
  }
  markLazyStateFromBootstrap();
  state.liveSignals.feedVersion = null;
  if (persist) persistPayloadCache();
  renderAll();
}

function restoreCachedPayload() {
  const userId = Number(state.authMe?.user_id || 0);
  if (!userId) return false;
  const cachedPayload = loadCachedPayload(userId);
  if (!cachedPayload) return false;
  applyBootstrapPayload(cachedPayload, { persist: false });
  return true;
}

function escapeHtml(value) {
  return String(value ?? '')
    .replaceAll('&', '&amp;')
    .replaceAll('<', '&lt;')
    .replaceAll('>', '&gt;')
    .replaceAll('"', '&quot;')
    .replaceAll("'", '&#039;');
}

function formatNumber(value, digits = 2) {
  if (value === null || value === undefined || Number.isNaN(Number(value))) return '—';
  return Number(value).toFixed(digits);
}

function priceDigits(value, minDigits = 2) {
  const num = Math.abs(Number(value));
  if (!Number.isFinite(num) || num === 0) return Math.max(2, Math.min(minDigits, 12));
  let autoDigits = 4;
  if (num >= 1000) autoDigits = 2;
  else if (num >= 100) autoDigits = 3;
  else if (num >= 1) autoDigits = 4;
  else if (num >= 0.1) autoDigits = 5;
  else if (num >= 0.01) autoDigits = 7;
  else if (num >= 0.001) autoDigits = 8;
  else if (num >= 0.0001) autoDigits = 10;
  else autoDigits = 12;
  return Math.max(minDigits, autoDigits);
}

function formatPrice(value, minDigits = 2) {
  if (value === null || value === undefined || Number.isNaN(Number(value))) return '—';
  const digits = Math.min(priceDigits(value, minDigits), 12);
  return Number(value).toFixed(digits).replace(/\.0+$/, '').replace(/(\.\d*?)0+$/, '$1');
}

function formatMoney(value) {
  if (value === null || value === undefined || Number.isNaN(Number(value))) return '—';
  return `${Number(value).toFixed(3)} USDT`;
}

function formatDate(value) {
  if (!value) return '—';
  try {
    return new Date(value).toLocaleString();
  } catch {
    return String(value);
  }
}

function formatPercentSigned(value, digits = 2) {
  if (value === null || value === undefined || Number.isNaN(Number(value))) return '—';
  const num = Number(value);
  const prefix = num > 0 ? '+' : '';
  return `${prefix}${num.toFixed(digits)}%`;
}

function formatFractionPercent(value, digits = 2) {
  if (value === null || value === undefined || Number.isNaN(Number(value))) return '—';
  return `${(Number(value) * 100).toFixed(digits)}%`;
}

function billingToneClass(value) {
  const normalized = String(value || '').toLowerCase();
  if (normalized === 'positive') return 'is-positive';
  if (normalized === 'warning') return 'is-warning';
  if (normalized === 'accent') return 'is-accent';
  return '';
}

function billingStepClass(value) {
  const normalized = String(value || '').toLowerCase();
  if (normalized === 'done') return 'is-done';
  if (normalized === 'current') return 'is-current';
  if (normalized === 'blocked') return 'is-blocked';
  return 'is-upcoming';
}

function paymentReasonMessage(reason, fallbackOk) {
  const normalized = String(reason || '').toLowerCase();
  const map = {
    payment_confirmed: 'Pago confirmado correctamente.',
    already_completed: 'La orden ya estaba completada.',
    verification_in_progress: 'Ya hay una verificación en curso para esa orden.',
    verification_error: 'La verificación falló temporalmente. Vuelve a intentarlo en unos segundos.',
    order_expired: 'La orden expiró. Genera una nueva si todavía quieres pagar.',
    order_cancelled: 'La orden ya estaba cancelada.',
    tx_already_used: 'Esa transacción ya fue usada por otra orden.',
    payment_config_missing: 'La configuración de pagos no está lista todavía.',
    activation_failed: 'El pago se detectó, pero la activación falló. Revisa soporte.',
    no_match: 'Todavía no aparece un pago válido para esa orden.',
    no_transfer_found: 'Todavía no aparece una transferencia válida para esa orden.',
    payment_not_found: 'Todavía no aparece una transferencia válida para esa orden.',
    awaiting_confirmations: 'Se detectó el pago, pero aún faltan confirmaciones.',
    payment_waiting_confirmations: 'Se detectó el pago, pero aún faltan confirmaciones.',
  };
  return map[normalized] || fallbackOk || 'Estado de pago actualizado.';
}

function profileLabel(value) {
  const map = { conservador: 'Conservador', moderado: 'Moderado', agresivo: 'Agresivo' };
  return map[String(value || '').toLowerCase()] || String(value || '—');
}

function formatCompactAmount(value) {
  if (value === null || value === undefined || Number.isNaN(Number(value))) return '—';
  const num = Number(value);
  const abs = Math.abs(num);
  if (abs >= 1_000_000_000) return `${(num / 1_000_000_000).toFixed(2)}B`;
  if (abs >= 1_000_000) return `${(num / 1_000_000).toFixed(2)}M`;
  if (abs >= 1_000) return `${(num / 1_000).toFixed(2)}K`;
  return num.toFixed(0);
}

function formatStatusLabel(value) {
  const normalized = String(value || '').toLowerCase();
  const map = {
    free: 'Free',
    trial: 'Trial',
    active: 'Activo',
    expired: 'Expirado',
    banned: 'Bloqueado',
    awaiting_payment: 'Esperando pago',
    verification_in_progress: 'Verificando',
    paid_unconfirmed: 'Pago sin confirmar',
    completed: 'Completado',
    cancelled: 'Cancelado',
    expired_order: 'Expirada',
  };
  return map[normalized] || String(value || '—').toUpperCase();
}

function resultLabel(item) {
  const resolution = String(item?.resolution || '').toLowerCase();
  const result = String(item?.result || '').toLowerCase();
  if (resolution === 'tp2') return 'TP2';
  if (resolution === 'tp1') return 'TP1';
  if (resolution === 'sl') return 'SL';
  if (resolution === 'expired_clean' || result === 'expired') return 'EXP';
  if (result === 'won') return 'WIN';
  if (result === 'lost') return 'LOSS';
  return '—';
}

function badgeClassByResult(itemOrResult) {
  const normalized = typeof itemOrResult === 'object' && itemOrResult !== null
    ? String(itemOrResult.resolution || itemOrResult.result || '').toLowerCase()
    : String(itemOrResult || '').toLowerCase();
  if (normalized === 'tp1' || normalized === 'tp2' || normalized === 'won') return 'result-badge result-won';
  if (normalized === 'sl' || normalized === 'lost') return 'result-badge result-lost';
  return 'result-badge result-expired';
}

function dirClass(direction) {
  return String(direction).toUpperCase() === 'SHORT' ? 'dir-badge dir-short' : 'dir-badge dir-long';
}

function sideClassByValue(value) {
  return Number(value || 0) >= 0 ? 'positive-text' : 'negative-text';
}

function watchlistBiasClass(label) {
  const normalized = String(label || '').toLowerCase();
  if (normalized.includes('máximo')) return 'positive-text';
  if (normalized.includes('mínimo')) return 'negative-text';
  return '';
}

function formatInteger(value) {
  if (value === null || value === undefined || Number.isNaN(Number(value))) return '—';
  return Number(value).toLocaleString();
}

function watchlistRangePosition(value) {
  if (value === null || value === undefined || Number.isNaN(Number(value))) return '—';
  return `${Number(value).toFixed(0)}% del rango`;
}

function watchlistPriorityClass(label) {
  const normalized = String(label || '').toLowerCase();
  if (normalized.includes('máxima') || normalized.includes('setup activo')) return 'watchlist-pill-critical';
  if (normalized.includes('alta')) return 'watchlist-pill-strong';
  if (normalized.includes('media')) return 'watchlist-pill-medium';
  return 'watchlist-pill-soft';
}

function watchlistProximityClass(label) {
  const normalized = String(label || '').toLowerCase();
  if (normalized.includes('setup activo') || normalized.includes('muy alta')) return 'watchlist-pill-critical';
  if (normalized.includes('alta')) return 'watchlist-pill-strong';
  if (normalized.includes('media')) return 'watchlist-pill-medium';
  return 'watchlist-pill-soft';
}

function watchlistSignalSummary(signal) {
  if (!signal) return 'Sin señal';
  const bits = [];
  if (signal.direction) bits.push(String(signal.direction).toUpperCase());
  if (signal.visibility_name || signal.visibility) bits.push(String(signal.visibility_name || signal.visibility).toUpperCase());
  if (signal.score !== null && signal.score !== undefined && !Number.isNaN(Number(signal.score))) bits.push(`Score ${formatNumber(signal.score, 0)}`);
  return bits.join(' · ') || 'Sin señal';
}

function radarWindowClass(label) {
  const normalized = String(label || '').toLowerCase();
  if (normalized.includes('seguimiento') || normalized.includes('inmediata')) return 'watchlist-pill-critical';
  if (normalized.includes('intradía')) return 'watchlist-pill-strong';
  if (normalized.includes('preparando')) return 'watchlist-pill-medium';
  return 'watchlist-pill-soft';
}

function radarConvictionClass(label) {
  const normalized = String(label || '').toLowerCase();
  if (normalized.includes('seguimiento')) return 'watchlist-pill-active';
  if (normalized.includes('alta')) return 'watchlist-pill-strong';
  if (normalized.includes('media')) return 'watchlist-pill-medium';
  return 'watchlist-pill-soft';
}

function radarSignalContextClass(label) {
  const normalized = String(label || '').toLowerCase();
  if (normalized.includes('activa')) return 'watchlist-pill-active';
  if (normalized.includes('reciente')) return 'watchlist-pill-strong';
  return 'watchlist-pill-soft';
}

function radarExecutionClass(label) {
  const normalized = String(label || '').toLowerCase();
  if (normalized.includes('seguimiento')) return 'watchlist-pill-active';
  if (normalized.includes('ejecutable')) return 'watchlist-pill-critical';
  if (normalized.includes('preparación')) return 'watchlist-pill-strong';
  if (normalized.includes('observación')) return 'watchlist-pill-medium';
  return 'watchlist-pill-soft';
}

function radarAlignmentClass(label) {
  const normalized = String(label || '').toLowerCase();
  if (normalized.includes('a favor')) return 'watchlist-pill-critical';
  if (normalized.includes('flujo')) return 'watchlist-pill-strong';
  if (normalized.includes('selectivo')) return 'watchlist-pill-medium';
  return 'watchlist-pill-soft';
}

function radarRiskClass(label) {
  const normalized = String(label || '').toLowerCase();
  if (normalized.includes('gestionar')) return 'watchlist-pill-active';
  if (normalized.includes('normal')) return 'watchlist-pill-strong';
  if (normalized.includes('cauto')) return 'watchlist-pill-medium';
  return 'watchlist-pill-soft';
}

function normalizeTextLookup(value) {
  return String(value || '')
    .normalize('NFD')
    .replace(/[̀-ͯ]/g, '')
    .toLowerCase();
}

function radarFilterCount(items, predicate) {
  return items.filter(predicate).length;
}

function sortRadarItems(items, sortKey) {
  const copy = [...items];
  copy.sort((a, b) => {
    const activeDelta = Number(Boolean(b.has_active_signal)) - Number(Boolean(a.has_active_signal));
    if (activeDelta !== 0 && sortKey !== 'change') return activeDelta;

    const comparators = {
      ranking: [Number(b.ranking_score || 0) - Number(a.ranking_score || 0), Number(b.priority_score || 0) - Number(a.priority_score || 0)],
      priority: [Number(b.priority_score || 0) - Number(a.priority_score || 0), Number(b.proximity_score || 0) - Number(a.proximity_score || 0)],
      proximity: [Number(b.proximity_score || 0) - Number(a.proximity_score || 0), Number(b.priority_score || 0) - Number(a.priority_score || 0)],
      execution: [Number(b.execution_rank || 0) - Number(a.execution_rank || 0), Number(b.ranking_score || 0) - Number(a.ranking_score || 0)],
      score: [Number(b.final_score || 0) - Number(a.final_score || 0), Number(b.priority_score || 0) - Number(a.priority_score || 0)],
      volume: [Number(b.quote_volume || 0) - Number(a.quote_volume || 0), Number(b.activity_score || 0) - Number(a.activity_score || 0)],
      change: [Math.abs(Number(b.change_pct || 0)) - Math.abs(Number(a.change_pct || 0)), Number(b.final_score || 0) - Number(a.final_score || 0)],
    };

    const selected = comparators[sortKey] || comparators.ranking;
    for (const delta of selected) {
      if (delta !== 0) return delta;
    }
    return String(a.symbol || '').localeCompare(String(b.symbol || ''));
  });
  return copy;
}

function getRadarPresentation(items, view) {
  const search = normalizeTextLookup(view?.search || '');
  let filtered = [...(items || [])];
  if (search) {
    filtered = filtered.filter(item => {
      const haystack = normalizeTextLookup(`${item.symbol || ''} ${item.direction || ''} ${item.action_label || ''} ${item.reason_short || ''} ${(item.reasons || []).join(' ')}`);
      return haystack.includes(search);
    });
  }
  if (view?.direction && view.direction !== 'all') {
    filtered = filtered.filter(item => String(item.direction || '').toLowerCase() === String(view.direction).toLowerCase());
  }
  if (view?.priority && view.priority !== 'all') {
    filtered = filtered.filter(item => String(item.priority_label || '') === String(view.priority));
  }
  if (view?.proximity && view.proximity !== 'all') {
    filtered = filtered.filter(item => String(item.proximity_label || '') === String(view.proximity));
  }
  if (view?.signal && view.signal !== 'all') {
    if (view.signal === 'active') filtered = filtered.filter(item => Boolean(item.has_active_signal));
    if (view.signal === 'recent') filtered = filtered.filter(item => !item.has_active_signal && Boolean(item.latest_signal));
    if (view.signal === 'none') filtered = filtered.filter(item => !item.has_active_signal && !item.latest_signal);
  }
  if (view?.execution && view.execution !== 'all') {
    filtered = filtered.filter(item => String(item.execution_state_label || '') === String(view.execution));
  }
  if (view?.alignment && view.alignment !== 'all') {
    filtered = filtered.filter(item => String(item.alignment_label || '') === String(view.alignment));
  }
  return sortRadarItems(filtered, view?.sort || 'ranking');
}

function radarSortLabel(value) {
  const map = {
    ranking: 'Ranking',
    priority: 'Prioridad',
    proximity: 'Proximidad',
    execution: 'Estado operativo',
    score: 'Score radar',
    volume: 'Volumen',
    change: 'Movimiento 24h',
  };
  return map[String(value || '').toLowerCase()] || 'Ranking';
}

const RADAR_VISIBLE_COUNT = 6;

function getRadarWindow(items, offset = 0, visibleCount = RADAR_VISIBLE_COUNT) {
  const source = Array.isArray(items) ? items : [];
  const total = source.length;
  const count = Math.max(1, Number(visibleCount || RADAR_VISIBLE_COUNT));
  const normalizedOffset = total > count ? (((Number(offset || 0) % total) + total) % total) : 0;
  const windowItems = total <= count ? source.slice(0, count) : source.slice(normalizedOffset, normalizedOffset + count);
  return {
    items: windowItems,
    total,
    count,
    start: total ? normalizedOffset + 1 : 0,
    end: total ? normalizedOffset + windowItems.length : 0,
    canRotate: total > count,
    offset: normalizedOffset,
  };
}

function rotateRadarWindow(items, visibleCount = RADAR_VISIBLE_COUNT) {
  const total = Array.isArray(items) ? items.length : 0;
  const count = Math.max(1, Number(visibleCount || RADAR_VISIBLE_COUNT));
  const currentOffset = Number(state.radarView?.offset || 0);
  const nextOffset = total <= count || currentOffset + count >= total ? 0 : currentOffset + count;
  state.radarView = { ...state.radarView, offset: nextOffset };
}

function resetRadarView(patch = {}) {
  state.radarView = { ...DEFAULT_RADAR_VIEW, ...patch, offset: 0 };
}

function radarWindowMeta(windowState, totalUniverse) {
  if (!windowState.total) return `Mostrando 0 de ${Number(totalUniverse || 0)}`;
  const suffix = totalUniverse > windowState.total
    ? ` · filtrados de ${Number(totalUniverse || 0)}`
    : ` de ${Number(totalUniverse || 0)}`;
  return `Mostrando ${windowState.start}–${windowState.end}${suffix}`;
}


function metricToneClass(kind, value) {
  const num = Number(value || 0);
  if (kind === 'pf') {
    if (num >= 1.5) return 'metric-positive';
    if (num >= 1.0) return 'metric-warning';
    return 'metric-negative';
  }
  if (kind === 'expectancy') {
    if (num > 0.15) return 'metric-positive';
    if (num >= 0) return 'metric-warning';
    return 'metric-negative';
  }
  if (kind === 'drawdown') {
    if (num <= 4) return 'metric-positive';
    if (num <= 8) return 'metric-warning';
    return 'metric-negative';
  }
  if (kind === 'winrate') {
    if (num >= 60) return 'metric-positive';
    if (num >= 50) return 'metric-warning';
    return 'metric-negative';
  }
  return '';
}

function summaryDiagnosis(summary) {
  const pfInfinite = Boolean(summary?.profit_factor_infinite);
  const pf = pfInfinite ? Infinity : Number(summary?.profit_factor || 0);
  const exp = Number(summary?.expectancy_r || 0);
  const dd = Number(summary?.max_drawdown_r || 0);
  if ((pfInfinite || pf >= 1.5) && exp > 0 && dd <= 5) {
    return {
      tone: 'diagnostic-positive',
      title: 'Sistema con edge positivo',
      text: 'La ventana actual muestra un perfil rentable: PF por R sólido, expectativa positiva y drawdown contenido.',
    };
  }
  if (pf >= 1.0 && exp >= 0) {
    return {
      tone: 'diagnostic-warning',
      title: 'Sistema operativo, pero vigilando riesgo',
      text: 'La estructura sigue viva, pero conviene vigilar el drawdown y la calidad reciente de resolución.',
    };
  }
  return {
    tone: 'diagnostic-negative',
    title: 'Ventana débil o deteriorada',
    text: 'La lectura actual sugiere pérdida de edge o mala relación entre ganadoras y perdedoras. Revisa resolución y setups.',
  };
}

function resolutionCard(label, value, subtitle, toneClass = '') {
  return `
    <div class="resolution-card ${toneClass}">
      <div class="resolution-label">${escapeHtml(label)}</div>
      <div class="resolution-value">${escapeHtml(value)}</div>
      <div class="resolution-subtitle">${escapeHtml(subtitle)}</div>
    </div>
  `;
}

function showError(message) {
  els.home.innerHTML = `<div class="error-banner">${escapeHtml(message)}</div>`;
  els.loading.classList.add('hidden');
  els.content.classList.remove('hidden');
  els.bottomNav.classList.remove('hidden');
}

async function api(path, options = {}) {
  const headers = Object.assign({}, options.headers || {});
  if (state.token) headers.Authorization = `Bearer ${state.token}`;
  if (options.body && !headers['Content-Type']) headers['Content-Type'] = 'application/json';
  const response = await fetch(path, { ...options, headers });
  const data = await response.json().catch(() => ({}));
  if (!response.ok) throw new Error(data.detail || data.message || 'request_failed');
  return data;
}

function openExternalUrl(url) {
  const normalized = String(url || '').trim();
  if (!normalized) return;
  try {
    if (window.Telegram?.WebApp?.openLink) {
      window.Telegram.WebApp.openLink(normalized, { try_instant_view: false });
      return;
    }
  } catch (_) {}
  window.open(normalized, '_blank', 'noopener,noreferrer');
}


async function createAndOpenSentinelLink(button = null) {
  const original = button ? button.textContent : '';
  if (button) {
    button.disabled = true;
    button.textContent = 'Vinculando...';
  }
  try {
    const result = await api('/api/miniapp/sentinel/link', { method: 'POST' });
    if (!result?.url) throw new Error('sentinel_link_unavailable');
    const days = Number(result.days_left || 0);
    setAccountNotice(`Sentinel vinculado. La sesión queda activa hasta el vencimiento premium${days ? ` (${days} días restantes)` : ''}.`, 'positive');
    openExternalUrl(result.url);
  } catch (error) {
    const message = String(error.message || 'No se pudo vincular Sentinel.');
    const display = message === 'premium_required'
      ? 'Sentinel requiere PREMIUM activo. Renueva o activa tu plan para vincularlo.'
      : `No se pudo vincular Sentinel: ${message}`;
    setAccountNotice(display, 'warning');
    renderAccount();
    bindViewButtons();
    tg?.showAlert(display);
  } finally {
    if (button && button.isConnected) {
      button.disabled = false;
      button.textContent = original;
    }
  }
}

async function createAndOpenOraculumLink(button = null) {
  const original = button ? button.textContent : '';
  if (button) {
    button.disabled = true;
    button.textContent = 'Vinculando...';
  }
  try {
    const result = await api('/api/miniapp/oraculum/link', { method: 'POST' });
    if (!result?.url) throw new Error('oraculum_link_unavailable');
    const days = Number(result.days_left || 0);
    setAccountNotice(`Oraculum vinculado. La sesión queda activa hasta el vencimiento premium${days ? ` (${days} días restantes)` : ''}.`, 'positive');
    openExternalUrl(result.url);
  } catch (error) {
    const message = String(error.message || 'No se pudo vincular Oraculum.');
    const display = message === 'premium_required'
      ? 'Oraculum requiere PREMIUM activo. Renueva o activa tu plan para vincularlo.'
      : `No se pudo vincular Oraculum: ${message}`;
    setAccountNotice(display, 'warning');
    renderAccount();
    bindViewButtons();
    tg?.showAlert(display);
  } finally {
    if (button && button.isConnected) {
      button.disabled = false;
      button.textContent = original;
    }
  }
}

const SESSION_TOKEN_KEY = 'hades_session_token';
const SESSION_ME_KEY = 'hades_session_me';

// Leer token del query param si viene desde Telegram via botón conectar
(function() {
  try {
    const params = new URLSearchParams(window.location.search);
    const t = params.get('pwa_token');
    if (t) {
      window.localStorage.setItem(SESSION_TOKEN_KEY, t);
      // Limpiar el param sin recargar
      params.delete('pwa_token');
      const newUrl = window.location.pathname + (params.toString() ? '?' + params.toString() : '');
      window.history.replaceState(null, '', newUrl);
    }
  } catch(_) {}
})();

async function authenticate() {
  const params = new URLSearchParams(window.location.search);
  const devUserId = params.get('dev_user_id');
  const initData = tg?.initData || '';

  // Desde Telegram: autenticar y guardar token
  if (initData) {
    const auth = await api('/api/miniapp/auth', {
      method: 'POST',
      body: JSON.stringify({ init_data: initData, dev_user_id: devUserId ? Number(devUserId) : null }),
    });
    state.token = auth.session_token;
    state.authMe = auth?.me && typeof auth.me === 'object' ? auth.me : null;
    try {
      window.localStorage.setItem(SESSION_TOKEN_KEY, auth.session_token);
      if (state.authMe) window.localStorage.setItem(SESSION_ME_KEY, JSON.stringify(state.authMe));
    } catch (_) {}
    return auth;
  }

  // Desde PWA directa: reusar token guardado
  const savedToken = (() => { try { return window.localStorage.getItem(SESSION_TOKEN_KEY); } catch(_) { return null; } })();
  if (savedToken) {
    // Verificar que el token no esté expirado (parsear el payload)
    try {
      const [bodyB64] = savedToken.split('.');
      const pad = bodyB64.length % 4 ? 4 - (bodyB64.length % 4) : 0;
      const body = JSON.parse(atob((bodyB64 + '='.repeat(pad)).replace(/-/g,'+').replace(/_/g,'/')));
      if (body.exp && body.exp < Math.floor(Date.now() / 1000)) {
        // Token expirado: limpiar y pedir al usuario que abra desde Telegram
        window.localStorage.removeItem(SESSION_TOKEN_KEY);
        window.localStorage.removeItem(SESSION_ME_KEY);
        throw new Error('Sesión expirada. Abre la app desde Telegram para renovar tu sesión.');
      }
    } catch (parseErr) {
      if (parseErr.message.includes('Sesión expirada')) throw parseErr;
      // Si no se puede parsear, usar el token igual y dejar que el servidor decida
    }
    state.token = savedToken;
    const savedMe = (() => { try { return JSON.parse(window.localStorage.getItem(SESSION_ME_KEY) || 'null'); } catch(_) { return null; } })();
    state.authMe = savedMe;
    return { session_token: savedToken, me: savedMe };
  }

  // Sin token guardado: pedir que abra desde Telegram
  throw new Error('Abre la app desde Telegram la primera vez para iniciar sesión.');
}

async function bootstrap() {
  if (!state.token || state.bootstrapRequestInFlight) return state.payload;
  state.bootstrapRequestInFlight = true;
  try {
    const payload = await api('/api/miniapp/bootstrap');
    applyBootstrapPayload(payload);
    return state.payload;
  } finally {
    state.bootstrapRequestInFlight = false;
  }
}

function ensureDashboardShell() {
  ensurePayloadShell();
  if (!state.payload.dashboard || typeof state.payload.dashboard !== 'object') state.payload.dashboard = {};
  if (!Array.isArray(state.payload.signals)) state.payload.signals = [];
}

function applyLiveSignalsPayload(payload = {}) {
  ensureDashboardShell();
  state.payload.dashboard.active_signals_count = Number(payload.active_signals_count || 0);
  state.payload.dashboard.recent_signals = Array.isArray(payload.recent_signals) ? payload.recent_signals : [];
  state.payload.signals = Array.isArray(payload.signals) ? payload.signals : [];
  if (payload.generated_at) state.payload.generated_at = payload.generated_at;
  persistPayloadCache();
  state.liveSignals.feedVersion = String(payload.feed_version || '');
  state.liveSignals.lastSyncedAt = payload.generated_at || null;
  state.lazy.signals.loaded = true;
}

function shouldPollLiveSignals() {
  return Boolean(state.token) && !document.hidden && ['home', 'signals'].includes(String(state.currentView || 'home'));
}

function getLiveSignalsPollIntervalMs() {
  return state.currentView === 'signals'
    ? LIVE_SIGNALS_VIEW_POLL_INTERVAL_MS
    : LIVE_SIGNALS_HOME_POLL_INTERVAL_MS;
}

function stopLiveSignalsPolling() {
  if (state.liveSignals.timer) {
    clearTimeout(state.liveSignals.timer);
    state.liveSignals.timer = null;
  }
}

function scheduleLiveSignalsTick(delay = getLiveSignalsPollIntervalMs()) {
  stopLiveSignalsPolling();
  if (!shouldPollLiveSignals()) return;
  state.liveSignals.timer = setTimeout(async () => {
    try {
      await refreshLiveSignalsState(false, 'interval');
    } catch (error) {
      console.warn('MiniApp live signals refresh failed', error);
    } finally {
      scheduleLiveSignalsTick(getLiveSignalsPollIntervalMs());
    }
  }, Math.max(1000, Number(delay || getLiveSignalsPollIntervalMs())));
}

async function refreshLiveSignalsState(force = false, reason = 'manual') {
  if (!state.token || state.liveSignals.requestInFlight || !shouldPollLiveSignals()) return null;
  state.liveSignals.requestInFlight = true;
  try {
    const currentVersion = String(state.liveSignals.feedVersion || '');
    const url = (!force && currentVersion)
      ? `/api/miniapp/live-signals?since_version=${encodeURIComponent(currentVersion)}`
      : '/api/miniapp/live-signals';
    const headers = {};
    if (state.token) headers.Authorization = `Bearer ${state.token}`;
    const response = await fetch(url, { headers });

    if (response.status === 204) {
      state.liveSignals.feedVersion = response.headers.get('X-Live-Signals-Version') || currentVersion;
      state.liveSignals.lastSyncedAt = response.headers.get('X-Live-Signals-Generated-At') || new Date().toISOString();
      return null;
    }

    const payload = await response.json().catch(() => ({}));
    if (!response.ok) throw new Error(payload.detail || payload.message || 'request_failed');

    const nextVersion = String(payload.feed_version || '');
    if (!force && nextVersion && nextVersion === currentVersion) {
      state.liveSignals.lastSyncedAt = payload.generated_at || state.liveSignals.lastSyncedAt || null;
      return payload;
    }
    applyLiveSignalsPayload(payload);
    renderHome();
    renderSignals();
    bindViewButtons();
    return payload;
  } finally {
    state.liveSignals.requestInFlight = false;
  }
}

function startLiveSignalsPolling() {
  scheduleLiveSignalsTick(getLiveSignalsPollIntervalMs());
}

function syncLiveSignalsPolling() {
  if (!shouldPollLiveSignals()) {
    stopLiveSignalsPolling();
    return;
  }
  scheduleLiveSignalsTick(getLiveSignalsPollIntervalMs());
}

function queueLiveSignalsRefresh(reason = 'focus') {
  if (!shouldPollLiveSignals()) {
    stopLiveSignalsPolling();
    return;
  }
  scheduleLiveSignalsTick(getLiveSignalsPollIntervalMs());
  Promise.resolve(refreshLiveSignalsState(true, reason)).catch(error => {
    console.warn(`MiniApp live signals ${reason} refresh failed`, error);
  });
}

function ensurePayloadShell() {
  if (!state.payload || typeof state.payload !== 'object') state.payload = {};
  if (!state.payload.me || typeof state.payload.me !== 'object') state.payload.me = {};
  if (!state.payload.dashboard || typeof state.payload.dashboard !== 'object') state.payload.dashboard = {};
  if (!Array.isArray(state.payload.dashboard.recent_signals)) state.payload.dashboard.recent_signals = [];
  if (!Array.isArray(state.payload.dashboard.recent_history)) state.payload.dashboard.recent_history = [];
  if (!Array.isArray(state.payload.signals)) state.payload.signals = [];
  if (!Array.isArray(state.payload.history)) state.payload.history = [];
  if (!state.payload.market || typeof state.payload.market !== 'object') state.payload.market = {};
  if (!Array.isArray(state.payload.watchlist)) state.payload.watchlist = [];
  if (!state.payload.watchlist_meta || typeof state.payload.watchlist_meta !== 'object') state.payload.watchlist_meta = {};
  if (!state.payload.plans || typeof state.payload.plans !== 'object') state.payload.plans = { plus: [], premium: [] };
  if (!Array.isArray(state.payload.plans.plus)) state.payload.plans.plus = [];
  if (!Array.isArray(state.payload.plans.premium)) state.payload.plans.premium = [];
  if (!state.payload.account || typeof state.payload.account !== 'object') state.payload.account = {};
  if (!state.payload.account.billing || typeof state.payload.account.billing !== 'object') state.payload.account.billing = {};
}

function markLazyStateFromBootstrap() {
  const isLightBootstrap = String(state.payload?.bootstrap_mode || '').toLowerCase() === 'light';
  const dashboard = state.payload?.dashboard || {};
  const market = state.payload?.market || {};
  const account = state.payload?.account || {};

  state.lazy.dashboard.loaded = !isLightBootstrap || Boolean(
    Number(dashboard.active_signals_count || 0)
    || (Array.isArray(dashboard.recent_signals) && dashboard.recent_signals.length)
    || (Array.isArray(dashboard.recent_history) && dashboard.recent_history.length)
  );
  state.lazy.dashboard.loading = false;
  state.lazy.signals.loaded = !isLightBootstrap || (Array.isArray(state.payload?.signals) && state.payload.signals.length > 0);
  state.lazy.signals.loading = false;
  state.lazy.history.loaded = !isLightBootstrap || (Array.isArray(state.payload?.history) && state.payload.history.length > 0);
  state.lazy.history.loading = false;
  state.lazy.market.loaded = !isLightBootstrap || Boolean(
    (Array.isArray(market.radar) && market.radar.length)
    || (Array.isArray(market.top_gainers) && market.top_gainers.length)
    || (Array.isArray(market.top_losers) && market.top_losers.length)
    || market.generated_at
  );
  state.lazy.market.loading = false;
  state.lazy.market.error = null;
  state.lazy.account.loaded = !isLightBootstrap || Boolean(
    Object.keys(account).filter(key => key !== 'billing').length
    || account?.billing?.active_order
    || (Array.isArray(account?.billing?.recent_orders) && account.billing.recent_orders.length)
  );
  state.lazy.account.loading = false;
}

async function refreshDashboardState(force = false) {
  if (state.lazy.dashboard.loading) return state.payload?.dashboard || null;
  if (!force && state.lazy.dashboard.loaded) return state.payload?.dashboard || null;
  state.lazy.dashboard.loading = true;
  try {
    const payload = await api('/api/miniapp/dashboard');
    ensurePayloadShell();
    state.payload.dashboard = payload && typeof payload === 'object' ? payload : {};
    state.lazy.dashboard.loaded = true;
    persistPayloadCache();
    setTopSummary();
    if (state.currentView === 'home') {
      renderHome();
      bindViewButtons();
    }
    return state.payload.dashboard;
  } finally {
    state.lazy.dashboard.loading = false;
  }
}

async function refreshSignalsState(force = false) {
  if (state.lazy.signals.loading) return state.payload?.signals || [];
  if (!force && state.lazy.signals.loaded && Array.isArray(state.payload?.signals) && state.payload.signals.length) return state.payload.signals;
  state.lazy.signals.loading = true;
  try {
    const payload = await api('/api/miniapp/signals');
    ensurePayloadShell();
    state.payload.signals = Array.isArray(payload?.items) ? payload.items : [];
    state.lazy.signals.loaded = true;
    persistPayloadCache();
    if (state.currentView === 'signals') {
      renderSignals();
      bindViewButtons();
    }
    return state.payload.signals;
  } finally {
    state.lazy.signals.loading = false;
  }
}

async function refreshHistoryState(force = false) {
  if (state.lazy.history.loading) return state.payload?.history || [];
  if (!force && state.lazy.history.loaded) return state.payload?.history || [];
  state.lazy.history.loading = true;
  try {
    const payload = await api('/api/miniapp/history');
    ensurePayloadShell();
    state.payload.history = Array.isArray(payload?.items) ? payload.items : [];
    state.lazy.history.loaded = true;
    persistPayloadCache();
    if (state.currentView === 'history') {
      renderHistory();
      bindViewButtons();
    }
    return state.payload.history;
  } finally {
    state.lazy.history.loading = false;
  }
}

async function refreshMarketState(force = false) {
  if (state.lazy.market.loading) return state.payload?.market || {};
  if (!force && state.lazy.market.loaded) return state.payload?.market || {};
  state.lazy.market.loading = true;
  state.lazy.market.error = null;
  if (state.currentView === 'market') {
    renderMarket();
    bindViewButtons();
  }
  try {
    const [marketResult, watchlistResult] = await Promise.allSettled([
      api('/api/miniapp/market'),
      api('/api/miniapp/watchlist'),
    ]);
    ensurePayloadShell();
    let updated = false;

    if (marketResult.status === 'fulfilled') {
      const market = marketResult.value;
      state.payload.market = market && typeof market === 'object' ? market : {};
      state.lazy.market.loaded = true;
      state.lazy.market.error = null;
      updated = true;
    } else {
      state.lazy.market.error = 'No pude actualizar la lectura de mercado ahora mismo.';
      if (state.payload?.market && Object.keys(state.payload.market).length) {
        state.lazy.market.loaded = true;
      }
    }

    if (watchlistResult.status === 'fulfilled') {
      const watchlist = watchlistResult.value;
      state.payload.watchlist = Array.isArray(watchlist?.items) ? watchlist.items : [];
      state.payload.watchlist_meta = watchlist?.meta && typeof watchlist.meta === 'object' ? watchlist.meta : {};
      updated = true;
    }

    if (updated) persistPayloadCache();
    if (state.currentView === 'market') {
      renderMarket();
      bindViewButtons();
    }
    if (marketResult.status === 'rejected' && watchlistResult.status === 'rejected') {
      throw marketResult.reason || watchlistResult.reason || new Error('market_unavailable');
    }
    return state.payload.market;
  } finally {
    state.lazy.market.loading = false;
    if (state.currentView === 'market') {
      renderMarket();
      bindViewButtons();
    }
  }
}

function applyPaymentOrderPreview(order) {
  ensurePayloadShell();
  const billing = state.payload.account.billing;
  const summary = { ...(billing.summary || {}) };
  const recentOrders = Array.isArray(billing.recent_orders) ? [...billing.recent_orders] : [];
  const openStatuses = new Set(['awaiting_payment', 'verification_in_progress', 'paid_unconfirmed']);

  if (!order) {
    billing.active_order = null;
    summary.open = 0;
    billing.summary = summary;
    return;
  }

  billing.active_order = order;
  const filtered = recentOrders.filter(item => String(item?.order_id || '') !== String(order.order_id || ''));
  billing.recent_orders = [order, ...filtered].slice(0, 6);
  summary.total = Math.max(Number(summary.total || 0), billing.recent_orders.length);
  summary.open = openStatuses.has(String(order.status || '').toLowerCase()) ? Math.max(Number(summary.open || 0), 1) : 0;
  billing.summary = summary;
}

async function refreshAccountState(force = false) {
  if (state.lazy.account.loading) return state.payload?.account || null;
  if (!force && state.lazy.account.loaded && state.payload?.account && Object.keys(state.payload.account).length) return state.payload.account;
  state.lazy.account.loading = true;
  try {
    const [account, me] = await Promise.all([
      api('/api/miniapp/account'),
      api('/api/miniapp/me'),
    ]);
    ensurePayloadShell();
    if (me && typeof me === 'object') state.payload.me = me;
    if (account && typeof account === 'object') state.payload.account = account;
    state.lazy.account.loaded = true;
    setTopSummary();
    if (state.currentView === 'account') {
      renderAccount();
      bindViewButtons();
    }
    return state.payload.account;
  } finally {
    state.lazy.account.loading = false;
  }
}

function focusPaymentCard() {
  const schedule = typeof window !== 'undefined' && typeof window.requestAnimationFrame === 'function'
    ? window.requestAnimationFrame.bind(window)
    : (callback) => setTimeout(callback, 0);
  schedule(() => {
    const target = document.querySelector('[data-payment-active-card]') || document.querySelector('.payment-card');
    if (!target || typeof target.scrollIntoView !== 'function') return;
    target.scrollIntoView({ behavior: 'smooth', block: 'start' });
  });
}

function focusPlanBlock() {
  const schedule = typeof window !== 'undefined' && typeof window.requestAnimationFrame === 'function'
    ? window.requestAnimationFrame.bind(window)
    : (callback) => setTimeout(callback, 0);
  schedule(() => {
    const target = document.querySelector('[data-plan-block]');
    if (!target || typeof target.scrollIntoView !== 'function') return;
    target.scrollIntoView({ behavior: 'smooth', block: 'start' });
  });
}

function setAccountNotice(message, tone = 'warning') {
  const normalized = String(message || '').trim();
  state.accountNotice = normalized ? { message: normalized, tone: String(tone || 'warning') } : null;
}

function accountNoticeCard(notice) {
  if (!notice?.message) return '';
  const toneClass = billingToneClass(notice.tone);
  return `
    <div class="card payment-focus-panel card-span-12 ${toneClass}" data-account-notice="true">
      <div class="payment-focus-card ${toneClass}">
        <div class="payment-focus-copy">
          <div class="payment-focus-kicker">Estado</div>
          <div class="payment-focus-title">Actividad de billing</div>
          <div class="payment-focus-message">${escapeHtml(notice.message)}</div>
        </div>
      </div>
    </div>
  `;
}

function isPlanExpired() {
  const me = state.payload?.me || state.authMe || {};
  const status = String(me.subscription_status || '').toLowerCase();
  // Bloquear tanto planes vencidos como usuarios sin suscripción activa (free)
  return status === 'expired' || status === 'free';
}

function setTopSummary() {
  const me = state.payload?.me || {};
  els.planBadge.textContent = String(me.plan_name || 'FREE').toUpperCase();
  els.daysBadge.textContent = `${Number(me.days_left || 0)} días`;
  // Mostrar candado visual en la pestaña Mercado si el plan está vencido
  const marketNav = document.querySelector('.nav-item[data-view="market"]');
  if (marketNav) marketNav.classList.toggle('nav-item-locked', isPlanExpired());
}

function setSettingsNotice(message, tone = 'warning') {
  const normalized = String(message || '').trim();
  state.settingsCenter.notice = normalized ? { message: normalized, tone: String(tone || 'warning') } : null;
}

function settingsNoticeCard(notice) {
  if (!notice?.message) return '';
  const toneClass = billingToneClass(notice.tone);
  return `
    <div class="card payment-focus-panel card-span-12 ${toneClass}">
      <div class="payment-focus-card ${toneClass}">
        <div class="payment-focus-copy">
          <div class="payment-focus-kicker">Ajustes</div>
          <div class="payment-focus-title">Estado de preferencias</div>
          <div class="payment-focus-message">${escapeHtml(notice.message)}</div>
        </div>
      </div>
    </div>
  `;
}

async function refreshSettingsCenter(force = false) {
  if (state.settingsCenter.loading) return state.settingsCenter.payload;
  if (!force && state.settingsCenter.payload) return state.settingsCenter.payload;
  state.settingsCenter.loading = true;
  renderSettings();
  bindViewButtons();
  try {
    const payload = await api('/api/miniapp/settings');
    state.settingsCenter.payload = payload;
    return payload;
  } catch (error) {
    setSettingsNotice(`No se pudieron cargar los ajustes: ${error.message || 'error'}`, 'warning');
    throw error;
  } finally {
    state.settingsCenter.loading = false;
    renderSettings();
    bindViewButtons();
  }
}

async function openSettingsCenter(force = false) {
  closeSignalDetailModal();
  setView('settings');
  renderSettings();
  bindViewButtons();
  try {
    await refreshSettingsCenter(force);
  } catch (_) {}
}

function collectSettingsPatch() {
  const language = String(document.getElementById('settingsLanguageSelect')?.value || '').trim() || null;
  const push_alerts_enabled = Boolean(document.getElementById('settingsPushEnabled')?.checked);
  const push_tiers = {
    free: Boolean(document.getElementById('settingsPushTierFree')?.checked),
    plus: Boolean(document.getElementById('settingsPushTierPlus')?.checked),
    premium: Boolean(document.getElementById('settingsPushTierPremium')?.checked),
  };
  return { language, push_alerts_enabled, push_tiers };
}

function settingsTierCard(item) {
  const disabled = !item.available ? 'disabled' : '';
  return `
    <label class="card card-span-4" style="padding:12px;">
      <div class="item-header">
        <div>
          <div class="item-title">${escapeHtml(item.label)}</div>
          <div class="item-subtitle">${escapeHtml(item.available ? 'Disponible para tu plan actual' : (item.locked_reason || 'No disponible'))}</div>
        </div>
        <input type="checkbox" id="settingsPushTier${escapeHtml(item.label)}" ${item.selected ? 'checked' : ''} ${disabled}>
      </div>
    </label>
  `;
}

function renderSettings() {
  if (!els.settings) return;
  const payload = state.settingsCenter.payload || {};
  const overview = payload.overview || state.payload?.me || {};
  const language = payload.language || { current: overview.language || 'es', options: [{ value: 'es', label: 'Español' }, { value: 'en', label: 'English' }] };
  const pushAlerts = payload.push_alerts || { enabled: true, tiers: [], summary: 'Configura qué niveles quieres recibir como push.' };
  const tierByKey = Object.fromEntries((pushAlerts.tiers || []).map(item => [String(item.key || '').toLowerCase(), item]));
  const normalizedTiers = ['free', 'plus', 'premium'].map(key => tierByKey[key] || { key, label: key.toUpperCase(), available: false, selected: false, locked_reason: 'No disponible' });
  const loadingBanner = state.settingsCenter.loading ? '<div class="card card-span-12"><div class="loading-inline">Cargando ajustes...</div></div>' : '';

  if (!state.settingsCenter.payload && !state.settingsCenter.loading) {
    els.settings.innerHTML = `
      <div class="section-grid">
        ${settingsNoticeCard(state.settingsCenter.notice)}
        <div class="card card-span-12">
          <h2>Centro de ajustes</h2>
          <p>Configura idioma y preferencias de alertas push sin tocar el resto de la cuenta.</p>
          <div class="action-row"><button class="button button-primary" data-open-settings-center="true">Abrir ajustes</button></div>
        </div>
      </div>
    `;
    return;
  }

  els.settings.innerHTML = `
    <div class="section-grid">
      ${settingsNoticeCard(state.settingsCenter.notice)}
      ${loadingBanner}
      <div class="card card-span-12">
        <div class="item-header">
          <div>
            <h2 style="margin:0;">Centro de ajustes</h2>
            <div class="item-subtitle">Preferencias de idioma y alertas push del ecosistema HADES.</div>
          </div>
          <div class="action-row compact">
            <button class="button button-secondary" data-goto="account">Volver a cuenta</button>
            <button class="button button-secondary" data-settings-refresh="true">Refrescar</button>
          </div>
        </div>
        <div class="pill-row compact-pill-row" style="margin-top:12px;">
          <span class="pill">Plan actual: ${escapeHtml(overview.plan_name || 'FREE')}</span>
          <span class="pill">Idioma: ${escapeHtml(language.current || 'es')}</span>
          <span class="pill">Push: ${pushAlerts.enabled ? 'Activo' : 'Silenciado'}</span>
        </div>
      </div>

      <div class="card card-span-6">
        <h2>Idioma</h2>
        <p>Este ajuste prepara la experiencia multilenguaje de la MiniApp y del bot.</p>
        <label style="display:flex; flex-direction:column; gap:8px;">
          <span class="metric-label">Idioma preferido</span>
          <select id="settingsLanguageSelect" class="input">
            ${(language.options || []).map(option => `<option value="${escapeHtml(option.value)}" ${option.value === language.current ? 'selected' : ''}>${escapeHtml(option.label)}</option>`).join('')}
          </select>
        </label>
      </div>

      <div class="card card-span-6">
        <h2>Push de señales</h2>
        <p>${escapeHtml(pushAlerts.summary || 'Configura qué niveles quieres recibir como push.')}</p>
        <label class="feature-item" style="margin-top:12px; display:flex; align-items:center; justify-content:space-between; gap:12px;">
          <span>Activar avisos push</span>
          <input type="checkbox" id="settingsPushEnabled" ${pushAlerts.enabled ? 'checked' : ''}>
        </label>
        <div class="detail-note" style="margin-top:12px;">Los pushes siguen siendo avisos simples en Telegram. El detalle completo vive dentro de la MiniApp.</div>
      </div>

      <div class="card card-span-12">
        <h2>Niveles de señal que quieres recibir</h2>
        <div class="section-grid" style="margin-top:12px;">
          ${normalizedTiers.map(settingsTierCard).join('')}
        </div>
      </div>

      <div class="card card-span-12">
        <h2>Resumen operativo</h2>
        <div class="feature-list">
          <div class="feature-item">• Free solo puede recibir pushes Free.</div>
          <div class="feature-item">• Plus puede elegir Free + Plus.</div>
          <div class="feature-item">• Premium puede elegir Free + Plus + Premium.</div>
          <div class="feature-item">• Silenciar push no afecta el acceso a las señales dentro de la MiniApp.</div>
        </div>
        <div class="action-row" style="margin-top:12px;">
          <button class="button button-primary" data-settings-save="true">Guardar ajustes</button>
        </div>
      </div>
    </div>
  `;
}

function setAdminNotice(message, tone = 'warning') {
  const normalized = String(message || '').trim();
  state.adminPanel.notice = normalized ? { message: normalized, tone: String(tone || 'warning') } : null;
}

function adminNoticeCard(notice) {
  if (!notice?.message) return '';
  const toneClass = billingToneClass(notice.tone);
  return `
    <div class="card payment-focus-panel card-span-12 ${toneClass}">
      <div class="payment-focus-card ${toneClass}">
        <div class="payment-focus-copy">
          <div class="payment-focus-kicker">Admin</div>
          <div class="payment-focus-title">Estado operativo</div>
          <div class="payment-focus-message">${escapeHtml(notice.message)}</div>
        </div>
      </div>
    </div>
  `;
}

function adminOverviewMetricCard(label, value, subtitle = '', toneClass = '') {
  return metricCard(label, value, subtitle, '', toneClass);
}

function adminSummaryLine(label, value) {
  return `
    <div class="feature-item">
      <span>${escapeHtml(label)}</span>
      <strong>${escapeHtml(String(value ?? '—'))}</strong>
    </div>
  `;
}

function adminResetSummaryCard(summary) {
  if (!summary) return '';
  return `
    <div class="card card-span-12">
      <h2>Último reset ejecutado</h2>
      <div class="feature-list">
        ${adminSummaryLine('Modo', summary.mode || 'full_reset')}
        ${adminSummaryLine('Señales base borradas', summary.deleted_base_signals ?? 0)}
        ${adminSummaryLine('Señales usuario borradas', summary.deleted_user_signals ?? 0)}
        ${adminSummaryLine('Resultados borrados', summary.deleted_results ?? 0)}
        ${adminSummaryLine('Histórico borrado', summary.deleted_history ?? 0)}
        ${adminSummaryLine('Snapshots borrados', summary.deleted_snapshots ?? 0)}
      </div>
    </div>
  `;
}


function adminManualActivationButton(option, selectedPlan, disabled = false) {
  const optionKey = String(option?.key || '').toLowerCase();
  const isSelected = optionKey === String(selectedPlan || '').toLowerCase();
  const isAvailable = Boolean(option?.available);
  const buttonClass = isSelected && isAvailable ? 'button button-primary' : 'button button-secondary';
  return `<button class="${buttonClass}" data-admin-plan-select="${escapeHtml(optionKey)}" ${disabled || !isAvailable ? 'disabled' : ''}>${escapeHtml(option?.label || optionKey.toUpperCase())}</button>`;
}

function adminManualTargetSummaryCard(target) {
  if (!target) return '';
  return `
    <div class="card" style="margin-top:12px;">
      <div class="item-header">
        <div>
          <div class="item-title">${escapeHtml(target.username || 'Sin username')} · ID ${escapeHtml(target.user_id)}</div>
          <div class="item-subtitle">${escapeHtml(target.subscription_status_label || '—')} · ${escapeHtml(target.plan_name || 'FREE')} · ${escapeHtml(target.days_left ?? 0)} días restantes</div>
        </div>
        <span class="plan-tag">${escapeHtml(target.plan_name || 'FREE')}</span>
      </div>
      <div class="pill-row compact-pill-row">
        <span class="pill">Estado: ${escapeHtml(target.subscription_status_label || '—')}</span>
        <span class="pill">Idioma: ${escapeHtml(String(target.language || 'es').toUpperCase())}</span>
        <span class="pill">Vence: ${escapeHtml(formatDate(target.expires_at || target.trial_end || target.plan_end))}</span>
        <span class="pill">Baneado: ${target.banned ? 'Sí' : 'No'}</span>
      </div>
    </div>
  `;
}

function adminModerationActionButton(label, action, enabled, isBusy = false, variant = 'secondary') {
  const klass = variant === 'danger' ? 'button button-danger' : variant === 'warning' ? 'button button-secondary' : 'button button-secondary';
  return `<button class="${klass}" data-admin-moderation-action="${escapeHtml(action)}" ${enabled && !isBusy ? '' : 'disabled'}>${escapeHtml(label)}</button>`;
}

function adminModerationSummaryCard(target, moderationState) {
  if (!target) return '';
  const stateLabel = target.ban_active ? (target.ban_label || 'Baneo activo') : 'Sin baneo';
  const untilLabel = target.ban_until ? formatDate(target.ban_until) : '—';
  const confirm = moderationState?.confirmAction || null;
  const durationValue = String(moderationState?.draft?.durationValue || '7');
  const durationUnit = String(moderationState?.draft?.durationUnit || 'days');
  const tempUnits = [
    { value: 'hours', label: 'Horas' },
    { value: 'days', label: 'Días' },
    { value: 'weeks', label: 'Semanas' },
  ];
  const moderation = target.moderation || {};
  const confirmBlock = confirm ? `
    <div class="notice-list" style="margin-top:12px;">
      <div class="notice-item">${escapeHtml(confirm.message || 'Confirma la acción administrativa antes de ejecutarla.')}</div>
    </div>
    <div class="action-row" style="margin-top:12px;">
      <button class="button button-danger" data-admin-moderation-confirm="true" ${moderationState?.actionLoading ? 'disabled' : ''}>${moderationState?.actionLoading ? 'Procesando...' : 'Confirmar acción'}</button>
      <button class="button button-secondary" data-admin-moderation-cancel="true" ${moderationState?.actionLoading ? 'disabled' : ''}>Cancelar</button>
    </div>
  ` : '';
  return `
    <div class="card card-span-12">
      <h2>Moderación de usuario</h2>
      <div class="pill-row compact-pill-row">
        <span class="pill">Estado baneo: ${escapeHtml(stateLabel)}</span>
        <span class="pill">Modo: ${escapeHtml(target.ban_mode || '—')}</span>
        <span class="pill">Hasta: ${escapeHtml(untilLabel)}</span>
      </div>
      <div class="action-row compact" style="margin-top:12px; align-items:flex-end; flex-wrap:wrap;">
        <label style="display:flex; flex-direction:column; gap:6px; min-width:150px;">
          <span>Duración temporal</span>
          <input id="adminModerationDurationValueInput" class="input" type="number" min="1" step="1" value="${escapeHtml(durationValue)}" placeholder="Ej: 7">
        </label>
        <label style="display:flex; flex-direction:column; gap:6px; min-width:150px;">
          <span>Unidad</span>
          <select id="adminModerationDurationUnitSelect" class="input">${tempUnits.map(item => `<option value="${escapeHtml(item.value)}" ${item.value === durationUnit ? 'selected' : ''}>${escapeHtml(item.label)}</option>`).join('')}</select>
        </label>
        ${adminModerationActionButton('Ban temporal', 'ban_temporary', moderation.can_temporary_ban, moderationState?.actionLoading, 'warning')}
        ${adminModerationActionButton('Ban permanente', 'ban_permanent', moderation.can_permanent_ban, moderationState?.actionLoading, 'danger')}
        ${adminModerationActionButton('Levantar baneo', 'unban', moderation.can_unban, moderationState?.actionLoading, 'secondary')}
        ${adminModerationActionButton('Eliminar usuario', 'delete', moderation.can_delete, moderationState?.actionLoading, 'danger')}
      </div>
      <div class="notice-list" style="margin-top:12px;">
        <div class="notice-item">El baneo temporal bloquea Bot + MiniApp hasta que expire o el admin lo levante.</div>
        <div class="notice-item">El baneo permanente bloquea indefinidamente el acceso.</div>
        <div class="notice-item">Eliminar usuario borra su documento principal y sus datos operativos ligados al user_id.</div>
      </div>
      ${confirmBlock}
    </div>
  `;
}

function adminRuntimeStatusClass(value) {
  const normalized = String(value || '').toLowerCase();
  if (normalized === 'ok') return 'is-positive';
  if (normalized === 'degraded' || normalized === 'stale' || normalized === 'warning') return 'is-warning';
  if (normalized === 'error' || normalized === 'stopped' || normalized === 'missing') return 'metric-negative';
  return '';
}

function adminRuntimeRoleCard(role, report = {}) {
  const statusValue = String(report.overall_status || 'unknown');
  const components = Object.values(report.components || {});
  const incidentCount = components.filter(item => {
    const effective = String(item?.effective_status || item?.status || 'unknown').toLowerCase();
    return effective && effective !== 'ok';
  }).length;
  const subtitleBits = [];
  if (report.updated_at) subtitleBits.push(`Actualizado ${formatDate(report.updated_at)}`);
  if (Number.isFinite(Number(report.max_age_seconds))) subtitleBits.push(`Edad max ${formatNumber(report.max_age_seconds, 0)}s`);
  return `
    <div class="card card-span-3 ${adminRuntimeStatusClass(statusValue)}">
      <div class="metric-label">${escapeHtml(String(role || '').toUpperCase())}</div>
      <div class="metric-value">${escapeHtml(formatStatusLabel(statusValue))}</div>
      <div class="metric-subtitle">${escapeHtml(subtitleBits.join(' · ') || 'Sin telemetría adicional')}</div>
      <div class="pill-row compact-pill-row" style="margin-top:12px;">
        <span class="pill">Componentes ${escapeHtml(components.length)}</span>
        <span class="pill">Incidencias ${escapeHtml(incidentCount)}</span>
      </div>
    </div>
  `;
}

function adminStrategyPipelineCard(item = {}) {
  return `
    <div class="card card-span-6">
      <div class="item-header">
        <div>
          <h2 style="margin:0;">${escapeHtml(item.strategy_label || '—')}</h2>
          <div class="item-subtitle">Intentos ${escapeHtml(item.attempted_symbols ?? 0)} · Pool ${escapeHtml(item.candidate_pool ?? 0)} · Publicadas ${escapeHtml(item.selected_signals ?? 0)}</div>
        </div>
        <span class="plan-tag">ADMIN</span>
      </div>
      <div class="account-metric-grid">
        ${accountMetricCard('Intentos', item.attempted_symbols ?? 0)}
        ${accountMetricCard('Pool candidato', item.candidate_pool ?? 0)}
        ${accountMetricCard('Publicadas', item.selected_signals ?? 0)}
        ${accountMetricCard('Rechazadas', item.rejected_symbols ?? 0)}
      </div>
      <div class="pill-row compact-pill-row" style="margin-top:12px;">
        <span class="pill">Candidate rate ${escapeHtml(formatNumber(item.candidate_rate || 0))}%</span>
        <span class="pill">Publish rate ${escapeHtml(formatNumber(item.publish_rate || 0))}%</span>
        <span class="pill">Selección desde pool ${escapeHtml(formatNumber(item.selection_from_candidates_rate || 0))}%</span>
      </div>
    </div>
  `;
}

function adminStrategyRejectCard(item = {}) {
  const reasons = Array.isArray(item.top_reasons) ? item.top_reasons : [];
  return `
    <div class="item compact-item">
      <div class="item-header">
        <div>
          <div class="item-title">${escapeHtml(item.strategy_label || '—')}</div>
          <div class="item-subtitle">Rechazos terminales ${escapeHtml(item.rejected_symbols ?? 0)}</div>
        </div>
        <span class="plan-tag">TOP 5</span>
      </div>
      <div class="pill-row compact-pill-row" style="margin-top:8px;">
        ${reasons.length ? reasons.map(reason => `<span class="pill">${escapeHtml(reason.reason_label || 'UNKNOWN')} · ${escapeHtml(reason.count ?? 0)}</span>`).join('') : '<span class="pill">Sin razones registradas todavía</span>'}
      </div>
    </div>
  `;
}

function adminRegimeDistributionItem(item = {}) {
  return `
    <div class="item compact-item">
      <div class="item-header">
        <div>
          <div class="item-title">${escapeHtml(item.regime_label || '—')}</div>
          <div class="item-subtitle">Ciclos ${escapeHtml(item.cycles ?? 0)} · Intentos ${escapeHtml(item.attempted_symbols ?? 0)}</div>
        </div>
        <span class="plan-tag">${escapeHtml(item.selected_signals ?? 0)} sel</span>
      </div>
      <div class="inline-meta">
        <span>Pool ${escapeHtml(item.candidate_pool ?? 0)}</span>
        <span>Publicadas ${escapeHtml(item.selected_signals ?? 0)}</span>
      </div>
    </div>
  `;
}

function adminRegimeStrategyItem(item = {}) {
  return `
    <div class="item compact-item">
      <div class="item-header">
        <div>
          <div class="item-title">${escapeHtml(item.regime_label || '—')} → ${escapeHtml(item.strategy_label || '—')}</div>
          <div class="item-subtitle">Pool ${escapeHtml(item.candidate_pool ?? 0)} · Publicadas ${escapeHtml(item.selected_signals ?? 0)}</div>
        </div>
        <span class="plan-tag">${escapeHtml(formatNumber(item.publish_rate || 0))}%</span>
      </div>
    </div>
  `;
}

function adminLatestCycleCard(latestCycle = {}) {
  if (!latestCycle?.available) {
    return `
      <div class="card card-span-12">
        <h2>Shadow actual del scanner</h2>
        <div class="empty-state">Todavía no hay telemetría viva del scanner. En cuanto corra el scanner, este bloque mostrará el último ciclo.</div>
      </div>
    `;
  }
  const attempts = Array.isArray(latestCycle.attempts_by_strategy) ? latestCycle.attempts_by_strategy : [];
  const pool = Array.isArray(latestCycle.candidate_pool_by_strategy) ? latestCycle.candidate_pool_by_strategy : [];
  const selected = Array.isArray(latestCycle.selected_by_strategy) ? latestCycle.selected_by_strategy : [];
  const rejected = Array.isArray(latestCycle.rejected_by_strategy) ? latestCycle.rejected_by_strategy : [];
  const reasons = Array.isArray(latestCycle.top_reject_reasons) ? latestCycle.top_reject_reasons : [];
  const chips = [];
  attempts.forEach(item => chips.push(`Intentos ${item.strategy_label}: ${item.count}`));
  pool.forEach(item => chips.push(`Pool ${item.strategy_label}: ${item.count}`));
  selected.forEach(item => chips.push(`Publicadas ${item.strategy_label}: ${item.count}`));
  rejected.forEach(item => chips.push(`Rechazos ${item.strategy_label}: ${item.count}`));
  return `
    <div class="card card-span-12">
      <div class="item-header">
        <div>
          <h2 style="margin:0;">Shadow actual del scanner</h2>
          <div class="item-subtitle">Último ciclo vivo del scanner para validar de inmediato qué régimen está dominando y cómo está quedando el embudo.</div>
        </div>
        <span class="plan-tag">${escapeHtml(formatStatusLabel(latestCycle.status || 'unknown'))}</span>
      </div>
      <div class="pill-row compact-pill-row" style="margin-top:12px;">
        <span class="pill">Ciclo ${escapeHtml(latestCycle.cycle_number ?? 0)}</span>
        <span class="pill">Régimen ${escapeHtml(latestCycle.market_regime_label || '—')}</span>
        <span class="pill">Bias ${escapeHtml(String(latestCycle.market_regime_bias || 'neutral').toUpperCase())}</span>
        <span class="pill">Estrategia ${escapeHtml(latestCycle.market_strategy_label || '—')}</span>
        <span class="pill">Actualizado ${escapeHtml(formatDate(latestCycle.generated_at))}</span>
      </div>
      <div class="account-metric-grid" style="margin-top:12px;">
        ${accountMetricCard('Intentos', latestCycle.attempted_symbols_total ?? 0)}
        ${accountMetricCard('Pool', latestCycle.candidate_pool_total ?? 0)}
        ${accountMetricCard('Publicadas', latestCycle.selected_signals_total ?? 0)}
        ${accountMetricCard('Rechazos', latestCycle.rejected_symbols_total ?? 0)}
      </div>
      <div class="pill-row compact-pill-row" style="margin-top:12px;">
        ${chips.length ? chips.map(chip => `<span class="pill">${escapeHtml(chip)}</span>`).join('') : '<span class="pill">Sin conteos por estrategia todavía</span>'}
      </div>
      <div class="pill-row compact-pill-row" style="margin-top:8px;">
        <span class="pill">Risk off ${escapeHtml(latestCycle.risk_off_symbols_total ?? 0)}</span>
        <span class="pill">Fallos scanner ${escapeHtml(latestCycle.failure_symbols_total ?? 0)}</span>
        <span class="pill">Motivo régimen ${escapeHtml(latestCycle.market_regime_reason || '—')}</span>
      </div>
      <div class="pill-row compact-pill-row" style="margin-top:8px;">
        ${reasons.length ? reasons.map(reason => `<span class="pill">${escapeHtml(reason.reason_label || 'UNKNOWN')} · ${escapeHtml(reason.count ?? 0)}</span>`).join('') : '<span class="pill">Sin rechazos terminales en el último ciclo</span>'}
      </div>
    </div>
  `;
}

function setRiskNotice(message, tone = 'warning') {
  const normalized = String(message || '').trim();
  state.riskCenter.notice = normalized ? { message: normalized, tone: String(tone || 'warning') } : null;
}

function riskNoticeCard(notice) {
  if (!notice?.message) return '';
  const toneClass = billingToneClass(notice.tone);
  return `
    <div class="card payment-focus-panel card-span-12 ${toneClass}">
      <div class="payment-focus-card ${toneClass}">
        <div class="payment-focus-copy">
          <div class="payment-focus-kicker">Riesgo</div>
          <div class="payment-focus-title">Estado del centro de riesgo</div>
          <div class="payment-focus-message">${escapeHtml(notice.message)}</div>
        </div>
      </div>
    </div>
  `;
}

function riskMetricCard(label, value, subtitle = '', toneClass = '') {
  return metricCard(label, value, subtitle, '', toneClass);
}

function riskBandLabel(value) {
  const normalized = String(value || '').toLowerCase();
  const map = { normal: 'Normal', medio: 'Medio', alto: 'Alto' };
  return map[normalized] || String(value || '—');
}

function riskBandToneClass(value) {
  const normalized = String(value || '').toLowerCase();
  if (normalized === 'alto') return 'is-warning';
  if (normalized === 'medio') return 'is-accent';
  return 'is-positive';
}

function riskQueryString(query = {}) {
  const params = new URLSearchParams();
  if (query.signalId) params.set('signal_id', query.signalId);
  if (query.profile) params.set('profile', query.profile);
  if (query.leverage !== null && query.leverage !== undefined && String(query.leverage).trim() !== '') {
    params.set('leverage', String(query.leverage).trim());
  }
  const raw = params.toString();
  return raw ? `?${raw}` : '';
}

function normalizeRiskQuery(options = {}) {
  return {
    signalId: options.signalId ? String(options.signalId).trim() : null,
    profile: options.profile ? String(options.profile).trim().toLowerCase() : null,
    leverage: options.leverage !== null && options.leverage !== undefined && String(options.leverage).trim() !== ''
      ? String(options.leverage).trim()
      : null,
  };
}

async function refreshRiskCenter(force = false, options = {}) {
  const nextQuery = {
    ...state.riskCenter.query,
    ...normalizeRiskQuery(options),
  };
  if (options.signalId === null) nextQuery.signalId = null;
  if (options.profile === null) nextQuery.profile = null;
  if (options.leverage === null) nextQuery.leverage = null;

  const sameQuery = JSON.stringify(nextQuery) === JSON.stringify(state.riskCenter.query || {});
  if (state.riskCenter.loading) return state.riskCenter.payload;
  if (!force && state.riskCenter.payload && sameQuery) return state.riskCenter.payload;

  state.riskCenter.loading = true;
  state.riskCenter.query = nextQuery;
  renderRisk();
  bindViewButtons();
  try {
    const payload = await api(`/api/miniapp/risk${riskQueryString(nextQuery)}`);
    state.riskCenter.payload = payload;
    return payload;
  } catch (error) {
    setRiskNotice(`No se pudo cargar gestión de riesgo: ${error.message || 'error'}`, 'warning');
    throw error;
  } finally {
    state.riskCenter.loading = false;
    renderRisk();
    bindViewButtons();
  }
}

async function openRiskCenter(options = {}) {
  closeSignalDetailModal();
  setView('risk');
  renderRisk();
  bindViewButtons();
  try {
    await refreshRiskCenter(true, options);
  } catch (_) {}
}

function collectRiskProfilePatch() {
  const readNumber = (id) => {
    const raw = String(document.getElementById(id)?.value || '').trim();
    return raw === '' ? null : Number(raw);
  };
  return {
    capital_usdt: readNumber('riskCapitalInput'),
    risk_percent: readNumber('riskPercentInput'),
    exchange: String(document.getElementById('riskExchangeSelect')?.value || '').trim() || null,
    entry_mode: String(document.getElementById('riskEntryModeSelect')?.value || '').trim() || null,
    fee_percent_per_side: readNumber('riskFeeInput'),
    slippage_percent: readNumber('riskSlippageInput'),
    default_leverage: readNumber('riskLeverageInput'),
    default_profile: String(document.getElementById('riskDefaultProfileSelect')?.value || '').trim() || null,
  };
}

function applyRiskPresetToInputs() {
  const payload = state.riskCenter.payload || {};
  const presets = payload.catalog?.presets || {};
  const exchange = String(document.getElementById('riskExchangeSelect')?.value || '').trim();
  const entryMode = String(document.getElementById('riskEntryModeSelect')?.value || '').trim();
  const preset = presets?.[exchange]?.[entryMode];
  if (!preset) return false;
  const feeInput = document.getElementById('riskFeeInput');
  const slippageInput = document.getElementById('riskSlippageInput');
  if (feeInput) feeInput.value = String(preset.fee_percent_per_side ?? '');
  if (slippageInput) slippageInput.value = String(preset.slippage_percent ?? '');
  return true;
}

function riskCandidateCard(item, selectedSignalId) {
  const isSelected = String(item?.signal_id || '') === String(selectedSignalId || '');
  const statusValue = item?.result ? resultLabel(item) : formatStatusLabel(item?.status || 'active');
  return `
    <div class="item compact-item ${isSelected ? 'card is-accent' : ''}">
      <div class="item-header">
        <div>
          <div class="item-title">${escapeHtml(item.symbol)} <span class="${dirClass(item.direction)}">${escapeHtml(item.direction)}</span></div>
          <div class="item-subtitle">${escapeHtml(item.setup_group || 'setup')} · Score ${escapeHtml(formatNumber(item.score || 0, 1))}</div>
        </div>
        <span class="plan-tag">${escapeHtml(statusValue)}</span>
      </div>
      <div class="inline-meta">
        <span>${item.source === 'history' ? 'Historial' : 'En vivo'}</span>
        <span>Tier ${escapeHtml(String(item.visibility_name || item.visibility || '—').toUpperCase())}</span>
        <span>Emitida: ${escapeHtml(formatDate(item.created_at))}</span>
      </div>
      <div class="action-row compact">
        <button class="button button-secondary" data-open-risk-signal="${escapeHtml(item.signal_id)}">${isSelected ? 'Recalcular' : 'Calcular riesgo'}</button>
      </div>
    </div>
  `;
}

function riskPreviewCard(preview, payload) {
  if (!preview) return '';
  const diagnostics = preview.diagnostics || {};
  const tpResults = Array.isArray(preview.tp_results) ? preview.tp_results : [];
  const profileOptions = payload.overview?.profile_options || ['moderado'];
  const selectedProfile = payload.signals?.selected_profile || preview.profile_name || 'moderado';
  const leverageValue = state.riskCenter.query?.leverage || '';
  return `
    <div class="card card-span-12">
      <div class="item-header">
        <div>
          <h2 style="margin:0;">Calculadora de riesgo</h2>
          <div class="item-subtitle">${escapeHtml(preview.symbol)} · ${escapeHtml(preview.profile_label || preview.profile_name || 'Moderado')} · ${escapeHtml(preview.entry_mode_label || 'Límite')}</div>
        </div>
        <span class="plan-tag ${riskBandToneClass(diagnostics.risk_band)}">Banda ${escapeHtml(riskBandLabel(diagnostics.risk_band))}</span>
      </div>
      <div class="pill-row compact-pill-row">
        <span class="pill">Operable: ${preview.is_operable ? 'Sí' : 'No'}</span>
        <span class="pill">Señal activa: ${preview.signal_active_for_entry ? 'Sí' : 'No'}</span>
        <span class="pill">Exchange: ${escapeHtml(preview.exchange_label || preview.exchange || '—')}</span>
        <span class="pill">Leverage usado: ${escapeHtml(formatNumber(preview.leverage || 0, 0))}x</span>
      </div>
      <div class="action-row compact" style="margin-top:12px;">
        ${profileOptions.map(option => `<button class="button ${option === selectedProfile ? 'button-primary' : 'button-secondary'}" data-risk-select-profile="${escapeHtml(option)}">${escapeHtml(profileLabel(option))}</button>`).join('')}
      </div>
      <div class="action-row compact" style="margin-top:12px; align-items:flex-end;">
        <label style="display:flex; flex-direction:column; gap:6px; min-width:160px;">
          <span>Override leverage</span>
          <input id="riskPreviewLeverageInput" class="input" type="number" min="1" step="0.1" value="${escapeHtml(leverageValue)}" placeholder="Usar default">
        </label>
        <button class="button button-secondary" data-risk-preview-run="true">Recalcular</button>
        <button class="button button-secondary" data-risk-clear-selection="true">Limpiar selección</button>
      </div>
      <div class="section-grid" style="margin-top:12px;">
        ${riskMetricCard('Riesgo USDT', formatMoney(preview.risk_amount_usdt), 'Pérdida máxima prevista')}
        ${riskMetricCard('Margen requerido', formatMoney(preview.required_margin_usdt), `${formatNumber(diagnostics.margin_usage_pct || 0, 2)}% del capital`)}
        ${riskMetricCard('Notional', formatMoney(preview.position_notional_usdt), `Qty ${escapeHtml(formatNumber(preview.quantity_estimate || 0, 6))}`)}
        ${riskMetricCard('Buffer', formatMoney(diagnostics.capital_buffer_usdt), `Mejor RR ${escapeHtml(formatNumber(diagnostics.best_rr_net || 0, 2))}`)}
      </div>
      <div class="feature-list" style="margin-top:12px;">
        <div class="feature-item">Entrada <strong>${escapeHtml(formatPrice(preview.entry_price, 6))}</strong></div>
        <div class="feature-item">Stop <strong>${escapeHtml(formatPrice(preview.stop_loss, 6))}</strong></div>
        <div class="feature-item">Distancia al stop <strong>${escapeHtml(formatFractionPercent(preview.stop_distance_pct))}</strong></div>
        <div class="feature-item">Pérdida efectiva <strong>${escapeHtml(formatFractionPercent(preview.effective_loss_pct))}</strong></div>
        <div class="feature-item">Fee round-trip <strong>${escapeHtml(formatFractionPercent(preview.fee_roundtrip_pct))}</strong></div>
        <div class="feature-item">Slippage <strong>${escapeHtml(formatFractionPercent(preview.slippage_decimal))}</strong></div>
      </div>
      <div class="list" style="margin-top:12px;">
        ${tpResults.length ? tpResults.map(tp => `
          <div class="item compact-item">
            <div class="item-header">
              <div class="item-title">${escapeHtml(tp.name || 'TP')}</div>
              <span class="plan-tag">RR ${escapeHtml(formatNumber(tp.rr_net || 0, 2))}</span>
            </div>
            <div class="inline-meta">
              <span>Precio: ${escapeHtml(formatPrice(tp.price, 6))}</span>
              <span>Distancia: ${escapeHtml(formatFractionPercent(tp.distance_pct))}</span>
              <span>Neto: ${escapeHtml(formatMoney(tp.net_profit_usdt))}</span>
            </div>
          </div>
        `).join('') : '<div class="empty-state">No hay take profits calculables para esta señal.</div>'}
      </div>
      ${preview.warnings?.length ? `<div class="card" style="margin-top:12px;"><h3 style="margin-top:0;">Notas</h3><div class="feature-list">${preview.warnings.map(item => `<div class="feature-item">• ${escapeHtml(item)}</div>`).join('')}</div></div>` : ''}
    </div>
  `;
}

function renderRisk() {
  if (!els.risk) return;
  const payload = state.riskCenter.payload || {};
  const overview = payload.overview || {};
  const profile = payload.profile || {};
  const readiness = payload.readiness || {};
  const catalog = payload.catalog || {};
  const signals = payload.signals || {};
  const preview = payload.preview || null;
  const previewError = payload.preview_error || '';
  const liveSignals = Array.isArray(signals.live) ? signals.live : [];
  const historySignals = Array.isArray(signals.history) ? signals.history : [];
  const selectedSignalId = signals.selected_signal_id || preview?.signal_id || null;
  const selectedSignal = signals.selected_signal || null;
  const loadingBanner = state.riskCenter.loading
    ? '<div class="card card-span-12"><div class="loading-inline">Actualizando gestión de riesgo...</div></div>'
    : '';

  if (!state.riskCenter.payload && !state.riskCenter.loading) {
    els.risk.innerHTML = `
      <div class="section-grid">
        ${riskNoticeCard(state.riskCenter.notice)}
        <div class="card card-span-12">
          <h2>Gestión de riesgo</h2>
          <p>Configura tu capital, riesgo por trade y calculadora para señales activas e históricas.</p>
          <div class="action-row"><button class="button button-primary" data-open-risk-center="true">Abrir centro de riesgo</button></div>
        </div>
      </div>
    `;
    return;
  }

  const exchangeOptions = Array.isArray(catalog.exchanges) ? catalog.exchanges : [];
  const entryModeOptions = Array.isArray(catalog.entry_modes) ? catalog.entry_modes : [];
  const profileOptions = Array.isArray(overview.profile_options) && overview.profile_options.length ? overview.profile_options : ['moderado'];
  const isBasicTier = String(overview.feature_tier || '') === 'basic';
  const configStateClass = readiness.is_ready ? 'is-positive' : 'is-warning';

  els.risk.innerHTML = `
    <div class="section-grid">
      ${riskNoticeCard(state.riskCenter.notice)}
      ${loadingBanner}
      <div class="card card-span-12">
        <div class="item-header">
          <div>
            <h2 style="margin:0;">Gestión de riesgo</h2>
            <div class="item-subtitle">Centro único para capital, riesgo por trade, exchange, fees, slippage y calculadora sobre señales reales.</div>
          </div>
          <div class="action-row compact">
            <button class="button button-secondary" data-goto="account">Volver a cuenta</button>
            <button class="button button-secondary" data-risk-refresh="true">Refrescar</button>
          </div>
        </div>
        <div class="pill-row compact-pill-row">
          <span class="pill">Plan: ${escapeHtml(overview.plan_name || 'FREE')}</span>
          <span class="pill">Tier: ${escapeHtml(isBasicTier ? 'Básico' : 'Completo')}</span>
          <span class="pill">Perfiles: ${escapeHtml(profileOptions.map(profileLabel).join(' / '))}</span>
        </div>
      </div>

      ${riskMetricCard('Capital', formatMoney(profile.capital_usdt), 'Base para el sizing')}
      ${riskMetricCard('Riesgo / trade', `${escapeHtml(formatNumber(profile.risk_percent || 0, 2))}%`, 'Pérdida máxima objetivo')}
      ${riskMetricCard('Leverage base', `${escapeHtml(formatNumber(profile.default_leverage || 0, 0))}x`, escapeHtml(profile.entry_mode_label || 'Límite'))}
      ${riskMetricCard('Estado', readiness.is_ready ? 'Listo' : 'Bloqueado', readiness.message || 'Sin diagnóstico', configStateClass)}

      <div class="card card-span-12 ${configStateClass}">
        <h2>Perfil operativo</h2>
        <div class="section-grid" style="margin-top:12px;">
          <label class="card card-span-3" style="padding:12px;">
            <div class="metric-label">Capital USDT</div>
            <input id="riskCapitalInput" class="input" type="number" min="0" step="0.01" value="${escapeHtml(profile.capital_usdt ?? '')}">
          </label>
          <label class="card card-span-3" style="padding:12px;">
            <div class="metric-label">Riesgo %</div>
            <input id="riskPercentInput" class="input" type="number" min="0.01" step="0.01" value="${escapeHtml(profile.risk_percent ?? '')}">
          </label>
          <label class="card card-span-3" style="padding:12px;">
            <div class="metric-label">Exchange</div>
            <select id="riskExchangeSelect" class="input">${exchangeOptions.map(option => `<option value="${escapeHtml(option.value)}" ${option.value === profile.exchange ? 'selected' : ''}>${escapeHtml(option.label)}</option>`).join('')}</select>
          </label>
          <label class="card card-span-3" style="padding:12px;">
            <div class="metric-label">Tipo de entrada</div>
            <select id="riskEntryModeSelect" class="input">${entryModeOptions.map(option => `<option value="${escapeHtml(option.value)}" ${option.value === profile.entry_mode ? 'selected' : ''}>${escapeHtml(option.label)}</option>`).join('')}</select>
          </label>
          <label class="card card-span-3" style="padding:12px;">
            <div class="metric-label">Fee por lado %</div>
            <input id="riskFeeInput" class="input" type="number" min="0" step="0.001" value="${escapeHtml(profile.fee_percent_per_side ?? '')}">
          </label>
          <label class="card card-span-3" style="padding:12px;">
            <div class="metric-label">Slippage %</div>
            <input id="riskSlippageInput" class="input" type="number" min="0" step="0.001" value="${escapeHtml(profile.slippage_percent ?? '')}">
          </label>
          <label class="card card-span-3" style="padding:12px;">
            <div class="metric-label">Leverage por defecto</div>
            <input id="riskLeverageInput" class="input" type="number" min="1" step="0.1" value="${escapeHtml(profile.default_leverage ?? '')}">
          </label>
          <label class="card card-span-3" style="padding:12px;">
            <div class="metric-label">Perfil base</div>
            <select id="riskDefaultProfileSelect" class="input" ${isBasicTier ? 'disabled' : ''}>${profileOptions.map(option => `<option value="${escapeHtml(option)}" ${option === profile.default_profile ? 'selected' : ''}>${escapeHtml(profileLabel(option))}</option>`).join('')}</select>
          </label>
        </div>
        <div class="action-row" style="margin-top:12px;">
          <button class="button button-secondary" data-risk-apply-preset="true">Cargar preset exchange</button>
          <button class="button button-primary" data-risk-save-profile="true">Guardar perfil</button>
        </div>
        ${isBasicTier ? '<div class="detail-note" style="margin-top:12px;">En FREE el cálculo usa el perfil Moderado. Plus y Premium desbloquean Conservador y Agresivo.</div>' : ''}
      </div>

      <div class="card card-span-12">
        <div class="item-header">
          <div>
            <h2 style="margin:0;">Calculadora por señal</h2>
            <div class="item-subtitle">Selecciona una señal en vivo o del historial para calcular sizing, margen, pérdida y RR neto.</div>
          </div>
          ${selectedSignal ? `<span class="plan-tag">${escapeHtml(selectedSignal.symbol)} ${escapeHtml(selectedSignal.direction)}</span>` : ''}
        </div>
        ${previewError ? `<div class="error-banner" style="margin-top:12px;">${escapeHtml(previewError)}</div>` : ''}
      </div>

      <div class="card card-span-6">
        <h2>Señales en vivo</h2>
        <div class="list">${liveSignals.length ? liveSignals.map(item => riskCandidateCard(item, selectedSignalId)).join('') : '<div class="empty-state">No hay señales activas recientes para calcular ahora mismo.</div>'}</div>
      </div>

      <div class="card card-span-6">
        <h2>Historial reciente</h2>
        <div class="list">${historySignals.length ? historySignals.map(item => riskCandidateCard(item, selectedSignalId)).join('') : '<div class="empty-state">Todavía no hay histórico para calcular.</div>'}</div>
      </div>

      ${selectedSignal && !preview ? `
        <div class="card card-span-12">
          <h2>Señal seleccionada</h2>
          <div class="pill-row compact-pill-row">
            <span class="pill">${escapeHtml(selectedSignal.symbol)}</span>
            <span class="pill">${escapeHtml(selectedSignal.direction)}</span>
            <span class="pill">${selectedSignal.source === 'history' ? 'Historial' : 'En vivo'}</span>
          </div>
          <div class="action-row compact" style="margin-top:12px;">
            ${profileOptions.map(option => `<button class="button ${option === (signals.selected_profile || profile.default_profile) ? 'button-primary' : 'button-secondary'}" data-risk-select-profile="${escapeHtml(option)}">${escapeHtml(profileLabel(option))}</button>`).join('')}
            <button class="button button-secondary" data-risk-preview-run="true">Calcular</button>
            <button class="button button-secondary" data-risk-clear-selection="true">Limpiar</button>
          </div>
        </div>
      ` : ''}

      ${riskPreviewCard(preview, payload)}
    </div>
  `;
}

function setPerformanceNotice(message, tone = 'warning') {
  const normalized = String(message || '').trim();
  state.performanceCenter.notice = normalized ? { message: normalized, tone: String(tone || 'warning') } : null;
}

function performanceNoticeCard(notice) {
  if (!notice?.message) return '';
  const toneClass = billingToneClass(notice.tone);
  return `
    <div class="card payment-focus-panel card-span-12 ${toneClass}">
      <div class="payment-focus-card ${toneClass}">
        <div class="payment-focus-copy">
          <div class="payment-focus-kicker">Performance</div>
          <div class="payment-focus-title">Estado del módulo de rendimiento</div>
          <div class="payment-focus-message">${escapeHtml(notice.message)}</div>
        </div>
      </div>
    </div>
  `;
}

function performanceMetricCard(label, value, subtitle = '', toneClass = '') {
  return metricCard(label, value, subtitle, '', toneClass);
}

function formatRatioValue(value, infinite = false, digits = 2) {
  if (infinite) return '∞';
  if (value === null || value === undefined || Number.isNaN(Number(value))) return '—';
  return Number(value).toFixed(digits);
}

function normalizePerformanceDays(value) {
  const numeric = Number(value);
  if (numeric === 7 || numeric === 30 || numeric === 3650) return numeric;
  return 30;
}

function performanceWindowLabel(days) {
  return normalizePerformanceDays(days) === 3650 ? 'Total' : `${normalizePerformanceDays(days)}D`;
}

async function refreshPerformanceCenter(force = false, options = {}) {
  const nextDays = options.days !== undefined && options.days !== null
    ? normalizePerformanceDays(options.days)
    : normalizePerformanceDays(state.performanceCenter.query?.days || 30);
  const sameQuery = nextDays === normalizePerformanceDays(state.performanceCenter.query?.days || 30);
  if (state.performanceCenter.loading) return state.performanceCenter.payload;
  if (!force && state.performanceCenter.payload && sameQuery) return state.performanceCenter.payload;

  state.performanceCenter.loading = true;
  state.performanceCenter.query = { days: nextDays };
  renderPerformance();
  bindViewButtons();
  try {
    const payload = await api(`/api/miniapp/performance?days=${nextDays}`);
    state.performanceCenter.payload = payload;
    return payload;
  } catch (error) {
    setPerformanceNotice(`No se pudo cargar el rendimiento: ${error.message || 'error'}`, 'warning');
    throw error;
  } finally {
    state.performanceCenter.loading = false;
    renderPerformance();
    bindViewButtons();
  }
}

async function openPerformanceCenter(options = {}) {
  closeSignalDetailModal();
  setView('performance');
  renderPerformance();
  bindViewButtons();
  try {
    await refreshPerformanceCenter(true, options);
  } catch (_) {}
}

function performancePlanCard(item) {
  const summary = item?.summary || {};
  const activity = item?.activity || {};
  return `
    <div class="card card-span-4">
      <div class="item-header">
        <div>
          <h2 style="margin:0;">${escapeHtml(item.plan_name || item.plan || 'Plan')}</h2>
          <div class="item-subtitle">Scanner ${escapeHtml(activity.signals_total ?? 0)} · Score norm. ${escapeHtml(activity.avg_score === null ? '—' : formatNumber(activity.avg_score, 2))}</div>
        </div>
        <span class="plan-tag">30D</span>
      </div>
      <div class="account-metric-grid">
        ${accountMetricCard('Resueltas', summary.resolved ?? 0)}
        ${accountMetricCard('Win rate', `${formatNumber(summary.winrate || 0)}%`, '', metricToneClass('winrate', summary.winrate || 0))}
        ${accountMetricCard('PF (R)', formatRatioValue(summary.profit_factor, summary.profit_factor_infinite), '', metricToneClass('pf', summary.profit_factor_infinite ? 999 : summary.profit_factor || 0))}
        ${accountMetricCard('Expectancy', formatNumber(summary.expectancy_r || 0), 'R por resuelta', metricToneClass('expectancy', summary.expectancy_r || 0))}
      </div>
      <div class="pill-row compact-pill-row" style="margin-top:12px;">
        <span class="pill">Fill ${escapeHtml(summary.filled_total ?? 0)}</span>
        <span class="pill">Exp no fill ${escapeHtml(summary.expired_no_fill ?? 0)}</span>
        <span class="pill">Exp tras entry ${escapeHtml(summary.expired_after_entry ?? 0)}</span>
        <span class="pill">Exp total ${escapeHtml(summary.expired ?? 0)}</span>
      </div>
      <div class="pill-row compact-pill-row" style="margin-top:8px;">
        <span class="pill">TP1 ${escapeHtml(summary.tp1 ?? 0)}</span>
        <span class="pill">TP2 ${escapeHtml(summary.tp2 ?? 0)}</span>
        <span class="pill">SL ${escapeHtml(summary.sl ?? 0)}</span>
      </div>
    </div>
  `;
}

function performanceDirectionItem(item) {
  return `
    <div class="item compact-item">
      <div class="item-header">
        <div>
          <div class="item-title">${escapeHtml(item.direction || '—')}</div>
          <div class="item-subtitle">Resueltas ${escapeHtml(item.resolved ?? 0)} · Exp ${escapeHtml(item.expired ?? 0)}</div>
        </div>
        <span class="plan-tag ${metricToneClass('winrate', item.winrate || 0)}">${escapeHtml(formatNumber(item.winrate || 0))}%</span>
      </div>
      <div class="inline-meta">
        <span>PF ${escapeHtml(formatRatioValue(item.profit_factor, item.profit_factor_infinite))}</span>
        <span>Expectancy ${escapeHtml(formatNumber(item.expectancy_r || 0, 4))}R</span>
        <span>Loss ${escapeHtml(item.lost ?? 0)}</span>
        <span>No fill ${escapeHtml(item.expired_no_fill ?? 0)}</span>
        <span>Tras entry ${escapeHtml(item.expired_after_entry ?? 0)}</span>
      </div>
    </div>
  `;
}

function performanceStrategyCard(item) {
  return `
    <div class="card card-span-6">
      <div class="item-header">
        <div>
          <h2 style="margin:0;">${escapeHtml(item.strategy_label || '—')}</h2>
          <div class="item-subtitle">Scanner ${escapeHtml(item.signals_total ?? 0)} · Score norm. ${escapeHtml(item.avg_score === null ? '—' : formatNumber(item.avg_score, 2))} · ${escapeHtml(item.primary_send_mode_label || 'Modelo no identificado')}</div>
        </div>
        <span class="plan-tag">30D</span>
      </div>
      <div class="account-metric-grid">
        ${accountMetricCard('Resueltas', item.resolved ?? 0)}
        ${accountMetricCard('Win rate', `${formatNumber(item.winrate || 0)}%`, '', metricToneClass('winrate', item.winrate || 0))}
        ${accountMetricCard('PF (R)', formatRatioValue(item.profit_factor, item.profit_factor_infinite), '', metricToneClass('pf', item.profit_factor_infinite ? 999 : item.profit_factor || 0))}
        ${accountMetricCard('Expectancy', formatNumber(item.expectancy_r || 0, 4), 'R por resuelta', metricToneClass('expectancy', item.expectancy_r || 0))}
      </div>
      <div class="pill-row compact-pill-row" style="margin-top:12px;">
        <span class="pill">Fill rate ${escapeHtml(formatNumber(item.fill_rate || 0))}%</span>
        <span class="pill">Exp no fill ${escapeHtml(item.expired_no_fill ?? 0)}</span>
        <span class="pill">Exp tras entry ${escapeHtml(item.expired_after_entry ?? 0)}</span>
        <span class="pill">Exp total ${escapeHtml(item.expired ?? 0)}</span>
      </div>
      <div class="pill-row compact-pill-row" style="margin-top:8px;">
        <span class="pill">TP1 ${escapeHtml(item.tp1 ?? 0)}</span>
        <span class="pill">TP2 ${escapeHtml(item.tp2 ?? 0)}</span>
        <span class="pill">SL ${escapeHtml(item.sl ?? 0)}</span>
      </div>
    </div>
  `;
}

function performanceStrategyDirectionItem(item) {
  return `
    <div class="item compact-item">
      <div class="item-header">
        <div>
          <div class="item-title">${escapeHtml(item.strategy_label || '—')} · ${escapeHtml(item.direction || '—')}</div>
          <div class="item-subtitle">Resueltas ${escapeHtml(item.resolved ?? 0)} · Exp ${escapeHtml(item.expired ?? 0)}</div>
        </div>
        <span class="plan-tag ${metricToneClass('winrate', item.winrate || 0)}">${escapeHtml(formatNumber(item.winrate || 0))}%</span>
      </div>
      <div class="inline-meta">
        <span>PF ${escapeHtml(formatRatioValue(item.profit_factor, item.profit_factor_infinite))}</span>
        <span>Expectancy ${escapeHtml(formatNumber(item.expectancy_r || 0, 4))}R</span>
        <span>Loss ${escapeHtml(item.lost ?? 0)}</span>
        <span>No fill ${escapeHtml(item.expired_no_fill ?? 0)}</span>
        <span>Tras entry ${escapeHtml(item.expired_after_entry ?? 0)}</span>
      </div>
    </div>
  `;
}

function performanceWeakSymbolItem(item) {
  return `
    <div class="item compact-item">
      <div class="item-header">
        <div>
          <div class="item-title">${escapeHtml(item.symbol || '—')}</div>
          <div class="item-subtitle">Resueltas ${escapeHtml(item.resolved ?? 0)} · Loss ${escapeHtml(item.lost ?? 0)} · Exp ${escapeHtml(item.expired ?? 0)}</div>
        </div>
        <span class="plan-tag ${metricToneClass('winrate', item.winrate || 0)}">${escapeHtml(formatNumber(item.winrate || 0))}%</span>
      </div>
      <div class="inline-meta">
        <span>PF ${escapeHtml(formatRatioValue(item.profit_factor, item.profit_factor_infinite))}</span>
        <span>Expectancy ${escapeHtml(formatNumber(item.expectancy_r || 0, 4))}R</span>
        <span>No fill ${escapeHtml(item.expired_no_fill ?? 0)}</span>
        <span>Tras entry ${escapeHtml(item.expired_after_entry ?? 0)}</span>
      </div>
    </div>
  `;
}

function performanceScoreBucketItem(item) {
  return `
    <div class="item compact-item">
      <div class="item-header">
        <div>
          <div class="item-title">Raw score ${escapeHtml(item.label || '—')}</div>
          <div class="item-subtitle">Muestra ${escapeHtml(item.n ?? 0)} · Won ${escapeHtml(item.won ?? 0)} · Lost ${escapeHtml(item.lost ?? 0)}</div>
        </div>
        <span class="plan-tag ${metricToneClass('winrate', item.winrate || 0)}">${escapeHtml(formatNumber(item.winrate || 0))}%</span>
      </div>
      <div class="inline-meta">
        <span>Net ${escapeHtml(formatNumber(item.net_r || 0, 4))}R</span>
      </div>
    </div>
  `;
}

function renderPerformance() {
  if (!els.performance) return;
  const payload  = state.performanceCenter.payload || {};
  const overview = payload.overview || {};
  const focus    = payload.focus   || {};
  const summary  = focus.summary   || {};
  const activity = focus.activity  || {};
  const windows  = Array.isArray(payload.windows) ? payload.windows : [];

  // ── Empty / notice-only state ─────────────────────────────────────────────
  if (!state.performanceCenter.payload && !state.performanceCenter.loading) {
    els.performance.innerHTML = `
      <div class="section-grid">
        ${performanceNoticeCard(state.performanceCenter.notice)}
        <div class="card card-span-12 perf-empty-card">
          <div class="perf-empty-icon">📊</div>
          <h2 class="perf-empty-title">Rendimiento serio</h2>
          <p class="perf-empty-sub">PF por R, expectancy, TP1/TP2/SL, fill rate y actividad del scanner — sin exponer la inteligencia interna del motor.</p>
          <div class="action-row" style="margin-top:18px;justify-content:center;">
            <button class="button button-primary perf-cta-btn" data-open-performance-center="true">Abrir rendimiento</button>
          </div>
        </div>
      </div>`;
    return;
  }

  const loading = state.performanceCenter.loading;

  // ── Diagnosis ──────────────────────────────────────────────────────────────
  const diag     = summaryDiagnosis(summary);
  const diagIcon = diag.tone === 'diagnostic-positive' ? '✦' : diag.tone === 'diagnostic-warning' ? '◈' : '◆';
  const diagColor = diag.tone === 'diagnostic-positive' ? '#22c55e' : diag.tone === 'diagnostic-warning' ? '#f59e0b' : '#ef4444';

  // ── Resolution bar builder ─────────────────────────────────────────────────
  const tp1 = Number(summary.tp1 ?? 0);
  const tp2 = Number(summary.tp2 ?? 0);
  const sl  = Number(summary.sl  ?? 0);
  const exp = Number(summary.expired ?? 0);
  const maxCount = Math.max(tp1 + tp2, sl, exp, 1);
  const barPct = v => `${Math.round((Number(v) / maxCount) * 100)}%`;

  function resBar(label, count, rVal, color, bgColor) {
    const pct = Math.round((Number(count) / maxCount) * 100);
    return `
      <div class="perf-res-row">
        <div class="perf-res-meta">
          <span class="perf-res-label">${escapeHtml(label)}</span>
          <span class="perf-res-r" style="color:${color}">${escapeHtml(rVal)}</span>
        </div>
        <div class="perf-res-track">
          <div class="perf-res-fill" style="width:${pct}%;background:${bgColor};"></div>
          <span class="perf-res-count">${escapeHtml(String(count))}</span>
        </div>
      </div>`;
  }

  // ── KPI card builder ───────────────────────────────────────────────────────
  function kpiCard(label, value, sub, tone = '') {
    const colorMap = {
      'metric-positive': '#22c55e',
      'metric-warning':  '#f59e0b',
      'metric-negative': '#ef4444',
      '': 'var(--text)',
    };
    const glowMap = {
      'metric-positive': 'rgba(34,197,94,0.15)',
      'metric-warning':  'rgba(245,158,11,0.15)',
      'metric-negative': 'rgba(239,68,68,0.15)',
      '': 'transparent',
    };
    const col  = colorMap[tone]  || colorMap[''];
    const glow = glowMap[tone]   || glowMap[''];
    return `
      <div class="perf-kpi-card ${tone}">
        <div class="perf-kpi-label">${escapeHtml(label)}</div>
        <div class="perf-kpi-value" style="color:${col};text-shadow:0 0 24px ${glow};">${escapeHtml(String(value))}</div>
        <div class="perf-kpi-sub">${escapeHtml(sub)}</div>
      </div>`;
  }

  // ── Secondary stat builder ─────────────────────────────────────────────────
  function statBox(label, value, tone = '') {
    return `
      <div class="perf-stat-box ${tone}">
        <div class="perf-stat-label">${escapeHtml(label)}</div>
        <div class="perf-stat-value">${escapeHtml(String(value))}</div>
      </div>`;
  }

  // ── Gross R breakdown ──────────────────────────────────────────────────────
  const grossPos = Number(summary.gross_profit_r || 0);
  const grossNeg = Number(summary.gross_loss_r   || 0);
  const netR     = Number(summary.net_r          || 0);
  const netRSign = netR >= 0 ? '+' : '';

  els.performance.innerHTML = `
    <div class="section-grid">
      ${performanceNoticeCard(state.performanceCenter.notice)}

      <!-- ── HERO CARD ──────────────────────────────────────────────── -->
      <div class="card card-span-12 perf-hero-card ${diag.tone}" style="--diag-color:${diagColor};">
        <div class="perf-hero-inner">
          <div class="perf-hero-left">
            <div class="perf-hero-eyebrow">
              <span class="live-dot" style="background:${diagColor};box-shadow:0 0 6px ${diagColor};"></span>
              RENDIMIENTO SERIO
            </div>
            <div class="perf-hero-title">${escapeHtml(diag.title)}</div>
            <p class="perf-hero-sub">${escapeHtml(diag.text)}</p>
            <div class="perf-window-tabs">
              ${windows.map(item => `
                <button class="perf-window-tab ${normalizePerformanceDays(item.days) === normalizePerformanceDays(overview.focus_days) ? 'active' : ''}"
                        data-performance-window="${escapeHtml(item.days)}">${escapeHtml(item.label)}</button>
              `).join('')}
              <button class="perf-window-tab perf-refresh-tab" data-performance-refresh="true">${loading ? '…' : '↻'}</button>
            </div>
          </div>
          <div class="perf-hero-right">
            <div class="perf-hero-icon" style="color:${diagColor};">${diagIcon}</div>
            <div class="perf-hero-window">${escapeHtml(overview.focus_label || performanceWindowLabel(overview.focus_days || 30))}</div>
            <div class="perf-hero-total">${escapeHtml(String(summary.total ?? '—'))}<span class="perf-hero-total-label"> evaluadas</span></div>
            <div class="perf-hero-gen">Act. ${escapeHtml(formatDate(overview.generated_at))}</div>
          </div>
        </div>
      </div>

      <!-- ── KPI GRID: 4 métricas clave ──────────────────────────── -->
      <div class="card card-span-12 perf-kpi-section">
        <div class="eyebrow" style="margin-bottom:14px;">MÉTRICAS CLAVE</div>
        <div class="perf-kpi-grid">
          ${kpiCard('Win rate', loading ? '…' : `${formatNumber(summary.winrate || 0)}%`,  'de señales resueltas',  metricToneClass('winrate', summary.winrate || 0))}
          ${kpiCard('Profit factor', loading ? '…' : formatRatioValue(summary.profit_factor, summary.profit_factor_infinite), 'TP vs SL en R', metricToneClass('pf', summary.profit_factor_infinite ? 999 : summary.profit_factor || 0))}
          ${kpiCard('Expectancy R', loading ? '…' : (summary.expectancy_r >= 0 ? '+' : '') + formatNumber(summary.expectancy_r || 0, 4), 'R promedio por resuelta', metricToneClass('expectancy', summary.expectancy_r || 0))}
          ${kpiCard('Net R', loading ? '…' : `${netRSign}${formatNumber(netR, 2)}R`, 'resultado neto del periodo', metricToneClass('expectancy', netR))}
        </div>
      </div>

      <!-- ── MODELO DE RESOLUCIÓN (barras) ─────────────────────── -->
      <div class="card card-span-12 perf-resolution-card">
        <div class="perf-resolution-header">
          <div>
            <div class="eyebrow">MODELO R</div>
            <h2 style="margin:4px 0 0;">Resoluciones</h2>
          </div>
          <div class="perf-gross-summary">
            <div class="perf-gross-item metric-positive">
              <span class="perf-gross-label">BRUTO +</span>
              <span class="perf-gross-val">+${escapeHtml(formatNumber(grossPos, 2))}R</span>
            </div>
            <div class="perf-gross-sep"></div>
            <div class="perf-gross-item metric-negative">
              <span class="perf-gross-label">BRUTO −</span>
              <span class="perf-gross-val">−${escapeHtml(formatNumber(Math.abs(grossNeg), 2))}R</span>
            </div>
          </div>
        </div>
        <div class="perf-res-bars">
          ${resBar('TP1', tp1, '+1R por señal', '#22c55e', 'rgba(34,197,94,0.70)')}
          ${resBar('TP2', tp2, '+2R por señal', '#4ade80', 'rgba(74,222,128,0.55)')}
          ${resBar('SL',  sl,  '−1R por señal', '#ef4444', 'rgba(239,68,68,0.70)')}
          ${resBar('Exp', exp, 'expiradas',      '#6b7280', 'rgba(107,114,128,0.45)')}
        </div>
        <div class="perf-res-footer">
          <span class="perf-res-footer-item">Resueltas <strong>${escapeHtml(String(summary.resolved ?? 0))}</strong></span>
          <span class="perf-res-footer-sep">·</span>
          <span class="perf-res-footer-item">Fill rate <strong class="${metricToneClass('winrate', summary.fill_rate || 0)}">${escapeHtml(formatNumber(summary.fill_rate || 0))}%</strong></span>
          <span class="perf-res-footer-sep">·</span>
          <span class="perf-res-footer-item">No fill <strong>${escapeHtml(String(summary.expired_no_fill ?? 0))}</strong></span>
          <span class="perf-res-footer-sep">·</span>
          <span class="perf-res-footer-item">Fallo post-entry <strong>${escapeHtml(String(summary.expired_after_entry ?? 0))}</strong></span>
        </div>
      </div>

      <!-- ── MÉTRICAS SECUNDARIAS ───────────────────────────────── -->
      <div class="card card-span-12 perf-secondary-card">
        <div class="eyebrow" style="margin-bottom:12px;">RIESGO Y ACTIVIDAD</div>
        <div class="perf-secondary-grid">
          ${statBox('Max DD (R)',       loading ? '…' : formatNumber(summary.max_drawdown_r || 0, 2) + 'R',   metricToneClass('drawdown', summary.max_drawdown_r || 0))}
          ${statBox('Fill rate',        loading ? '…' : `${formatNumber(summary.fill_rate || 0)}%`,           metricToneClass('winrate', summary.fill_rate || 0))}
          ${statBox('Exp no fill',      loading ? '…' : `${escapeHtml(String(summary.expired_no_fill ?? 0))}  (${formatNumber(summary.no_fill_rate || 0)}%)`, metricToneClass('drawdown', -(summary.no_fill_rate || 0)))}
          ${statBox('Exp tras entry',   loading ? '…' : `${escapeHtml(String(summary.expired_after_entry ?? 0))}  (${formatNumber(summary.after_entry_failure_rate || 0)}%)`, metricToneClass('drawdown', -(summary.after_entry_failure_rate || 0)))}
          ${statBox('Señales scanner',  loading ? '…' : String(activity.signals_total ?? '—'), '')}
          ${statBox('Score medio',      loading ? '…' : (activity.avg_score === null ? '—' : formatNumber(activity.avg_score, 2)), '')}
          ${statBox('Eval media',       loading ? '…' : (summary.avg_resolution_minutes === null ? '—' : `${formatNumber(summary.avg_resolution_minutes, 1)} min`), '')}
          ${statBox('Total evaluadas',  loading ? '…' : String(summary.total ?? '—'), '')}
        </div>
      </div>

    </div>
  `;
}


async function refreshAdminOverview(force = false) {
  if (!state.payload?.me?.is_admin) return null;
  if (state.adminPanel.loading) return state.adminPanel.overview;
  if (!force && state.adminPanel.overview) return state.adminPanel.overview;
  state.adminPanel.loading = true;
  renderAdmin();
  try {
    const overview = await api('/api/miniapp/admin/overview');
    state.adminPanel.overview = overview;
    if (!state.adminPanel.notice) {
      setAdminNotice('Panel admin listo. Desde aquí vivirán las herramientas operativas sensibles de la MiniApp.', 'accent');
    }
    return overview;
  } catch (error) {
    setAdminNotice(`No se pudo cargar el panel admin: ${error.message || 'error'}`, 'warning');
    throw error;
  } finally {
    state.adminPanel.loading = false;
    renderAdmin();
    bindViewButtons();
  }
}

async function openAdminPanel(force = false) {
  if (!state.payload?.me?.is_admin) {
    tg?.showAlert('Solo los administradores pueden abrir este panel.');
    return;
  }
  setView('admin');
  renderAdmin();
  bindViewButtons();
  try {
    await refreshAdminOverview(force);
  } catch (_) {}
}

function renderAdmin() {
  const me = state.payload?.me || {};
  if (!els.admin) return;
  if (!me.is_admin) {
    els.admin.innerHTML = `
      <div class="section-grid">
        <div class="card card-span-12">
          <h2>Acceso restringido</h2>
          <div class="empty-state">Este panel solo está disponible para administradores autorizados.</div>
        </div>
      </div>
    `;
    return;
  }

  const overview = state.adminPanel.overview || {};
  const runtime = overview.runtime || {};
  const users = overview.users || {};
  const signals = overview.signals || {};
  const payments = overview.payments || {};
  const audit = overview.audit || {};
  const performance = overview.performance || {};
  const performanceOverview = performance.overview || {};
  const performanceFocus = performance.focus || {};
  const performanceSummary = performanceFocus.summary || {};
  const performanceActivity = performanceFocus.activity || {};
  const performanceDiagnostics = performance.diagnostics_30d || {};
  const planBreakdown = Array.isArray(performance.plan_breakdown_30d) ? performance.plan_breakdown_30d : [];
  const directions = Array.isArray(performance.direction_30d) ? performance.direction_30d : [];
  const strategies = Array.isArray(performance.strategy_30d) ? performance.strategy_30d : [];
  const strategyDirections = Array.isArray(performance.strategy_direction_30d) ? performance.strategy_direction_30d : [];
  const weakSymbols = Array.isArray(performance.weak_symbols_30d) ? performance.weak_symbols_30d : [];
  const scoreBuckets = Array.isArray(performance.score_buckets_30d) ? performance.score_buckets_30d : [];
  const strategyObservability = performance.strategy_observability_30d || {};
  const strategyTelemetryOverview = strategyObservability.overview || {};
  const strategyPipeline = Array.isArray(strategyObservability.strategy_pipeline) ? strategyObservability.strategy_pipeline : [];
  const strategyRejects = Array.isArray(strategyObservability.reject_reasons_by_strategy) ? strategyObservability.reject_reasons_by_strategy : [];
  const regimeDistribution = Array.isArray(strategyObservability.regime_distribution) ? strategyObservability.regime_distribution : [];
  const regimeStrategyMatrix = Array.isArray(strategyObservability.regime_strategy_matrix) ? strategyObservability.regime_strategy_matrix : [];
  const latestCycle = strategyObservability.latest_cycle || {};
  const manualActivation = state.adminPanel.manualActivation || {};
  const activationDraft = manualActivation.draft || { userId: '', plan: 'plus', days: '30' };
  const moderationState = state.adminPanel.moderation || { actionLoading: false, confirmAction: null, draft: { durationValue: '7', durationUnit: 'days' } };
  const lookupPayload = manualActivation.lookup || null;
  const target = lookupPayload?.target || null;
  const planOptions = Array.isArray(lookupPayload?.plan_options) ? lookupPayload.plan_options : [];
  const selectedPlan = planOptions.some(item => item.available && String(item.key || '').toLowerCase() === String(activationDraft.plan || '').toLowerCase())
    ? String(activationDraft.plan || '').toLowerCase()
    : (planOptions.find(item => item.available)?.key || String(activationDraft.plan || 'plus').toLowerCase());
  const selectedPlanOption = planOptions.find(item => String(item.key || '').toLowerCase() === selectedPlan) || null;
  const adminBusy = Boolean(state.adminPanel.loading || manualActivation.lookupLoading || manualActivation.activationLoading || moderationState.actionLoading);
  const loadingBanner = state.adminPanel.loading
    ? '<div class="card card-span-12"><div class="loading-inline">Actualizando panel admin...</div></div>'
    : '';
  const confirmBlock = state.adminPanel.confirmReset
    ? `
      <div class="notice-list" style="margin-top:12px;">
        <div class="notice-item">Esta acción borrará señales base, señales de usuario, resultados, histórico y snapshots activos.</div>
        <div class="notice-item">Úsalo solo cuando cambies la estrategia y quieras reiniciar la credibilidad estadística desde cero.</div>
      </div>
      <div class="action-row" style="margin-top:12px;">
        <button class="button button-danger" data-admin-reset-confirm="true">Confirmar reset</button>
        <button class="button button-secondary" data-admin-reset-cancel="true">Cancelar</button>
      </div>
    `
    : '<div class="action-row" style="margin-top:12px;"><button class="button button-danger" data-admin-reset-request="true">Resetear resultados</button></div>';

  els.admin.innerHTML = `
    <div class="section-grid">
      ${adminNoticeCard(state.adminPanel.notice)}
      ${loadingBanner}
      <div class="card card-span-12">
        <div class="item-header">
          <div>
            <h2 style="margin:0;">Panel admin</h2>
            <div class="item-subtitle">Base operativa exclusiva para administradores. Aquí crecerá el control interno de la plataforma.</div>
          </div>
          <span class="plan-tag">ADMIN</span>
        </div>
        <div class="action-row compact">
          <button class="button button-secondary" data-goto="account">Volver a cuenta</button>
          <button class="button button-secondary" data-admin-refresh="true">Refrescar panel</button>
        </div>
      </div>

      ${adminOverviewMetricCard('Runtime', runtime.overall_status || '—', runtime.ok ? 'Estado general' : 'Revisar salud', runtime.ok ? 'is-positive' : 'is-warning')}
      ${adminOverviewMetricCard('Señales 24h', formatInteger(signals.created_last_24h || 0), `${formatInteger(signals.pending_evaluation || 0)} pendientes`)}
      ${adminOverviewMetricCard('Pagos', payments.configuration_ready ? 'Configurado' : 'Incompleto', `${formatInteger(payments.pending_orders || 0)} pendientes · ${formatInteger(payments.awaiting_confirmation || 0)} por confirmar`, payments.configuration_ready ? 'is-positive' : 'is-warning')}
      ${adminOverviewMetricCard('Audit 24h', formatInteger(audit.errors_last_24h || 0), `${formatInteger(audit.warnings_last_24h || 0)} warnings`, Number(audit.errors_last_24h || 0) > 0 ? 'is-warning' : 'is-positive')}

      <div class="card card-span-12">
        <h2>Base de usuarios</h2>
        <div class="section-grid" style="margin-top:12px;">
          ${adminOverviewMetricCard('Usuarios totales', formatInteger(users.total || 0), `${formatInteger(users.banned || 0)} bloqueados`)}
          ${adminOverviewMetricCard('Free actuales', formatInteger(users.free || 0), `${formatInteger(users.trialing || 0)} en trial`)}
          ${adminOverviewMetricCard('Plus activos', formatInteger(users.plus_active || 0), 'Acceso Plus vigente', Number(users.plus_active || 0) > 0 ? 'is-accent' : '')}
          ${adminOverviewMetricCard('Premium activos', formatInteger(users.premium_active || 0), 'Acceso Premium vigente', Number(users.premium_active || 0) > 0 ? 'is-positive' : '')}
        </div>
        <div class="pill-row compact-pill-row" style="margin-top:12px;">
          <span class="pill">Pagos activos: ${escapeHtml(formatInteger(users.active_paid || 0))}</span>
          <span class="pill">Free expirado: ${escapeHtml(formatInteger(users.expired_free || 0))}</span>
          <span class="pill">Mix actual Free / Plus / Premium: ${escapeHtml(formatInteger(users.current_mix?.free || 0))} / ${escapeHtml(formatInteger(users.current_mix?.plus || 0))} / ${escapeHtml(formatInteger(users.current_mix?.premium || 0))}</span>
        </div>
      </div>

      <div class="card card-span-12">
        <h2>Salud operativa</h2>
        <div class="section-grid" style="margin-top:12px;">
          ${Object.entries(runtime.runtimes || {}).length ? Object.entries(runtime.runtimes || {}).map(([role, report]) => adminRuntimeRoleCard(role, report)).join('') : '<div class="empty-state">No hay matriz de salud disponible todavía.</div>'}
        </div>
      </div>

      <div class="card card-span-12">
        <div class="item-header">
          <div>
            <h2 style="margin:0;">Inteligencia interna de rendimiento</h2>
            <div class="item-subtitle">Bloque exclusivo para administración con los desgloses sensibles del motor y del desempeño comercial.</div>
          </div>
          <span class="plan-tag">${escapeHtml(performanceOverview.focus_label || '30D')}</span>
        </div>
        <div class="pill-row compact-pill-row" style="margin-top:12px;">
          <span class="pill">Scanner ${escapeHtml(performanceActivity.signals_total ?? 0)}</span>
          <span class="pill">Score medio ${escapeHtml(performanceActivity.avg_score === null ? '—' : formatNumber(performanceActivity.avg_score, 2))}</span>
          <span class="pill">Pendientes ${escapeHtml(performanceDiagnostics.pending_to_evaluate ?? 0)}</span>
          <span class="pill">Generado ${escapeHtml(formatDate(performanceOverview.generated_at || overview.generated_at))}</span>
        </div>
      </div>

      ${adminOverviewMetricCard('PF 30D', formatRatioValue(performanceDiagnostics.profit_factor, performanceDiagnostics.profit_factor_infinite), 'Diagnóstico interno', metricToneClass('pf', performanceDiagnostics.profit_factor_infinite ? 999 : performanceDiagnostics.profit_factor || 0))}
      ${adminOverviewMetricCard('DD 30D', formatNumber(performanceDiagnostics.max_drawdown_r || 0, 4), 'R acumulado', metricToneClass('drawdown', performanceDiagnostics.max_drawdown_r || 0))}
      ${adminOverviewMetricCard('Loss rate', `${formatNumber(performanceDiagnostics.loss_rate || 0)}%`, 'Solo admin', metricToneClass('drawdown', -(performanceDiagnostics.loss_rate || 0)))}
      ${adminOverviewMetricCard('Expiry rate', `${formatNumber(performanceDiagnostics.expiry_rate || 0)}%`, 'Solo admin', metricToneClass('drawdown', -(performanceDiagnostics.expiry_rate || 0)))}

      <div class="card card-span-12">
        <h2>Breakdown por plan (30D)</h2>
        <div class="section-grid">
          ${planBreakdown.length ? planBreakdown.map(performancePlanCard).join('') : '<div class="empty-state">No hay breakdown por plan disponible.</div>'}
        </div>
      </div>

      <div class="card card-span-6">
        <h2>Por dirección (30D)</h2>
        <div class="list">
          ${directions.length ? directions.map(performanceDirectionItem).join('') : '<div class="empty-state">Sin datos por dirección.</div>'}
        </div>
      </div>

      <div class="card card-span-6">
        <h2>Win rate por score normalizado (30D)</h2>
        <div class="list">
          ${scoreBuckets.length ? scoreBuckets.map(performanceScoreBucketItem).join('') : '<div class="empty-state">Sin buckets de score disponibles.</div>'}
        </div>
      </div>

      <div class="card card-span-12">
        <h2>Rendimiento por estrategia (30D)</h2>
        <div class="section-grid">
          ${strategies.length ? strategies.map(performanceStrategyCard).join('') : '<div class="empty-state">Sin datos suficientes por estrategia.</div>'}
        </div>
      </div>

      <div class="card card-span-6">
        <h2>Estrategia × dirección (30D)</h2>
        <div class="list">
          ${strategyDirections.length ? strategyDirections.map(performanceStrategyDirectionItem).join('') : '<div class="empty-state">Sin cruces de estrategia por dirección.</div>'}
        </div>
      </div>

      <div class="card card-span-6">
        <h2>Símbolos más débiles (30D)</h2>
        <div class="list">
          ${weakSymbols.length ? weakSymbols.map(performanceWeakSymbolItem).join('') : '<div class="empty-state">No hay suficientes señales resueltas para diagnosticar símbolos.</div>'}
        </div>
      </div>

      <div class="card card-span-12">
        <div class="item-header">
          <div>
            <h2 style="margin:0;">Embudo interno por estrategia (telemetría admin)</h2>
            <div class="item-subtitle">Intentos, pool candidato, publicadas y rechazos terminales del scanner. Este bloque existe solo para administración.</div>
          </div>
          <span class="plan-tag">${escapeHtml(strategyTelemetryOverview.telemetry_ready ? 'ACTIVO' : 'PENDIENTE')}</span>
        </div>
        <div class="pill-row compact-pill-row" style="margin-top:12px;">
          <span class="pill">Ciclos ${escapeHtml(strategyTelemetryOverview.cycles_total ?? 0)}</span>
          <span class="pill">Intentos ${escapeHtml(strategyTelemetryOverview.attempted_symbols_total ?? 0)}</span>
          <span class="pill">Pool ${escapeHtml(strategyTelemetryOverview.candidate_pool_total ?? 0)}</span>
          <span class="pill">Publicadas ${escapeHtml(strategyTelemetryOverview.selected_signals_total ?? 0)}</span>
          <span class="pill">Rechazos ${escapeHtml(strategyTelemetryOverview.rejected_symbols_total ?? 0)}</span>
          <span class="pill">Risk off ${escapeHtml(strategyTelemetryOverview.risk_off_symbols_total ?? 0)}</span>
          <span class="pill">Cobertura ${escapeHtml(formatDate(strategyTelemetryOverview.coverage_started_at || strategyTelemetryOverview.latest_cycle_at))}</span>
        </div>
        <div class="notice-list" style="margin-top:12px;">
          <div class="notice-item">La telemetría profunda del embudo empieza a contar desde que este build queda desplegado en producción.</div>
          <div class="notice-item">Los bloques de rendimiento histórico y este embudo no miden lo mismo: uno mide resultados, el otro mide selección/rechazo del scanner.</div>
        </div>
      </div>

      ${adminLatestCycleCard(latestCycle)}

      <div class="card card-span-12">
        <h2>Embudo por estrategia (30D)</h2>
        <div class="section-grid">
          ${strategyPipeline.length ? strategyPipeline.map(adminStrategyPipelineCard).join('') : '<div class="empty-state">Todavía no hay suficiente telemetría del scanner para construir el embudo histórico. El shadow actual del scanner sí mostrará el último ciclo en vivo.</div>'}
        </div>
      </div>

      <div class="card card-span-6">
        <h2>Rechazos terminales por estrategia (30D)</h2>
        <div class="list">
          ${strategyRejects.length ? strategyRejects.map(adminStrategyRejectCard).join('') : '<div class="empty-state">Aún no hay razones terminales suficientes registradas.</div>'}
        </div>
      </div>

      <div class="card card-span-6">
        <h2>Distribución por régimen (30D)</h2>
        <div class="list">
          ${regimeDistribution.length ? regimeDistribution.map(adminRegimeDistributionItem).join('') : '<div class="empty-state">Sin telemetría histórica de régimen todavía.</div>'}
        </div>
      </div>

      <div class="card card-span-12">
        <h2>Régimen → estrategia (30D)</h2>
        <div class="list">
          ${regimeStrategyMatrix.length ? regimeStrategyMatrix.map(adminRegimeStrategyItem).join('') : '<div class="empty-state">Todavía no hay cruces suficientes entre régimen y estrategia.</div>'}
        </div>
      </div>

      <div class="card card-span-12">
        <h2>Activación manual de planes</h2>
        <p>Busca un usuario por su ID de Telegram, valida su estado actual y activa Free, Plus o Premium por la cantidad exacta de días que necesites.</p>
        <div class="action-row compact" style="margin-top:12px; align-items:flex-end;">
          <label style="display:flex; flex-direction:column; gap:6px; min-width:240px;">
            <span>ID de Telegram</span>
            <input id="adminManualPlanUserIdInput" class="input" type="number" min="1" step="1" value="${escapeHtml(activationDraft.userId || '')}" placeholder="Ej: 123456789">
          </label>
          <button class="button button-secondary" data-admin-plan-lookup="true" ${manualActivation.lookupLoading ? 'disabled' : ''}>${manualActivation.lookupLoading ? 'Buscando...' : 'Buscar usuario'}</button>
        </div>
        ${adminManualTargetSummaryCard(target)}
        ${target ? `
          <div class="pill-row compact-pill-row" style="margin-top:12px;">
            <span class="pill">Free manual: ${target.free_manual_allowed ? 'Permitido' : 'Bloqueado'}</span>
            <span class="pill">Plan actual: ${escapeHtml(target.plan_name || 'FREE')}</span>
            <span class="pill">Estado: ${escapeHtml(target.subscription_status_label || '—')}</span>
          </div>
          <div class="action-row compact" style="margin-top:12px; flex-wrap:wrap;">
            ${planOptions.map(option => adminManualActivationButton(option, selectedPlan, adminBusy)).join('')}
          </div>
          <div class="action-row compact" style="margin-top:12px; align-items:flex-end;">
            <label style="display:flex; flex-direction:column; gap:6px; min-width:180px;">
              <span>Días exactos</span>
              <input id="adminManualPlanDaysInput" class="input" type="number" min="1" step="1" value="${escapeHtml(activationDraft.days || '30')}" placeholder="Ej: 15">
            </label>
            <button class="button button-primary" data-admin-plan-activate="true" ${manualActivation.activationLoading || !selectedPlanOption?.available ? 'disabled' : ''}>${manualActivation.activationLoading ? 'Activando...' : 'Activar manualmente'}</button>
          </div>
          ${selectedPlanOption && !selectedPlanOption.available ? `<div class="detail-note" style="margin-top:12px;">${escapeHtml(selectedPlanOption.disabled_reason || 'Esa activación no está permitida para el estado actual del usuario.')}</div>` : ''}
          <div class="notice-list" style="margin-top:12px;">
            <div class="notice-item">Free manual solo aplica a usuarios Free cuyo trial ya expiró.</div>
            <div class="notice-item">Plus y Premium se activan por la cantidad exacta de días que defina el admin.</div>
          </div>
        ` : '<div class="detail-note" style="margin-top:12px;">Introduce un ID y busca el usuario antes de activar un plan manual.</div>'}
      </div>

      ${target ? adminModerationSummaryCard({ ...target, moderation: lookupPayload?.moderation || {} }, moderationState) : ''}

      <div class="card card-span-12">
        <h2>Reset de resultados</h2>
        <p>Herramienta administrativa para reiniciar estadísticas, histórico y señales acumuladas cuando cambie la estrategia y necesites comenzar desde cero.</p>
        ${confirmBlock}
      </div>

      ${adminResetSummaryCard(state.adminPanel.lastResetSummary)}

      <div class="card card-span-12">
        <h2>Resumen operativo</h2>
        <div class="feature-list">
          ${adminSummaryLine('Errores 24h', audit.errors_last_24h ?? 0)}
          ${adminSummaryLine('Warnings 24h', audit.warnings_last_24h ?? 0)}
          ${adminSummaryLine('Órdenes pendientes', payments.pending_orders ?? 0)}
          ${adminSummaryLine('Órdenes esperando confirmación', payments.awaiting_confirmation ?? 0)}
          ${adminSummaryLine('Pagos últimas 24h', payments.paid_last_24h ?? 0)}
        </div>
      </div>
    </div>
  `;
}

function metricCard(label, value, subtitle = '', extraClass = '', toneClass = '') {
  return `
    <div class="card metric-card card-span-3 ${extraClass} ${toneClass}">
      <div class="metric-label">${escapeHtml(label)}</div>
      <div class="metric-value">${escapeHtml(value)}</div>
      <div class="metric-subtitle">${escapeHtml(subtitle)}</div>
    </div>
  `;
}

function mixPills(title, mix) {
  return `
    <div class="mix-block">
      <div class="mix-title">${escapeHtml(title)}</div>
      <div class="pill-row compact-pill-row">
        <span class="pill">Free: ${escapeHtml(mix?.free ?? 0)}</span>
        <span class="pill">Plus: ${escapeHtml(mix?.plus ?? 0)}</span>
        <span class="pill">Premium: ${escapeHtml(mix?.premium ?? 0)}</span>
      </div>
    </div>
  `;
}

function signalCard(item, index) {
  const idx = typeof index === 'number' ? index : 0;
  const statusRaw = item.result || item.status || 'active';
  const isActive = !item.result && String(item.status || '').toLowerCase() === 'active';
  const dirRaw = String(item.direction || '').toUpperCase();
  const isLong = dirRaw.includes('LONG') || dirRaw === 'BUY';
  const tierRaw = String(item.visibility || '').toLowerCase();
  const isPremium = tierRaw === 'premium';
  const isPlus = tierRaw === 'plus';
  const score = Number(item.score || 0);
  const scorePct = Math.min(Math.max((score / 10) * 100, 0), 100);
  const scoreColor = score >= 7 ? '#22c55e' : score >= 4 ? '#f59e0b' : '#ef4444';

  const statusBadge = item.result
    ? `<span class="sx-result-badge sx-result-${String(item.result || '').toLowerCase()}">${escapeHtml(resultLabel(item))}</span>`
    : `<span class="sx-status-badge ${isActive ? 'sx-active' : ''}">${isActive ? '<span class="sx-pulse-dot"></span>' : ''}${escapeHtml(formatStatusLabel(statusRaw))}</span>`;

  const tierBadge = isPremium
    ? `<span class="sx-tier-badge sx-tier-premium">⬡ PREMIUM</span>`
    : isPlus
    ? `<span class="sx-tier-badge sx-tier-plus">◈ PLUS</span>`
    : `<span class="sx-tier-badge sx-tier-free">◇ FREE</span>`;

  return `
    <div class="sx-card ${isLong ? 'sx-long' : 'sx-short'} ${isPremium ? 'sx-premium' : ''} ${isActive ? 'sx-is-active' : ''}" style="animation-delay:${idx * 80}ms">
      <div class="sx-glow-bar"></div>
      <div class="sx-inner">

        <div class="sx-top-row">
          <div class="sx-left-col">
            <div class="sx-symbol-wrap">
              <span class="sx-symbol">${escapeHtml(item.symbol)}</span>
              <span class="sx-dir-badge ${isLong ? 'sx-dir-long' : 'sx-dir-short'}">
                ${isLong ? '▲ LONG' : '▼ SHORT'}
              </span>
            </div>
            <div class="sx-setup-label">${escapeHtml(item.setup_group || 'setup')}</div>
          </div>
          <div class="sx-right-col">
            ${statusBadge}
            ${tierBadge}
          </div>
        </div>

        <div class="sx-score-row">
          <div class="sx-score-label">SCORE</div>
          <div class="sx-score-bar-wrap">
            <div class="sx-score-bar" style="width:${scorePct}%;background:${scoreColor}"></div>
          </div>
          <div class="sx-score-val" style="color:${scoreColor}">${escapeHtml(formatNumber(score, 1))}</div>
        </div>

        <div class="sx-chips-row">
          ${item.entry_price ? `<div class="sx-chip"><span class="sx-chip-lbl">ENTRADA</span><span class="sx-chip-val">${escapeHtml(formatPrice(item.entry_price, 4))}</span></div>` : ''}
          <div class="sx-chip"><span class="sx-chip-lbl">EMITIDA</span><span class="sx-chip-val">${escapeHtml(formatDate(item.created_at))}</span></div>
          ${item.telegram_valid_until ? `<div class="sx-chip"><span class="sx-chip-lbl">EXPIRA</span><span class="sx-chip-val">${escapeHtml(formatDate(item.telegram_valid_until))}</span></div>` : ''}
        </div>

        <div class="sx-actions">
          <button class="sx-btn sx-btn-intel" data-signal-detail="${escapeHtml(item.signal_id)}" data-signal-source="signals">⚡ Ver inteligencia</button>
          <button class="sx-btn sx-btn-risk" data-open-risk-signal="${escapeHtml(item.signal_id)}">⚖ Calcular riesgo</button>
        </div>
      </div>
    </div>
  `;
}

function historyCard(item, index) {
  const idx = typeof index === 'number' ? index : 0;
  const dirRaw = String(item.direction || '').toUpperCase();
  const isLong = dirRaw.includes('LONG') || dirRaw === 'BUY';
  const rVal = item.r_multiple !== null && item.r_multiple !== undefined ? Number(item.r_multiple) : null;
  const rIsNum = rVal !== null && !isNaN(rVal);
  const rPositive = rIsNum && rVal >= 0;
  const rStr = rIsNum ? (rPositive ? `+${rVal.toFixed(2)}` : rVal.toFixed(2)) : '—';
  const rBarPct = rIsNum ? Math.min(Math.abs(rVal) / 3 * 100, 100) : 0;
  const resLabel = resultLabel(item);
  const resNorm = String(item.resolution || item.result || '').toLowerCase();
  const isWin = resNorm === 'tp1' || resNorm === 'tp2' || resNorm === 'won';
  const isLoss = resNorm === 'sl' || resNorm === 'lost';
  const scoreVal = item.score !== null && item.score !== undefined ? formatNumber(item.score, 1) : '—';
  const setupStr = item.setup_group || 'setup';
  const timeStr = item.resolution_minutes != null ? `${item.resolution_minutes}m` : '—';
  const dirLabel = dirRaw === 'BUY' ? 'LONG' : dirRaw === 'SELL' ? 'SHORT' : dirRaw;
  const dirArrow = isLong ? '↗' : '↘';

  return `
    <div class="hx-card ${isLong ? 'hx-long' : 'hx-short'} ${isWin ? 'hx-win' : isLoss ? 'hx-loss' : 'hx-exp'}" style="animation-delay:${idx * 55}ms">
      <div class="hx-accent-bar"></div>
      <div class="hx-inner">
        <div class="hx-row-top">
          <div class="hx-left">
            <span class="hx-symbol">${escapeHtml(item.symbol)}</span>
            <span class="hx-dir-pill ${isLong ? 'hx-dir-long' : 'hx-dir-short'}">${dirArrow} ${escapeHtml(dirLabel)}</span>
          </div>
          <div class="hx-right">
            <span class="hx-result-badge ${isWin ? 'hx-badge-win' : isLoss ? 'hx-badge-loss' : 'hx-badge-exp'}">${escapeHtml(resLabel)}</span>
          </div>
        </div>

        <div class="hx-r-block">
          <div class="hx-r-number ${rIsNum ? (rPositive ? 'hx-r-pos' : 'hx-r-neg') : ''}">${escapeHtml(rStr)}<span class="hx-r-suffix">R</span></div>
          <div class="hx-r-track"><div class="hx-r-fill ${rPositive ? 'hx-r-fill-pos' : 'hx-r-fill-neg'}" style="width:${rBarPct}%"></div></div>
        </div>

        <div class="hx-chips">
          <div class="hx-chip">
            <span class="hx-chip-lbl">SETUP</span>
            <span class="hx-chip-val">${escapeHtml(setupStr)}</span>
          </div>
          <div class="hx-chip">
            <span class="hx-chip-lbl">SCORE</span>
            <span class="hx-chip-val">${escapeHtml(scoreVal)}</span>
          </div>
          <div class="hx-chip">
            <span class="hx-chip-lbl">TIEMPO</span>
            <span class="hx-chip-val">${escapeHtml(timeStr)}</span>
          </div>
          <div class="hx-chip">
            <span class="hx-chip-lbl">FECHA</span>
            <span class="hx-chip-val hx-chip-date">${escapeHtml(formatDate(item.signal_created_at))}</span>
          </div>
        </div>

        <div class="hx-actions">
          <button class="hx-btn hx-btn-intel" data-signal-detail="${escapeHtml(item.signal_id)}" data-signal-source="history">⚡ Inteligencia</button>
          <button class="hx-btn hx-btn-risk" data-open-risk-signal="${escapeHtml(item.signal_id)}">⚖ Riesgo</button>
        </div>
      </div>
    </div>
  `;
}

function paymentInstructions(order, focus = null) {
  if (!order) return '';
  const address = order.deposit_address || '';
  const addressHref = address ? `https://bscscan.com/address/${encodeURIComponent(address)}` : '#';
  const uniqueExtra = order.amount_unique_delta ? `(+${formatMoney(order.amount_unique_delta)} único)` : 'Monto único por orden';
  const steps = Array.isArray(order.steps) && order.steps.length ? order.steps : (focus?.steps || []);
  const toneClass = billingToneClass(focus?.tone || (order.status === 'paid_unconfirmed' ? 'positive' : order.status === 'verification_in_progress' ? 'warning' : 'accent'));
  const canConfirm = order.status === 'awaiting_payment' || order.status === 'paid_unconfirmed';
  const canCancel = order.status === 'awaiting_payment';
  return `
    <div class="card payment-card card-span-12" data-payment-active-card="true">
      <div class="payment-focus-card ${toneClass}">
        <div class="payment-focus-copy">
          <div class="payment-focus-kicker">Billing activo</div>
          <div class="payment-focus-title">${escapeHtml(focus?.title || 'Pago actual')}</div>
          <div class="payment-focus-headline">${escapeHtml(focus?.headline || `${order.plan_name || String(order.plan || '').toUpperCase()} · ${order.days} días`)}</div>
          <div class="payment-focus-message">${escapeHtml(focus?.message || 'Revisa el estado antes de enviar o volver a confirmar.')}</div>
          ${focus?.hint ? `<div class="payment-focus-hint">${escapeHtml(focus.hint)}</div>` : ''}
        </div>
        <div class="payment-focus-side">
          <span class="plan-tag">${escapeHtml(formatStatusLabel(order.status_label || order.status))}</span>
          <div class="payment-timer">${escapeHtml(order.time_left_label || '—')}</div>
          <div class="payment-timer-label">Tiempo restante</div>
        </div>
      </div>

      ${steps.length ? `<div class="billing-step-row">${steps.map(step => `<div class="billing-step ${billingStepClass(step.state)}"><span class="billing-step-dot"></span><span>${escapeHtml(step.label)}</span></div>`).join('')}</div>` : ''}

      <div class="item">
        <div class="item-header">
          <div>
            <div class="item-title">${escapeHtml(order.plan_name || String(order.plan || '').toUpperCase())} · ${escapeHtml(order.days)} días</div>
            <div class="item-subtitle">Red ${escapeHtml(String(order.network || '').toUpperCase())} · ${escapeHtml(order.token_symbol || 'USDT')} · ${escapeHtml(order.confirmations ?? 0)} confirmaciones</div>
          </div>
          <span class="plan-tag">${escapeHtml(order.time_left_label || formatDate(order.expires_at))}</span>
        </div>

        <div class="payment-grid">
          <div class="payment-box">
            <div class="payment-label">Precio base</div>
            <div class="payment-value">${escapeHtml(formatMoney(order.base_price_usdt))}</div>
          </div>
          <div class="payment-box payment-box-accent">
            <div class="payment-label">Monto exacto a enviar</div>
            <div class="payment-value">${escapeHtml(formatMoney(order.amount_usdt))}</div>
            <div class="payment-hint">${escapeHtml(uniqueExtra)}</div>
          </div>
        </div>

        <div class="payment-box payment-address-box">
          <div class="payment-label">Dirección BEP-20</div>
          <a class="wallet-link" target="_blank" rel="noopener" href="${escapeHtml(addressHref)}">${escapeHtml(address)}</a>
          <div class="action-row compact">
            <button class="button button-secondary" data-copy-value="${escapeHtml(address)}">Copiar wallet</button>
            <button class="button button-secondary" data-copy-value="${escapeHtml(formatMoney(order.amount_usdt))}">Copiar monto</button>
          </div>
        </div>

        <div class="notice-list">
          <div class="notice-item">Envía exactamente el monto indicado y desde la red correcta.</div>
          <div class="notice-item">Usa únicamente la red BEP-20.</div>
          <div class="notice-item">Expira: ${escapeHtml(formatDate(order.expires_at))}</div>
          ${order.status === 'paid_unconfirmed' ? `<div class="notice-item">El pago ya fue detectado. No reenvíes fondos; espera confirmaciones y vuelve a revisar.</div>` : ''}
          ${order.status === 'verification_in_progress' ? `<div class="notice-item">Ya hay una verificación corriendo. Evita tocar varias veces hasta que termine.</div>` : ''}
        </div>

        <div class="action-row">
          <button class="button button-success" data-confirm-order="${escapeHtml(order.order_id)}" ${canConfirm ? '' : 'disabled'}>${order.status === 'paid_unconfirmed' ? 'Revisar confirmaciones' : 'Confirmar pago'}</button>
          <button class="button button-danger" data-cancel-order="${escapeHtml(order.order_id)}" ${canCancel ? '' : 'disabled'}>Cancelar orden</button>
        </div>
      </div>
    </div>
  `;
}

function renderHome() {
  const me = state.payload.me || {};
  const dashboard = state.payload.dashboard || {};
  const summary = dashboard.home_summary || dashboard.summary_7d || {};
  const summaryLabel = dashboard.home_summary_label || '7D';
  const market = state.payload.market || {};
  const activeOrder = dashboard.active_payment_order;
  const recentSignals = dashboard.recent_signals || [];
  const recentHistory = dashboard.recent_history || [];
  const generatedAt = state.payload.generated_at;
  const diagnosis = summaryDiagnosis(summary);
  const activeCount = Number(dashboard.active_signals_count || 0);
  const ecosystemPremiumActive = !isPlanExpired();
  const ecosystemPlan = String(me.plan_name || 'FREE').toUpperCase();
  const ecosystemDays = Number(me.days_left || 0);
  const ecosystemStatusLabel = ecosystemPremiumActive ? 'PREMIUM ACTIVO' : 'PREMIUM REQUERIDO';

  // Bias utilities
  const biasRaw = String(market.bias || '').toLowerCase();
  const biasIsBull = biasRaw.includes('bull') || biasRaw.includes('long') || biasRaw.includes('buy') || biasRaw === 'alcista';
  const biasIsBear = biasRaw.includes('bear') || biasRaw.includes('short') || biasRaw.includes('sell') || biasRaw === 'bajista';
  const biasDotClass = biasIsBull ? 'bias-dot-bull' : biasIsBear ? 'bias-dot-bear' : 'bias-dot-neutral';
  const heroGradientClass = biasIsBull ? 'hero-gradient-bull' : biasIsBear ? 'hero-gradient-bear' : 'hero-gradient-neutral';

  // Metric classes
  const winRate = Number(summary.winrate || 0);
  const winRateClass = winRate >= 60 ? 'stat-positive' : winRate >= 45 ? 'stat-warning' : 'stat-negative';
  const pfInfinite = Boolean(summary.profit_factor_infinite);
  const pf = pfInfinite ? Infinity : Number(summary.profit_factor || 0);
  const pfClass = (pfInfinite || pf >= 1.5) ? 'stat-positive' : pf >= 1.0 ? 'stat-warning' : 'stat-negative';
  const exp = Number(summary.expectancy_r || 0);
  const expClass = exp > 0 ? 'stat-positive' : exp < 0 ? 'stat-negative' : 'stat-warning';
  const dd = Number(summary.max_drawdown_r || 0);
  const ddClass = dd <= 3 ? 'stat-positive' : dd <= 6 ? 'stat-warning' : 'stat-negative';

  // Scoreboard bars
  const tp1Count = Number(summary.tp1 || 0);
  const tp2Count = Number(summary.tp2 || 0);
  const slCount = Number(summary.sl || 0);
  const expCount = Number(summary.expired || 0);
  const barTotal = Math.max(tp1Count + tp2Count + slCount + expCount, 1);
  const tp1Pct = Math.round(tp1Count / barTotal * 100);
  const tp2Pct = Math.round(tp2Count / barTotal * 100);
  const slPct = Math.round(slCount / barTotal * 100);
  const expPct = Math.round(expCount / barTotal * 100);

  els.home.innerHTML = `
    <div class="section-grid">

      <!-- ① COMMAND HEADER -->
      <div class="card card-span-12 cmd-header ${heroGradientClass}">
        <div class="cmd-topbar">
          <div class="cmd-market-pulse">
            <span class="bias-dot ${biasDotClass}"></span>
            <span class="cmd-bias-label">${escapeHtml(market.bias || 'Sin bias')}</span>
            ${market.regime ? `<span class="cmd-regime-badge">${escapeHtml(market.regime)}</span>` : ''}
          </div>
          <div class="cmd-plan-chip">
            <span class="cmd-plan-name">${escapeHtml(me.plan_name || 'FREE')}</span>
            <span class="cmd-days">${escapeHtml(String(me.days_left || 0))}d</span>
          </div>
        </div>

        <div class="cmd-center">
          <div class="cmd-signals-count ${activeCount > 0 ? 'cmd-signals-live' : 'cmd-signals-idle'}">
            ${activeCount > 0 ? '<span class="cmd-live-dot"></span>' : ''}
            <span class="cmd-signals-number">${escapeHtml(String(activeCount))}</span>
            <span class="cmd-signals-label">señales activas</span>
          </div>
        </div>

        <div class="cmd-metrics-strip">
          <div class="cmd-metric">
            <span class="cmd-metric-value ${winRateClass}">${escapeHtml(formatNumber(winRate))}%</span>
            <span class="cmd-metric-label">Win Rate ${escapeHtml(summaryLabel)}</span>
          </div>
          <div class="cmd-metric-sep"></div>
          <div class="cmd-metric">
            <span class="cmd-metric-value ${pfClass}">${escapeHtml(formatRatioValue(summary.profit_factor, pfInfinite))}</span>
            <span class="cmd-metric-label">Profit Factor</span>
          </div>
          <div class="cmd-metric-sep"></div>
          <div class="cmd-metric">
            <span class="cmd-metric-value ${expClass}">${exp >= 0 ? '+' : ''}${escapeHtml(formatNumber(exp))}</span>
            <span class="cmd-metric-label">Expectancy R</span>
          </div>
          <div class="cmd-metric-sep"></div>
          <div class="cmd-metric">
            <span class="cmd-metric-value ${ddClass}">${escapeHtml(formatNumber(dd))}</span>
            <span class="cmd-metric-label">Max DD (R)</span>
          </div>
        </div>
      </div>

      <!-- ② LIVE SIGNAL ALERT BANNER -->
      ${activeCount > 0 ? `
        <div class="card card-span-12 live-alert-banner" data-goto="signals">
          <span class="live-alert-dot"></span>
          <span class="live-alert-text">${escapeHtml(String(activeCount))} señal${activeCount !== 1 ? 'es' : ''} activa${activeCount !== 1 ? 's' : ''} en este momento</span>
          <span class="live-alert-cta">Ver ahora →</span>
        </div>
      ` : ''}

      <!-- ③ DIAGNÓSTICO DEL SISTEMA -->
      <div class="card card-span-12 diagnosis-card ${escapeHtml(diagnosis.tone)}">
        <div class="diagnosis-icon">${diagnosis.tone === 'diagnostic-positive' ? '▲' : diagnosis.tone === 'diagnostic-negative' ? '▼' : '◆'}</div>
        <div class="diagnosis-body">
          <div class="diagnosis-title">${escapeHtml(diagnosis.title)}</div>
          <div class="diagnosis-text">${escapeHtml(diagnosis.text)}</div>
        </div>
      </div>

      <!-- ④ ESTADO DE MERCADO -->
      <div class="card card-span-12 regime-card">
        <div class="regime-inner">
          <div class="regime-dot-block">
            <span class="bias-dot ${biasDotClass} bias-dot-lg"></span>
            <div>
              <div class="regime-eyebrow">ESTADO DE MERCADO</div>
              <div class="regime-value">${escapeHtml(market.regime || market.bias || 'Sin lectura')}</div>
            </div>
          </div>
          <div class="regime-chips">
            ${market.volatility ? `<span class="regime-chip">${escapeHtml(market.volatility)}</span>` : ''}
            ${market.environment ? `<span class="regime-chip">${escapeHtml(market.environment)}</span>` : ''}
            ${market.bias ? `<span class="regime-chip regime-chip-bias ${biasIsBull ? 'regime-chip-bull' : biasIsBear ? 'regime-chip-bear' : 'regime-chip-neutral'}">${escapeHtml(market.bias)}</span>` : ''}
          </div>
        </div>
        ${market.recommendation ? `<p class="regime-recommendation">${escapeHtml(market.recommendation)}</p>` : ''}
      </div>

      <!-- ⑤ SCOREBOARD TP1 / TP2 / SL -->
      <div class="card card-span-12 scoreboard-card">
        <div class="scoreboard-header">
          <div>
            <div class="scoreboard-eyebrow">RESOLUCIÓN ${escapeHtml(summaryLabel)}</div>
            <div class="scoreboard-resolved">${escapeHtml(String(summary.resolved ?? 0))} resueltas · ${escapeHtml(String(summary.total ?? 0))} evaluadas</div>
          </div>
          <div class="scoreboard-wr-badge ${winRateClass}">${escapeHtml(formatNumber(winRate))}%</div>
        </div>

        <div class="scoreboard-grid">
          <div class="scoreboard-row">
            <span class="scoreboard-row-label sb-label-tp">TP1</span>
            <div class="scoreboard-bar-track">
              <div class="scoreboard-bar sb-bar-tp1" style="width:${tp1Pct}%"></div>
            </div>
            <span class="scoreboard-row-count">${escapeHtml(String(tp1Count))}</span>
          </div>
          <div class="scoreboard-row">
            <span class="scoreboard-row-label sb-label-tp">TP2</span>
            <div class="scoreboard-bar-track">
              <div class="scoreboard-bar sb-bar-tp2" style="width:${tp2Pct}%"></div>
            </div>
            <span class="scoreboard-row-count">${escapeHtml(String(tp2Count))}</span>
          </div>
          <div class="scoreboard-row">
            <span class="scoreboard-row-label sb-label-sl">SL</span>
            <div class="scoreboard-bar-track">
              <div class="scoreboard-bar sb-bar-sl" style="width:${slPct}%"></div>
            </div>
            <span class="scoreboard-row-count">${escapeHtml(String(slCount))}</span>
          </div>
          ${expCount > 0 ? `
          <div class="scoreboard-row">
            <span class="scoreboard-row-label sb-label-exp">EXP</span>
            <div class="scoreboard-bar-track">
              <div class="scoreboard-bar sb-bar-exp" style="width:${expPct}%"></div>
            </div>
            <span class="scoreboard-row-count">${escapeHtml(String(expCount))}</span>
          </div>` : ''}
        </div>
      </div>

      <!-- ⑥ SEÑALES RECIENTES -->
      <div class="card card-span-12">
        <div class="feed-header">
          <h2>Señales recientes</h2>
          <button class="button button-secondary feed-see-all" data-goto="signals">Ver todas →</button>
        </div>
        <div class="feed-list">
          ${recentSignals.length
            ? recentSignals.slice(0, 3).map(item => signalFeedCard(item)).join('')
            : '<div class="empty-state">No hay señales recientes todavía.</div>'}
        </div>
      </div>

      <!-- ⑦ HISTORIAL RECIENTE -->
      <div class="card card-span-12">
        <div class="feed-header">
          <h2>Historial reciente</h2>
          <button class="button button-secondary feed-see-all" data-goto="history">Ver todo →</button>
        </div>
        <div class="feed-list">
          ${recentHistory.length
            ? recentHistory.slice(0, 3).map(item => historyFeedCard(item)).join('')
            : '<div class="empty-state">No hay histórico reciente todavía.</div>'}
        </div>
      </div>

      <!-- ⑧ RENDIMIENTO 30D TEASER -->
      <div class="card card-span-12 perf-teaser-card">
        <div class="perf-teaser-header">
          <div>
            <div class="perf-teaser-eyebrow">MÓDULO DE RENDIMIENTO</div>
            <h2 style="margin:0">Rendimiento 30D</h2>
          </div>
          <button class="button button-primary" data-open-performance-center="true">Abrir</button>
        </div>
        <div class="perf-teaser-grid">
          <div class="perf-teaser-stat ${metricToneClass('pf', dashboard.summary_30d?.profit_factor || 0)}">
            <div class="perf-teaser-val">${escapeHtml(formatNumber(dashboard.summary_30d?.profit_factor || 0))}</div>
            <div class="perf-teaser-label">Profit Factor</div>
          </div>
          <div class="perf-teaser-stat ${metricToneClass('expectancy', dashboard.summary_30d?.expectancy_r || 0)}">
            <div class="perf-teaser-val">${Number(dashboard.summary_30d?.expectancy_r || 0) >= 0 ? '+' : ''}${escapeHtml(formatNumber(dashboard.summary_30d?.expectancy_r || 0, 4))}</div>
            <div class="perf-teaser-label">Expectancy R</div>
          </div>
          <div class="perf-teaser-stat ${metricToneClass('winrate', dashboard.summary_30d?.winrate || 0)}">
            <div class="perf-teaser-val">${escapeHtml(formatNumber(dashboard.summary_30d?.winrate || 0))}%</div>
            <div class="perf-teaser-label">Win Rate</div>
          </div>
          <div class="perf-teaser-stat">
            <div class="perf-teaser-val">${escapeHtml(String(dashboard.summary_30d?.resolved || 0))}</div>
            <div class="perf-teaser-label">Resueltas</div>
          </div>
        </div>
      </div>


      <!-- ⑨ ECOSISTEMA HADES -->
      <div class="card card-span-12 ecosystem-command-card">
        <div class="ecosystem-command-head">
          <div>
            <div class="ecosystem-kicker">ECOSISTEMA HADES</div>
            <h2>Centro de mando premium</h2>
            <p>HADES Alpha organiza la operación. Oraculum filtra oportunidades predictivas. HADES Sentinel valida el riesgo antes de operar.</p>
          </div>
          <span class="ecosystem-status ${ecosystemPremiumActive ? 'active' : 'locked'}">${ecosystemStatusLabel}</span>
        </div>

        <div class="ecosystem-suite-grid">
          <article class="ecosystem-suite-card alpha">
            <div class="ecosystem-suite-icon">⚔️</div>
            <div class="ecosystem-suite-body">
              <strong>HADES Alpha</strong>
              <span>Centro principal</span>
              <p>Señales, radar, mercado, historial, rendimiento, gestión de riesgo, cuenta y acceso premium.</p>
            </div>
            <button class="ecosystem-suite-action" data-goto="signals">Ver señales</button>
          </article>

          <article class="ecosystem-suite-card oraculum ${ecosystemPremiumActive ? 'is-active' : 'is-locked'}">
            <div class="ecosystem-suite-icon">🔮</div>
            <div class="ecosystem-suite-body">
              <strong>Oraculum</strong>
              <span>Predicción accionable</span>
              <p>Motor predictivo con señales LONG/SHORT, entrada fija, objetivo y filtros de coherencia.</p>
            </div>
            <button class="ecosystem-suite-action" data-open-oraculum ${ecosystemPremiumActive ? '' : 'disabled'}>Abrir Oraculum</button>
          </article>

          <article class="ecosystem-suite-card sentinel ${ecosystemPremiumActive ? 'is-active' : 'is-locked'}">
            <div class="ecosystem-suite-icon">🛡️</div>
            <div class="ecosystem-suite-body">
              <strong>HADES Sentinel</strong>
              <span>Defensa y riesgo</span>
              <p>Riesgo operativo, anomalías, estrés de futuros, presión, noticias críticas y alertas internas.</p>
            </div>
            <button class="ecosystem-suite-action" data-open-sentinel ${ecosystemPremiumActive ? '' : 'disabled'}>Abrir Sentinel</button>
          </article>
        </div>

        <div class="pretrade-card">
          <div class="pretrade-orb">◎</div>
          <div class="pretrade-copy">
            <div class="ecosystem-kicker">ANTES DE OPERAR</div>
            <h3>Flujo recomendado</h3>
            <p>1) Revisa señales activas en HADES. 2) Consulta si Oraculum tiene oportunidad filtrada. 3) Valida en Sentinel si el mercado está limpio o cargado de riesgo.</p>
            <div class="pill-row compact-pill-row">
              <span class="pill">Plan: ${escapeHtml(ecosystemPlan)}</span>
              <span class="pill">Días premium: ${escapeHtml(ecosystemDays)}</span>
              <span class="pill">Señales activas: ${escapeHtml(activeCount)}</span>
            </div>
          </div>
          <div class="pretrade-actions">
            <button class="button button-secondary" data-goto="signals">1. Señales</button>
            <button class="button button-secondary" data-open-oraculum ${ecosystemPremiumActive ? '' : 'disabled'}>2. Oraculum</button>
            <button class="button button-primary" data-open-sentinel ${ecosystemPremiumActive ? '' : 'disabled'}>3. Sentinel</button>
          </div>
        </div>
      </div>


      <!-- ⑨ ACCIONES RÁPIDAS -->
      <div class="card card-span-12 quick-actions-card">
        <div class="quick-actions-eyebrow">ACCIONES RÁPIDAS</div>
        <div class="quick-actions-grid">
          <button class="qa-btn qa-btn-primary" data-goto="signals">
            <span class="qa-icon">⚡</span>
            <span class="qa-label">Señales</span>
          </button>
          <button class="qa-btn" data-goto="market">
            <span class="qa-icon">📊</span>
            <span class="qa-label">Mercado</span>
          </button>
          <button class="qa-btn" data-goto="history">
            <span class="qa-icon">📋</span>
            <span class="qa-label">Historial</span>
          </button>
          <button class="qa-btn" data-open-performance-center="true">
            <span class="qa-icon">🏆</span>
            <span class="qa-label">Rendimiento</span>
          </button>
          <button class="qa-btn" data-goto="account">
            <span class="qa-icon">👤</span>
            <span class="qa-label">Cuenta</span>
          </button>
        </div>
      </div>

      ${activeOrder ? paymentInstructions(activeOrder) : ''}
    </div>
  `;
}

function signalFeedCard(item) {
  const dirRaw = String(item.direction || '').toLowerCase();
  const isLong = dirRaw.includes('long') || dirRaw === 'buy';
  const borderClass = isLong ? 'feed-item-long' : 'feed-item-short';
  const statusBadge = item.result
    ? `<span class="${badgeClassByResult(item)}">${escapeHtml(resultLabel(item))}</span>`
    : `<span class="plan-tag">${escapeHtml(formatStatusLabel(item.status || 'active'))}</span>`;
  return `
    <div class="feed-item ${borderClass}">
      <div class="feed-item-top">
        <div class="feed-item-left">
          <span class="feed-symbol">${escapeHtml(item.symbol)}</span>
          <span class="${dirClass(item.direction)} feed-dir-badge">${escapeHtml(item.direction)}</span>
        </div>
        ${statusBadge}
      </div>
      <div class="feed-levels">
        ${item.entry_price ? `<span class="feed-level"><span class="feed-level-lbl">ENTRY</span><span class="feed-level-val">${escapeHtml(formatPrice(item.entry_price, 4))}</span></span>` : ''}
        <span class="feed-level"><span class="feed-level-lbl">SCORE</span><span class="feed-level-val">${escapeHtml(formatNumber(item.score || 0, 1))}</span></span>
        <span class="feed-level"><span class="feed-level-lbl">TIER</span><span class="feed-level-val">${escapeHtml(String(item.visibility || '').toUpperCase())}</span></span>
      </div>
      <div class="feed-item-footer">
        <span class="feed-date">${escapeHtml(formatDate(item.created_at))}</span>
        <div class="feed-actions">
          <button class="button button-secondary feed-btn" data-signal-detail="${escapeHtml(item.signal_id)}" data-signal-source="signals">Inteligencia</button>
          <button class="button button-secondary feed-btn" data-open-risk-signal="${escapeHtml(item.signal_id)}">Riesgo</button>
        </div>
      </div>
    </div>
  `;
}

function historyFeedCard(item) {
  const dirRaw = String(item.direction || '').toLowerCase();
  const isLong = dirRaw.includes('long') || dirRaw === 'buy';
  const borderClass = isLong ? 'feed-item-long' : 'feed-item-short';
  const rVal = item.r_multiple !== null && item.r_multiple !== undefined ? Number(item.r_multiple) : null;
  return `
    <div class="feed-item ${borderClass}">
      <div class="feed-item-top">
        <div class="feed-item-left">
          <span class="feed-symbol">${escapeHtml(item.symbol)}</span>
          <span class="${dirClass(item.direction)} feed-dir-badge">${escapeHtml(item.direction)}</span>
        </div>
        <span class="${badgeClassByResult(item)}">${escapeHtml(resultLabel(item))}</span>
      </div>
      <div class="feed-levels">
        ${rVal !== null ? `<span class="feed-level"><span class="feed-level-lbl">R</span><span class="feed-level-val ${rVal >= 0 ? 'stat-positive' : 'stat-negative'}">${rVal >= 0 ? '+' : ''}${escapeHtml(String(item.r_multiple))}</span></span>` : ''}
        ${item.resolution_minutes ? `<span class="feed-level"><span class="feed-level-lbl">TIEMPO</span><span class="feed-level-val">${escapeHtml(String(item.resolution_minutes))}m</span></span>` : ''}
        <span class="feed-level"><span class="feed-level-lbl">SCORE</span><span class="feed-level-val">${escapeHtml(formatNumber(item.score || 0, 1))}</span></span>
      </div>
      <div class="feed-item-footer">
        <span class="feed-date">${escapeHtml(formatDate(item.signal_created_at))}</span>
        <div class="feed-actions">
          <button class="button button-secondary feed-btn" data-signal-detail="${escapeHtml(item.signal_id)}" data-signal-source="history">Inteligencia</button>
        </div>
      </div>
    </div>
  `;
}

function renderSignals() {
  const signals = state.payload.signals || [];
  const counts = signals.reduce((acc, item) => {
    const visibility = String(item.visibility || '').toLowerCase();
    if (visibility === 'premium') acc.premium += 1;
    else if (visibility === 'plus') acc.plus += 1;
    else acc.free += 1;
    if (!item.result && String(item.status || '').toLowerCase() === 'active') acc.active += 1;
    if (item.result) acc.closed += 1;
    return acc;
  }, { free: 0, plus: 0, premium: 0, active: 0, closed: 0 });

  const hasActive = counts.active > 0;

  els.signals.innerHTML = `
    <div class="sx-view">

      <!-- HERO HEADER -->
      <div class="sx-hero ${hasActive ? 'sx-hero-live' : ''}">
        <canvas class="sx-hero-canvas" id="sxParticleCanvas"></canvas>
        <div class="sx-hero-content">
          <div class="sx-hero-eyebrow">
            ${hasActive ? '<span class="sx-live-ring"></span> SEÑALES EN VIVO' : '◈ RADAR DE SEÑALES'}
          </div>
          <div class="sx-hero-big-number">${escapeHtml(String(counts.active))}</div>
          <div class="sx-hero-subtitle">${hasActive ? `Señal${counts.active !== 1 ? 'es' : ''} activa${counts.active !== 1 ? 's' : ''} ahora mismo` : 'Sin señales activas en este momento'}</div>
          <div class="sx-hero-stats-row">
            <div class="sx-hero-stat">
              <span class="sx-hero-stat-val">${escapeHtml(String(signals.length))}</span>
              <span class="sx-hero-stat-lbl">Recientes</span>
            </div>
            <div class="sx-hero-stat-sep"></div>
            <div class="sx-hero-stat">
              <span class="sx-hero-stat-val sx-premium-color">${escapeHtml(String(counts.premium))}</span>
              <span class="sx-hero-stat-lbl">Premium</span>
            </div>
            <div class="sx-hero-stat-sep"></div>
            <div class="sx-hero-stat">
              <span class="sx-hero-stat-val sx-muted-color">${escapeHtml(String(counts.closed))}</span>
              <span class="sx-hero-stat-lbl">Cerradas</span>
            </div>
          </div>
        </div>
        <div class="sx-hero-tier-bar">
          <div class="sx-tier-track sx-tier-free-track" style="flex:${counts.free || 1}" title="Free: ${counts.free}"></div>
          <div class="sx-tier-track sx-tier-plus-track" style="flex:${counts.plus || 0}" title="Plus: ${counts.plus}"></div>
          <div class="sx-tier-track sx-tier-premium-track" style="flex:${counts.premium || 0}" title="Premium: ${counts.premium}"></div>
        </div>
      </div>

      <!-- GRID DE SEÑALES -->
      <div class="sx-grid">
        ${signals.length
          ? signals.map((s, i) => signalCard(s, i)).join('')
          : `<div class="sx-empty">
              <div class="sx-empty-icon">◈</div>
              <div class="sx-empty-title">Sin señales disponibles</div>
              <div class="sx-empty-sub">El sistema emitirá una alerta cuando se detecte una oportunidad.</div>
            </div>`
        }
      </div>
    </div>
  `;

  // Init particle canvas
  initSignalsParticles();
}

function initSignalsParticles() {
  const canvas = document.getElementById('sxParticleCanvas');
  if (!canvas) return;
  const ctx = canvas.getContext('2d');
  let W = canvas.offsetWidth;
  let H = canvas.offsetHeight;
  canvas.width = W;
  canvas.height = H;

  const PARTICLE_COUNT = 38;
  const particles = Array.from({ length: PARTICLE_COUNT }, () => ({
    x: Math.random() * W,
    y: Math.random() * H,
    r: Math.random() * 1.6 + 0.4,
    dx: (Math.random() - 0.5) * 0.35,
    dy: (Math.random() - 0.5) * 0.35,
    alpha: Math.random() * 0.5 + 0.1,
    color: Math.random() > 0.6 ? '#f59e0b' : Math.random() > 0.5 ? '#22c55e' : '#a78bfa',
  }));

  let rafId;
  function tick() {
    ctx.clearRect(0, 0, W, H);
    for (const p of particles) {
      p.x += p.dx;
      p.y += p.dy;
      if (p.x < 0) p.x = W;
      if (p.x > W) p.x = 0;
      if (p.y < 0) p.y = H;
      if (p.y > H) p.y = 0;
      ctx.beginPath();
      ctx.arc(p.x, p.y, p.r, 0, Math.PI * 2);
      ctx.fillStyle = p.color;
      ctx.globalAlpha = p.alpha;
      ctx.fill();
    }
    ctx.globalAlpha = 1;
    rafId = requestAnimationFrame(tick);
  }

  // Stop old animation if any
  if (window._sxParticleRaf) cancelAnimationFrame(window._sxParticleRaf);
  window._sxParticleRaf = rafId;
  tick();
  window._sxParticleRaf = rafId;
}

function renderMarket() {
  // Bloquear vista de Mercado si el plan está vencido o el usuario no tiene suscripción
  if (isPlanExpired()) {
    const me = state.payload?.me || state.authMe || {};
    const status = String(me.subscription_status || '').toLowerCase();
    const isExpired = status === 'expired';
    els.market.innerHTML = `
      <div class="plan-expired-gate">
        <div class="peg-icon">${isExpired ? '⏰' : '🔒'}</div>
        <div class="peg-title">${isExpired ? 'Tu plan ha vencido' : 'Acceso restringido'}</div>
        <p class="peg-desc">${isExpired
          ? 'Tu suscripción ha expirado. Adquiere o renueva un plan para volver a acceder al Mercado y todas sus funcionalidades.'
          : 'Necesitas un plan activo para acceder al Mercado. Elige el plan que mejor se adapte a ti.'
        }</p>
        <div class="peg-actions">
          <button class="button button-primary peg-btn-plans" data-billing-focus-action="open-plans">💼 Ver planes</button>
        </div>
      </div>
    `;
    bindViewButtons();
    return;
  }
  const market = state.payload.market || {};
  const marketLoading = Boolean(state.lazy.market?.loading);
  const marketError = state.lazy.market?.error || null;
  const watchlist = state.payload.watchlist || [];
  const watchlistMeta = state.payload.watchlist_meta || { symbols: [], symbols_count: 0, max_symbols: 0, slots_left: 0, can_add_more: false };
  const gainers = market.top_gainers || [];
  const losers = market.top_losers || [];
  const radar = market.radar || [];
  const topVolume = market.top_volume || [];
  const btc = market.btc || {};
  const eth = market.eth || {};
  const radarView = state.radarView || { ...DEFAULT_RADAR_VIEW };
  const visibleRadar = getRadarPresentation(radar, radarView);
  const radarWindow = getRadarWindow(visibleRadar, radarView.offset, RADAR_VISIBLE_COUNT);
  const radarCards = radarWindow.items;
  const radarSummary = market.radar_summary || {};
  if (radarWindow.offset !== Number(radarView.offset || 0)) {
    state.radarView = { ...radarView, offset: radarWindow.offset };
  }
  const watchlistSymbols = new Set((watchlistMeta.symbols || []).map(item => String(item || '').toUpperCase()));

  // Heatmap intensity level (1–5) based on absolute % move
  function heatIntensity(pct) {
    const abs = Math.abs(Number(pct) || 0);
    if (abs >= 8)   return 5;
    if (abs >= 5)   return 4;
    if (abs >= 3)   return 3;
    if (abs >= 1.5) return 2;
    return 1;
  }

  // Heatmap grid (gainers or losers)
  const heatmapGrid = (items, type) => items.length
    ? `<div class="heat-grid">${items.slice(0, 10).map(item => {
        const base = item.symbol.replace(/USDT$/, '');
        const pct  = Number(item.change || 0);
        const lvl  = heatIntensity(pct);
        return `<div class="heat-chip heat-chip-${type}-${lvl}" data-live-symbol="${escapeHtml(item.symbol)}" title="${escapeHtml(base)}: ${escapeHtml(formatPercentSigned(pct, 2))}">
          <span class="heat-chip-sym">${escapeHtml(base)}</span>
          <span class="heat-chip-pct" data-live-change="${escapeHtml(item.symbol)}">${escapeHtml(formatPercentSigned(pct, 2))}</span>
        </div>`;
      }).join('')}</div>`
    : `<div class="empty-state">${marketLoading ? 'Actualizando...' : (marketError ? 'Error de datos.' : 'Sin datos.')}</div>`;

  // Filter chip row builder
  function filterChips(filterKey, options) {
    const current = radarView[filterKey] || 'all';
    return `<div class="radar-chip-row">${options.map(([value, label]) =>
      `<button class="radar-filter-chip${current === value ? ' active' : ''}" data-radar-chip data-filter="${escapeHtml(filterKey)}" data-value="${escapeHtml(value)}">${escapeHtml(label)}</button>`
    ).join('')}</div>`;
  }

  // Filter chip option sets
  const dirOpts = [
    ['all',   `Todas (${radar.length})`],
    ['LONG',  `Long (${radarSummary.longs  ?? radarFilterCount(radar, i => i.direction === 'LONG')})`],
    ['SHORT', `Short (${radarSummary.shorts ?? radarFilterCount(radar, i => i.direction === 'SHORT')})`],
  ];
  const prioOpts = [
    ['all',        'Todas'],
    ['Máxima',     `Máxima (${radarSummary.priority_mix?.maxima    ?? radarFilterCount(radar, i => i.priority_label === 'Máxima')})`],
    ['Alta',       `Alta (${radarSummary.priority_mix?.alta        ?? radarFilterCount(radar, i => i.priority_label === 'Alta')})`],
    ['Media',      `Media (${radarSummary.priority_mix?.media      ?? radarFilterCount(radar, i => i.priority_label === 'Media')})`],
    ['Vigilancia', `Vigilancia`],
  ];
  const proxOpts = [
    ['all',         'Todas'],
    ['Activa',      `Activa (${radarSummary.proximity_mix?.activa       ?? radarFilterCount(radar, i => i.proximity_label === 'Activa')})`],
    ['Inmediata',   `Inmediata (${radarSummary.proximity_mix?.inmediata ?? radarFilterCount(radar, i => i.proximity_label === 'Inmediata')})`],
    ['Cercana',     `Cercana (${radarSummary.proximity_mix?.cercana     ?? radarFilterCount(radar, i => i.proximity_label === 'Cercana')})`],
    ['Preparando',  `Preparando`],
  ];
  const execOpts = [
    ['all',          'Todos'],
    ['Ejecutable',   `Ejecutable (${radarSummary.execution_mix?.ejecutable  ?? radarFilterCount(radar, i => i.execution_state_label === 'Ejecutable')})`],
    ['Seguimiento',  `Seguimiento`],
    ['Preparación',  `Preparación`],
    ['Observación',  `Observación`],
  ];
  const sortOpts = [
    ['ranking',   'Ranking'],
    ['execution', 'Estado'],
    ['priority',  'Prioridad'],
    ['proximity', 'Proximidad'],
    ['score',     'Score'],
    ['volume',    'Volumen'],
    ['change',    'Movimiento'],
  ];

  // Compact radar card with expand/collapse
  function radarCardV2(item) {
    const inWatchlist = watchlistSymbols.has(String(item.symbol || '').toUpperCase());
    const changePct   = Number(item.change_pct || 0);
    const funding     = Number(item.funding_rate_pct || 0);
    return `
      <div class="radar-card-v2 compact-item watchlist-item-card" data-radar-expanded="false">
        <div class="radar-card-header" data-radar-toggle>
          <div class="radar-card-title-block">
            <div class="radar-card-symbol-row">
              <span class="item-title">${escapeHtml(item.symbol)}</span>
              <span class="${dirClass(item.direction)} radar-dir-badge">${escapeHtml(item.direction || '—')}</span>
            </div>
            <div class="radar-card-note">${escapeHtml(item.operator_note || item.action_label || 'Sin gatillo operativo claro')}</div>
          </div>
          <div class="radar-card-score-block">
            <div class="radar-score-big">${escapeHtml(formatNumber(item.final_score, 0))}</div>
            <div class="radar-score-label">Score</div>
          </div>
        </div>
        <div class="radar-card-metrics">
          <div class="radar-metric-pill ${radarExecutionClass(item.execution_state_label)}">
            <span class="rmp-label">Estado</span>
            <span class="rmp-value">${escapeHtml(item.execution_state_label || 'Observación')}</span>
          </div>
          <div class="radar-metric-pill ${watchlistPriorityClass(item.priority_label)}">
            <span class="rmp-label">Prioridad</span>
            <span class="rmp-value">${escapeHtml(item.priority_label || '—')}</span>
          </div>
          <div class="radar-metric-pill">
            <span class="rmp-label">Funding</span>
            <span class="rmp-value ${sideClassByValue(funding)}">${escapeHtml(formatPercentSigned(funding, 3))}</span>
          </div>
          <div class="radar-metric-pill">
            <span class="rmp-label">24h</span>
            <span class="rmp-value ${sideClassByValue(changePct)}">${escapeHtml(formatPercentSigned(changePct, 2))}</span>
          </div>
        </div>
        <div class="radar-card-expanded-content" style="display:none;">
          <div class="pill-row compact-pill-row watchlist-priority-row radar-pill-row" style="margin:8px 0 4px;">
            <span class="watchlist-priority-pill ${radarAlignmentClass(item.alignment_label)}">${escapeHtml(item.alignment_label || 'Selectivo')}</span>
            <span class="watchlist-priority-pill ${watchlistProximityClass(item.proximity_label)}">Prox: ${escapeHtml(item.proximity_label || '—')}</span>
            <span class="watchlist-priority-pill ${radarRiskClass(item.risk_label)}">Riesgo ${escapeHtml(item.risk_label || '—')}</span>
            <span class="watchlist-priority-pill">${escapeHtml(item.conviction_label || '—')}</span>
          </div>
          <div class="watchlist-metric-grid radar-metric-grid">
            <div class="watchlist-metric-box">
              <span class="watchlist-metric-label">Ranking</span>
              <span class="watchlist-metric-value">${escapeHtml(formatNumber(item.ranking_score, 1))}</span>
            </div>
            <div class="watchlist-metric-box">
              <span class="watchlist-metric-label">Setup</span>
              <span class="watchlist-metric-value">${escapeHtml(item.setup_mode_label || '—')}</span>
            </div>
            <div class="watchlist-metric-box">
              <span class="watchlist-metric-label">Posición rango</span>
              <span class="watchlist-metric-value">${escapeHtml(watchlistRangePosition(item.range_position_pct))}</span>
            </div>
            <div class="watchlist-metric-box">
              <span class="watchlist-metric-label">Open Interest</span>
              <span class="watchlist-metric-value">${escapeHtml(formatCompactAmount(item.open_interest))}</span>
            </div>
            <div class="watchlist-metric-box">
              <span class="watchlist-metric-label">Volumen 24h</span>
              <span class="watchlist-metric-value">${escapeHtml(formatCompactAmount(item.quote_volume))}</span>
            </div>
            <div class="watchlist-metric-box">
              <span class="watchlist-metric-label">Última señal</span>
              <span class="watchlist-metric-value">${escapeHtml(watchlistSignalSummary(item.latest_signal))}</span>
            </div>
          </div>
          ${(item.trade_plan || []).length ? `<div class="radar-plan-list">${item.trade_plan.map(step => `<div class="radar-plan-item">${escapeHtml(step)}</div>`).join('')}</div>` : ''}
          ${(item.reasons || []).length ? `<div class="watchlist-reason-list radar-reason-list">${item.reasons.map(r => `<span class="watchlist-reason-chip">${escapeHtml(r)}</span>`).join('')}</div>` : ''}
          <div class="inline-meta" style="margin-top:8px;">
            <span>Precio: ${escapeHtml(formatPrice(item.last_price, 4))}</span>
            <span>Trades: ${escapeHtml(formatInteger(item.trade_count))}</span>
            <span>Momentum: ${escapeHtml(item.momentum || '—')}</span>
          </div>
        </div>
        <div class="action-row compact watchlist-card-actions radar-card-actions" style="margin-top:10px;">
          <button class="button button-secondary radar-card-expand-btn" data-radar-toggle>Expandir</button>
          <button class="button button-secondary" data-radar-detail="${escapeHtml(item.symbol)}">Radar táctico</button>
          ${item.latest_signal?.signal_id ? `<button class="button button-secondary" data-signal-detail="${escapeHtml(item.latest_signal.signal_id)}" data-signal-source="radar">Inteligencia</button>` : ''}
          ${inWatchlist
            ? `<button class="button button-secondary" disabled>En watchlist</button>`
            : `<button class="button button-primary" data-radar-follow="${escapeHtml(item.symbol)}">Seguir</button>`}
        </div>
      </div>`;
  }

  // Top volume chips for ticker strip
  const topVolChips = topVolume.slice(0, 1).map(item => {
    const base = item.symbol.replace(/USDT$/, '');
    return `<span class="ticker-vol-chip"><span class="ticker-vol-sym">${escapeHtml(base)}</span><span class="ticker-vol-num">${escapeHtml(formatCompactAmount(item.quote_volume))}</span></span>`;
  }).join('');

  els.market.innerHTML = `
    <div class="section-grid">

      <!-- ── TICKER STRIP: BTC / ETH / Pulso ───────────────────────────── -->
      <div class="card card-span-12 market-ticker-card">
        <div class="market-ticker-strip">

          <div class="ticker-asset live-ticker-item" data-live-symbol="BTCUSDT">
            <img class="ticker-logo" src="https://cdn.jsdelivr.net/gh/spothq/cryptocurrency-icons/svg/color/btc.svg" alt="BTC" />
            <div class="ticker-asset-info">
              <span class="ticker-sym">BTC</span>
              <span class="ticker-price" data-live-price="BTCUSDT">${escapeHtml(formatPrice(btc.last_price || 0, 0))}</span>
              <span class="ticker-change ${sideClassByValue(btc.change)}" data-live-change="BTCUSDT">${escapeHtml(formatPercentSigned(btc.change, 2))}</span>
              <span class="ticker-meta">F:${escapeHtml(formatPercentSigned(btc.funding_rate_pct, 3))}</span>
            </div>
          </div>

          <div class="ticker-sep"></div>

          <div class="ticker-asset live-ticker-item" data-live-symbol="ETHUSDT">
            <img class="ticker-logo" src="https://cdn.jsdelivr.net/gh/spothq/cryptocurrency-icons/svg/color/eth.svg" alt="ETH" />
            <div class="ticker-asset-info">
              <span class="ticker-sym">ETH</span>
              <span class="ticker-price" data-live-price="ETHUSDT">${escapeHtml(formatPrice(eth.last_price || 0, 0))}</span>
              <span class="ticker-change ${sideClassByValue(eth.change)}" data-live-change="ETHUSDT">${escapeHtml(formatPercentSigned(eth.change, 2))}</span>
              <span class="ticker-meta">F:${escapeHtml(formatPercentSigned(eth.funding_rate_pct, 3))}</span>
            </div>
          </div>

          <div class="ticker-sep"></div>

          <div class="ticker-pulse-block">
            <span class="live-dot"></span>
            <div class="ticker-pulse-info">
              <span class="ticker-bias">${escapeHtml(market.bias || 'Neutral')} · ${escapeHtml(market.preferred_side || 'Selectivo')}</span>
              <span class="ticker-adv">Adv ${escapeHtml(formatNumber(market.adv_ratio_pct || 0, 1))}%</span>
            </div>
          </div>

          ${topVolume.length ? `<div class="ticker-sep"></div><div class="ticker-vol-block"><span class="ticker-vol-label">VOL</span>${topVolChips}</div>` : ''}

          <button class="button button-secondary ticker-refresh-btn" data-market-refresh style="margin-left:auto;flex-shrink:0;">${marketLoading ? '…' : '↻'}</button>
        </div>
      </div>

      <!-- ── GRÁFICA TRADINGVIEW ─────────────────────────────────────────── -->
      <div class="card card-span-12 chart-card" id="chartCard">
        <div class="chart-header">
          <div>
            <div class="eyebrow">BINANCE · EN VIVO</div>
            <h2 class="chart-title">Gráfica de Mercado</h2>
          </div>
          <div class="chart-controls">
            <div class="chart-symbol-search-wrapper" id="chartSymbolWrapper">
              <input
                id="chartSymbolInput"
                class="text-input chart-symbol-input"
                placeholder="🔍 Buscar par (ej: BTC, SOL, PEPE...)"
                autocomplete="off"
                spellcheck="false"
              />
              <div id="chartSymbolDropdown" class="chart-symbol-dropdown hidden"></div>
            </div>
            <div class="chart-interval-group">
              <button class="chart-interval-btn" data-interval="1">1m</button>
              <button class="chart-interval-btn" data-interval="5">5m</button>
              <button class="chart-interval-btn" data-interval="15">15m</button>
              <button class="chart-interval-btn active" data-interval="30">30m</button>
              <button class="chart-interval-btn" data-interval="60">1h</button>
              <button class="chart-interval-btn" data-interval="240">4h</button>
              <button class="chart-interval-btn" data-interval="D">1D</button>
            </div>
          </div>
        </div>
        <div id="tradingview-chart" class="tradingview-chart-container">
          <div class="chart-loading"><div class="spinner"></div><span>Cargando gráfica...</span></div>
        </div>
      </div>

      <!-- ── HEATMAP SUBIDAS ─────────────────────────────────────────────── -->
      <div class="card card-span-6">
        <div class="eyebrow">MERCADO</div>
        <h2>Mayores subidas</h2>
        ${heatmapGrid(gainers, 'gain')}
      </div>

      <!-- ── HEATMAP CAÍDAS ──────────────────────────────────────────────── -->
      <div class="card card-span-6">
        <div class="eyebrow">MERCADO</div>
        <h2>Mayores caídas</h2>
        ${heatmapGrid(losers, 'loss')}
      </div>

      <!-- ── RADAR V2 ────────────────────────────────────────────────────── -->
      <div class="card card-span-12">
        <div class="item-header radar-section-header">
          <div>
            <div class="eyebrow">INTELIGENCIA TÁCTICA</div>
            <h2>Radar V2</h2>
          </div>
          <div class="pill-row compact-pill-row radar-summary-row">
            <span class="pill">Hot: ${escapeHtml(radarSummary.hot ?? 0)}</span>
            <span class="pill">Inmediatos: ${escapeHtml(radarSummary.immediate ?? 0)}</span>
            <span class="pill">Focus: ${escapeHtml(radarSummary.focus_now ?? 0)}</span>
            <span class="pill">Señal: ${escapeHtml(radarSummary.active_signals ?? 0)}</span>
          </div>
        </div>

        <div class="radar-context-grid">
          <div class="radar-context-card">
            <span class="radar-context-label">Entorno</span>
            <strong>${escapeHtml(market.environment || 'Mixto')}</strong>
            <span>${escapeHtml(market.recommendation || 'Sin lectura disponible.')}</span>
          </div>
          <div class="radar-context-card">
            <span class="radar-context-label">Sesgo</span>
            <strong>${escapeHtml(market.bias || 'Neutral')}</strong>
            <span>Lado preferido: ${escapeHtml(market.preferred_side || 'Selectivo')}</span>
          </div>
          <div class="radar-context-card">
            <span class="radar-context-label">Régimen</span>
            <strong>${escapeHtml(market.regime || '—')}</strong>
            <span>Vol ${escapeHtml(market.volatility || '—')} · Part ${escapeHtml(market.participation || '—')}</span>
          </div>
        </div>

        <div class="radar-toolbar">
          <input id="radarSearchInput" class="text-input radar-search-input" placeholder="Buscar símbolo, motivo o acción" value="${escapeHtml(radarView.search || '')}" />

          <div class="radar-chip-filter-section">
            <div class="radar-chip-filter-row">
              <span class="radar-chip-label">Dirección</span>
              ${filterChips('direction', dirOpts)}
            </div>
            <div class="radar-chip-filter-row">
              <span class="radar-chip-label">Prioridad</span>
              ${filterChips('priority', prioOpts)}
            </div>
            <div class="radar-chip-filter-row">
              <span class="radar-chip-label">Proximidad</span>
              ${filterChips('proximity', proxOpts)}
            </div>
            <div class="radar-chip-filter-row">
              <span class="radar-chip-label">Estado</span>
              ${filterChips('execution', execOpts)}
            </div>
            <div class="radar-chip-filter-row">
              <span class="radar-chip-label">Orden</span>
              ${filterChips('sort', sortOpts)}
            </div>
          </div>

          <div class="radar-toolbar-footer">
            <div class="pill-row compact-pill-row radar-results-row">
              <span class="pill">${escapeHtml(radarWindowMeta(radarWindow, radar.length))}</span>
              ${radarView.search ? `<span class="pill">Búsqueda: ${escapeHtml(radarView.search)}</span>` : ''}
            </div>
            <div class="action-row compact">
              <button class="button button-secondary" data-radar-rotate ${radarWindow.canRotate ? '' : 'disabled'}>Actualizar radar</button>
              <button class="button button-secondary radar-reset-button" data-radar-reset>Reset filtros</button>
            </div>
          </div>
        </div>

        <div class="radar-card-grid">
          ${radarCards.length ? radarCards.map(radarCardV2).join('') : '<div class="empty-state">No hay activos que cumplan ese filtro ahora mismo.</div>'}
        </div>
      </div>

      <!-- ── WATCHLIST ───────────────────────────────────────────────────── -->
      <div class="card card-span-12 watchlist-section-card">
        <div class="eyebrow">MI WATCHLIST</div>
        <div class="watchlist-section-header">
          <h2>Watchlist</h2>
          <div class="pill-row compact-pill-row" style="margin-top:6px;">
            <span class="pill">${escapeHtml(watchlistLimitText(watchlistMeta))}</span>
            <span class="pill">Slots: ${escapeHtml(watchlistMeta.slots_left ?? '\u221e')}</span>
            <span class="pill">${escapeHtml(String(watchlistMeta.plan_name || watchlistMeta.plan || 'FREE').toUpperCase())}</span>
          </div>
        </div>
        <div class="symbol-chip-row watchlist-symbols-row">
          ${(watchlistMeta.symbols || []).length
            ? (watchlistMeta.symbols || []).map(symbol =>
                `<button class="symbol-chip" data-watchlist-remove="${escapeHtml(symbol)}">${escapeHtml(symbol)} \u2715</button>`
              ).join('')
            : '<div class="empty-state" style="padding:12px 0;">Todav\u00eda no tienes s\u00edmbolos guardados.</div>'}
        </div>
        <div class="watchlist-controls">
          <input id="watchlistInput" class="text-input" placeholder="BTC, ETH, SOL o BTCUSDT" />
          <div class="action-row compact">
            <button class="button button-primary" data-watchlist-add>Agregar</button>
            <button class="button button-secondary" data-watchlist-replace>Reemplazar</button>
            <button class="button button-danger" data-watchlist-clear>Limpiar</button>
          </div>
        </div>
        <div class="list">
          ${watchlist.length ? watchlist.map(item => `
            <div class="item compact-item watchlist-item-card">
              <div class="item-header">
                <div>
                  <div class="item-title">${escapeHtml(item.symbol)}</div>
                  <div class="item-subtitle ${watchlistBiasClass(item.range_bias_label)}">${escapeHtml(item.range_bias_label || 'Sin sesgo')}</div>
                  <div class="watchlist-opportunity-copy">${escapeHtml(item.setup_action_label || 'Sin lectura operativa disponible')}</div>
                </div>
                <div class="radar-header-side">
                  <span class="radar-score-chip">${escapeHtml(item.radar_direction || '\u2014')}</span>
                </div>
              </div>
              <div class="pill-row compact-pill-row watchlist-priority-row">
                <span class="watchlist-priority-pill ${watchlistPriorityClass(item.setup_priority_label)}">Prioridad ${escapeHtml(item.setup_priority_label || '\u2014')} \u00b7 ${escapeHtml(formatNumber(item.setup_priority_score, 0))}</span>
                <span class="watchlist-priority-pill ${watchlistProximityClass(item.setup_proximity_label)}">Proximidad ${escapeHtml(item.setup_proximity_label || '\u2014')}</span>
                ${item.active_signal ? `<span class="watchlist-priority-pill watchlist-pill-active">Se\u00f1al activa \u00b7 ${escapeHtml(item.active_signal?.visibility_name || 'HADES')}</span>` : (item.latest_signal ? `<span class="watchlist-priority-pill watchlist-pill-soft">\u00daltima se\u00f1al \u00b7 ${escapeHtml(item.latest_signal.visibility_name || item.latest_signal.visibility || '\u2014')}</span>` : '')}
              </div>
              <div class="watchlist-metric-grid">
                <div class="watchlist-metric-box">
                  <span class="watchlist-metric-label">Precio</span>
                  <span class="watchlist-metric-value">${escapeHtml(formatPrice(item.last_price, 4))}</span>
                </div>
                <div class="watchlist-metric-box">
                  <span class="watchlist-metric-label">Rango 24h</span>
                  <span class="watchlist-metric-value">${escapeHtml(formatPercentSigned(item.range_pct_24h, 2))}</span>
                </div>
                <div class="watchlist-metric-box">
                  <span class="watchlist-metric-label">Posici\u00f3n</span>
                  <span class="watchlist-metric-value">${escapeHtml(watchlistRangePosition(item.range_position_pct))}</span>
                </div>
                <div class="watchlist-metric-box">
                  <span class="watchlist-metric-label">Radar</span>
                  <span class="watchlist-metric-value">${escapeHtml(item.radar_score ? formatNumber(item.radar_score, 0) : '\u2014')}</span>
                </div>
                <div class="watchlist-metric-box">
                  <span class="watchlist-metric-label">Volumen</span>
                  <span class="watchlist-metric-value">${escapeHtml(formatCompactAmount(item.quote_volume))}</span>
                </div>
                <div class="watchlist-metric-box">
                  <span class="watchlist-metric-label">\u00daltima se\u00f1al</span>
                  <span class="watchlist-metric-value">${escapeHtml(watchlistSignalSummary(item.latest_signal))}</span>
                </div>
              </div>
              <div class="watchlist-reason-list">
                ${(item.priority_reasons || []).map(reason => `<span class="watchlist-reason-chip">${escapeHtml(reason)}</span>`).join('')}
              </div>
              ${item.latest_signal?.signal_id ? `
                <div class="action-row compact watchlist-card-actions">
                  <button class="button button-secondary" data-signal-detail="${escapeHtml(item.latest_signal.signal_id)}" data-signal-source="watchlist">Ver inteligencia</button>
                  <button class="button button-secondary" data-open-risk-signal="${escapeHtml(item.latest_signal.signal_id)}">Calcular riesgo</button>
                </div>
              ` : ''}
            </div>
          `).join('') : '<div class="empty-state">Tu watchlist est\u00e1 vac\u00eda.</div>'}
        </div>
      </div>

    </div>
  `;
  // Inicializar gráfica TradingView tras renderizar el HTML
  requestAnimationFrame(() => initChartWidget());
}

// ── TradingView Live Chart ────────────────────────────────────────────────────
const _chartState = { symbol: 'BINANCE:BTCUSDT', interval: '30', allPairs: [], filteredPairs: [] };

async function _loadBinancePairs() {
  if (_chartState.allPairs.length) return _chartState.allPairs;
  try {
    const data = await api('/api/miniapp/symbols');
    _chartState.allPairs = data.symbols || [];
  } catch (e) {
    _chartState.allPairs = [
      'BTCUSDT','ETHUSDT','BNBUSDT','SOLUSDT','XRPUSDT','ADAUSDT','DOGEUSDT',
      'AVAXUSDT','DOTUSDT','LINKUSDT','MATICUSDT','LTCUSDT','UNIUSDT','ATOMUSDT',
      'NEARUSDT','APTUSDT','ARBUSDT','OPUSDT','INJUSDT','SUIUSDT','PEPEUSDT',
      'SHIBUSDT','TRXUSDT','TONUSDT','FETUSDT','RENDERUSDT','WLDUSDT'
    ].map(s => ({ symbol: s, volume: 0 }));
  }
  return _chartState.allPairs;
}

function _searchPairs(query) {
  const q = query.toUpperCase().replace(/[\/\s-]/g, '');
  if (!q) return _chartState.allPairs.slice(0, 30);
  return _chartState.allPairs
    .filter(p => p.symbol.includes(q))
    .slice(0, 40);
}

function _formatPairLabel(symbol) {
  // e.g. BTCUSDT → BTC / USDT
  const bases = ['USDT','BUSD','BTC','ETH','BNB','USDC'];
  for (const quote of bases) {
    if (symbol.endsWith(quote)) {
      return `${symbol.slice(0, -quote.length)} / ${quote}`;
    }
  }
  return symbol;
}

function initChartWidget(symbol, interval) {
  if (symbol) _chartState.symbol = symbol;
  if (interval) _chartState.interval = interval;

  const container = document.getElementById('tradingview-chart');
  if (!container) return;

  container.innerHTML = '';

  const wrapper = document.createElement('div');
  wrapper.className = 'tradingview-widget-container';
  wrapper.style.height = '420px';

  const widgetDiv = document.createElement('div');
  widgetDiv.className = 'tradingview-widget-container__widget';
  widgetDiv.style.height = '100%';

  const script = document.createElement('script');
  script.type = 'text/javascript';
  script.src = 'https://s3.tradingview.com/external-embedding/embed-widget-advanced-chart.js';
  script.async = true;
  script.textContent = JSON.stringify({
    autosize: true,
    symbol: _chartState.symbol,
    interval: _chartState.interval,
    timezone: 'Etc/UTC',
    theme: 'dark',
    style: '1',
    locale: 'es',
    gridColor: 'rgba(255,255,255,0.04)',
    hide_top_toolbar: false,
    hide_legend: false,
    allow_symbol_change: false,
    save_image: false,
    studies: ['STD;EMA', 'STD;RSI', 'STD;MACD'],
    calendar: false,
    support_host: 'https://www.tradingview.com'
  });

  wrapper.appendChild(widgetDiv);
  wrapper.appendChild(script);
  container.appendChild(wrapper);

  requestAnimationFrame(bindChartControls);
}

function _renderDropdown(pairs, input) {
  const dd = document.getElementById('chartSymbolDropdown');
  if (!dd) return;
  if (!pairs.length) {
    dd.innerHTML = '<div class="chart-dd-empty">Sin resultados</div>';
    dd.classList.remove('hidden');
    return;
  }
  dd.innerHTML = pairs.map(p => `
    <div class="chart-dd-item" data-symbol="BINANCE:${p.symbol}">
      <span class="chart-dd-label">${_formatPairLabel(p.symbol)}</span>
      <span class="chart-dd-vol">${p.volume > 0 ? '$' + (p.volume / 1e6).toFixed(1) + 'M' : ''}</span>
    </div>
  `).join('');
  dd.classList.remove('hidden');
  dd.querySelectorAll('.chart-dd-item').forEach(item => {
    item.onclick = () => {
      const sym = item.dataset.symbol;
      _chartState.symbol = sym;
      if (input) input.value = _formatPairLabel(sym.replace('BINANCE:', ''));
      dd.classList.add('hidden');
      initChartWidget(sym, _chartState.interval);
    };
  });
}

async function bindChartControls() {
  const input = document.getElementById('chartSymbolInput');
  const dd = document.getElementById('chartSymbolDropdown');

  if (input) {
    // Set current value label
    input.value = _formatPairLabel(_chartState.symbol.replace('BINANCE:', ''));

    // Load pairs and show top 30 on focus
    input.addEventListener('focus', async () => {
      await _loadBinancePairs();
      const results = _searchPairs(input.value === _formatPairLabel(_chartState.symbol.replace('BINANCE:', '')) ? '' : input.value);
      _renderDropdown(results, input);
    });

    input.addEventListener('input', async () => {
      await _loadBinancePairs();
      _renderDropdown(_searchPairs(input.value), input);
    });

    // Close dropdown on outside click
    document.addEventListener('click', (e) => {
      if (!e.target.closest('#chartSymbolWrapper')) {
        dd && dd.classList.add('hidden');
      }
    }, { capture: true });
  }

  document.querySelectorAll('.chart-interval-btn').forEach(btn => {
    btn.classList.toggle('active', btn.dataset.interval === _chartState.interval);
    btn.onclick = () => {
      _chartState.interval = btn.dataset.interval;
      document.querySelectorAll('.chart-interval-btn').forEach(b => b.classList.remove('active'));
      btn.classList.add('active');
      initChartWidget(_chartState.symbol, _chartState.interval);
    };
  });
}
// ─────────────────────────────────────────────────────────────────────────────

// ── Coin Name Lookup ─────────────────────────────────────────────────────────
const _COIN_NAMES = {
  BTC:'Bitcoin',ETH:'Ethereum',BNB:'BNB',SOL:'Solana',XRP:'Ripple',
  ADA:'Cardano',DOGE:'Dogecoin',AVAX:'Avalanche',DOT:'Polkadot',LINK:'Chainlink',
  MATIC:'Polygon',LTC:'Litecoin',UNI:'Uniswap',ATOM:'Cosmos',NEAR:'NEAR',
  APT:'Aptos',ARB:'Arbitrum',OP:'Optimism',INJ:'Injective',SUI:'Sui',
  PEPE:'Pepe',SHIB:'Shiba Inu',TRX:'TRON',TON:'Toncoin',FET:'Fetch.ai',
  RENDER:'Render',WLD:'Worldcoin',FIL:'Filecoin',ICP:'Internet Computer',
  IMX:'Immutable',SAND:'The Sandbox',MANA:'Decentraland',GALA:'Gala',
  CRV:'Curve',AAVE:'Aave',MKR:'Maker',SNX:'Synthetix',COMP:'Compound',
  LDO:'Lido',RUNE:'THORChain',SEI:'Sei',TIA:'Celestia',JUP:'Jupiter',
  PYTH:'Pyth',STRK:'Starknet',ENA:'Ethena',W:'Wormhole',NEIRO:'Neiro',
  FLOKI:'Floki',BONK:'Bonk',WIF:'dogwifhat',POPCAT:'Popcat',
  NOT:'Notcoin',HMSTR:'Hamster Kombat',DOGS:'Dogs',CATI:'Catizen',
  LAB:'LabDAO',GUA:'Gua',PHB:'Phoenix',CLOU:'Cloud',AI:'Sleepless AI',
  UB:'UB',
};
function _coinName(base) { return _COIN_NAMES[base.toUpperCase()] || ''; }
// ─────────────────────────────────────────────────────────────────────────────

// ── Live Market WebSocket ─────────────────────────────────────────────────────
const _marketStream = {
  ws: null,
  active: false,
  reconnectTimer: null,
  tickBuffer: {},   // symbol → latest tick data
  rafPending: false,
  onTick: null,     // optional callback(symbol, {price, change, vol}) for signal detail
};

// ✅ Migrated to Binance new /market/ws/ path (legacy /ws/ deprecated April 2026)
// Docs: https://developers.binance.com/docs/derivatives/usds-margined-futures/websocket-market-streams/Important-WebSocket-Change-Notice
const _WS_URL = 'wss://fstream.binance.com/market/ws/!miniTicker@arr';

function startMarketStream() {
  if (_marketStream.ws && _marketStream.ws.readyState <= 1) return;
  _marketStream.active = true;
  _openMarketWS();
}

function stopMarketStream() {
  _marketStream.active = false;
  clearTimeout(_marketStream.reconnectTimer);
  if (_marketStream.ws) {
    try { _marketStream.ws.close(); } catch(_) {}
    _marketStream.ws = null;
  }
}

function _openMarketWS() {
  if (!_marketStream.active) return;
  try {
    const ws = new WebSocket(_WS_URL);
    _marketStream.ws = ws;

    ws.onmessage = (evt) => {
      let ticks;
      try { ticks = JSON.parse(evt.data); } catch(_) { return; }
      if (!Array.isArray(ticks)) return;
      for (const t of ticks) {
        if (!t.s) continue;
        const open = parseFloat(t.o);
        const close = parseFloat(t.c);
        const changePct = open > 0 ? ((close - open) / open) * 100 : 0;
        _marketStream.tickBuffer[t.s] = {
          price: close,
          change: changePct,
          vol: parseFloat(t.q),
        };
      }
      if (!_marketStream.rafPending) {
        _marketStream.rafPending = true;
        requestAnimationFrame(_flushMarketTicks);
      }
    };

    ws.onerror = () => { ws.close(); };
    ws.onclose = () => {
      _marketStream.ws = null;
      if (_marketStream.active) {
        _marketStream.reconnectTimer = setTimeout(_openMarketWS, 3000);
      }
    };
  } catch(_) {
    if (_marketStream.active) {
      _marketStream.reconnectTimer = setTimeout(_openMarketWS, 5000);
    }
  }
}

function _flushMarketTicks() {
  _marketStream.rafPending = false;
  const buffer = _marketStream.tickBuffer;
  _marketStream.tickBuffer = {};

  for (const [symbol, data] of Object.entries(buffer)) {
    // Notify signal detail price ticker if it's listening for this symbol
    if (_marketStream.onTick) {
      try { _marketStream.onTick(symbol, data); } catch {}
    }

    // Update all [data-live-change], [data-live-price], [data-live-vol] for this symbol
    const changeEls = document.querySelectorAll(`[data-live-change="${symbol}"]`);
    changeEls.forEach(el => {
      const formatted = formatPercentSigned(data.change, 2);
      if (el.textContent !== formatted) {
        el.textContent = formatted;
        const positive = data.change >= 0;
        el.className = el.className.replace(/positive-text|negative-text/g, '').trim()
          + ' ' + (positive ? 'positive-text' : 'negative-text') + ' live-flash';
        setTimeout(() => el.classList.remove('live-flash'), 600);
      }
    });

    const priceEls = document.querySelectorAll(`[data-live-price="${symbol}"]`);
    priceEls.forEach(el => {
      const formatted = formatPrice(data.price, data.price < 1 ? 6 : data.price < 100 ? 4 : 2);
      if (el.textContent !== formatted) {
        el.textContent = formatted;
        el.classList.add('live-flash');
        setTimeout(() => el.classList.remove('live-flash'), 600);
      }
    });

    const volEls = document.querySelectorAll(`[data-live-vol="${symbol}"]`);
    volEls.forEach(el => {
      const formatted = formatCompactAmount(data.vol);
      if (el.textContent !== formatted) el.textContent = formatted;
    });
  }
}
// ─────────────────────────────────────────────────────────────────────────────

function watchlistLimitText(meta) {
  if (!meta) return '—';
  if (meta.max_symbols === null || meta.max_symbols === undefined) return 'Sin límite';
  return `${meta.symbols_count || 0} / ${meta.max_symbols}`;
}

async function refreshWatchlist() {
  const payload = await api('/api/miniapp/watchlist');
  state.payload.watchlist = payload.items || [];
  state.payload.watchlist_meta = payload.meta || { symbols: [], symbols_count: 0, max_symbols: 0, slots_left: 0, can_add_more: false };
}

async function mutateWatchlist(path, body, successMessage) {
  const payload = await api(path, {
    method: 'POST',
    body: body ? JSON.stringify(body) : undefined,
  });
  state.payload.watchlist = payload.items || [];
  state.payload.watchlist_meta = payload.meta || { symbols: [], symbols_count: 0, max_symbols: 0, slots_left: 0, can_add_more: false };
  state.payload.dashboard.watchlist_count = payload.meta?.symbols_count || 0;
  renderMarket();
  renderHome();
  bindViewButtons();
  if (successMessage || payload.message) tg?.showAlert(successMessage || payload.message);
}

function renderHistory() {
  const allItems = state.payload.history || [];
  const f = state.historyFilter || 'all';

  const items = f === 'all' ? allItems : allItems.filter(item => {
    const norm = String(item.resolution || item.result || '').toLowerCase();
    if (f === 'win') return norm === 'tp1' || norm === 'tp2' || norm === 'won';
    if (f === 'loss') return norm === 'sl' || norm === 'lost';
    if (f === 'exp') return norm !== 'tp1' && norm !== 'tp2' && norm !== 'won' && norm !== 'sl' && norm !== 'lost';
    return true;
  });

  const total = allItems.length;
  const wonCount = allItems.filter(i => { const n = String(i.resolution || i.result || '').toLowerCase(); return n === 'tp1' || n === 'tp2' || n === 'won'; }).length;
  const lossCount = allItems.filter(i => { const n = String(i.resolution || i.result || '').toLowerCase(); return n === 'sl' || n === 'lost'; }).length;
  const winRate = total ? Math.round(wonCount / total * 100) : 0;
  const rVals = allItems.map(i => Number(i.r_multiple)).filter(v => !isNaN(v) && v !== null);
  const avgR = rVals.length ? rVals.reduce((a, b) => a + b, 0) / rVals.length : null;
  const avgRStr = avgR !== null ? (avgR >= 0 ? `+${avgR.toFixed(2)}` : avgR.toFixed(2)) : '—';
  const avgRPos = avgR !== null && avgR >= 0;

  const filterBtn = (val, label) =>
    `<button class="hx-filter-btn${f === val ? ' hx-filter-active' : ''}" data-hx-filter="${val}">${label}</button>`;

  els.history.innerHTML = `
    <div class="hx-header-block">
      <div class="hx-header-eyebrow">HADES · VERIFIED RECORD</div>
      <h2 class="hx-header-title">Historial</h2>
      <p class="hx-header-sub">Señales cerradas y resultados verificados</p>
      <div class="hx-stats-row">
        <div class="hx-stat">
          <span class="hx-stat-val">${total}</span>
          <span class="hx-stat-lbl">SEÑALES</span>
        </div>
        <div class="hx-stat hx-stat-accent">
          <span class="hx-stat-val">${winRate}%</span>
          <span class="hx-stat-lbl">WIN RATE</span>
        </div>
        <div class="hx-stat ${avgRPos ? 'hx-stat-green' : 'hx-stat-red'}">
          <span class="hx-stat-val">${avgRStr}</span>
          <span class="hx-stat-lbl">AVG R</span>
        </div>
        <div class="hx-stat">
          <span class="hx-stat-val">${wonCount}/${lossCount}</span>
          <span class="hx-stat-lbl">W/L</span>
        </div>
      </div>
      <div class="hx-filter-row">
        ${filterBtn('all', 'Todas')}
        ${filterBtn('win', '✓ WIN')}
        ${filterBtn('loss', '✗ LOSS')}
        ${filterBtn('exp', '○ EXP')}
      </div>
    </div>
    <div class="hx-list">
      ${items.length ? items.map((item, i) => historyCard(item, i)).join('') : '<div class="empty-state">No hay señales en este filtro.</div>'}
    </div>
  `;
}


function accountMetricCard(label, value, toneClass = '') {
  return `
    <div class="account-metric-card ${escapeHtml(toneClass)}">
      <div class="account-metric-label">${escapeHtml(label)}</div>
      <div class="account-metric-value">${escapeHtml(value ?? '—')}</div>
    </div>
  `;
}

function billingFocusCard(focus = {}, billing = {}) {
  if (!focus || !Object.keys(focus).length) return '';
  const toneClass = billingToneClass(focus.tone);
  const steps = Array.isArray(focus.steps) ? focus.steps : [];
  const supportUrl = billing?.support_url || '#';
  const primaryCta = String(focus.primary_cta || '').trim();
  const primaryCtaLower = primaryCta.toLowerCase();
  let primaryAction = '';
  if (primaryCta) {
    if (primaryCtaLower === 'soporte') {
      primaryAction = `<a class="button button-secondary" target="_blank" rel="noopener" href="${escapeHtml(supportUrl)}">${escapeHtml(primaryCta)}</a>`;
    } else if (primaryCtaLower === 'generar orden' || primaryCtaLower === 'renovar') {
      primaryAction = `<button type="button" class="button button-secondary" data-billing-focus-action="open-plans">${escapeHtml(primaryCta)}</button>`;
    } else if (primaryCtaLower === 'confirmar pago' || primaryCtaLower === 'revisar de nuevo') {
      primaryAction = `<button type="button" class="button button-secondary" data-billing-focus-action="focus-order">${escapeHtml(primaryCta)}</button>`;
    } else if (primaryCtaLower === 'refrescar cuenta' || primaryCtaLower === 'esperando verificación') {
      primaryAction = `<button type="button" class="button button-secondary" data-billing-focus-action="refresh-account">${escapeHtml(primaryCta)}</button>`;
    } else {
      primaryAction = `<span class="button button-secondary" aria-disabled="true">${escapeHtml(primaryCta)}</span>`;
    }
  }
  const diagnostics = !billing?.payment_config_ready ? paymentConfigDiagnosticsInline(billing) : '';
  return `
    <div class="card payment-focus-panel card-span-12 ${toneClass}">
      <div class="payment-focus-card ${toneClass}">
        <div class="payment-focus-copy">
          <div class="payment-focus-kicker">Billing Overview</div>
          <div class="payment-focus-title">${escapeHtml(focus.title || 'Billing')}</div>
          <div class="payment-focus-headline">${escapeHtml(focus.headline || focus.message || 'Estado comercial disponible.')}</div>
          ${focus.message ? `<div class="payment-focus-message">${escapeHtml(focus.message)}</div>` : ''}
          ${focus.hint ? `<div class="payment-focus-hint">${escapeHtml(focus.hint)}</div>` : ''}
          ${primaryAction ? `<div class="action-row compact" style="margin-top:12px;">${primaryAction}</div>` : ''}
          ${diagnostics}
        </div>
      </div>
      ${steps.length ? `<div class="billing-step-row">${steps.map(step => `<div class="billing-step ${billingStepClass(step.state)}"><span class="billing-step-dot"></span><span>${escapeHtml(step.label)}</span></div>`).join('')}</div>` : ''}
    </div>
  `;
}

function paymentConfigDiagnosticsInline(billing = {}) {
  const status = billing?.payment_config_status || {};
  const checks = Array.isArray(status.checks) ? status.checks : [];
  const missingKeys = Array.isArray(status.missing_keys) ? status.missing_keys : [];
  if (!checks.length && !missingKeys.length) return '';
  return `
    <div class="payment-focus-diagnostics">
      <div class="payment-focus-diagnostics-title">Diagnóstico de configuración</div>
      <div class="payment-focus-diagnostics-list">
        ${checks.length ? checks.map(check => `<span class="pill ${check.value_present ? '' : 'pill-warning'}">${escapeHtml(check.label || check.key)}: ${check.value_present ? 'OK' : 'Falta'}</span>`).join('') : missingKeys.map(key => `<span class="pill pill-warning">${escapeHtml(key)}: Falta</span>`).join('')}
      </div>
    </div>
  `;
}

function paymentConfigDiagnosticsCard(billing = {}) {
  const status = billing?.payment_config_status || {};
  const checks = Array.isArray(status.checks) ? status.checks : [];
  const missingKeys = Array.isArray(status.missing_keys) ? status.missing_keys : [];
  if (billing?.payment_config_ready || (!checks.length && !missingKeys.length)) return '';
  return `
    <div class="card config-diagnostics-card card-span-12">
      <h2>Diagnóstico de pago</h2>
      <div class="config-check-grid">
        ${(checks.length ? checks : missingKeys.map(key => ({ key, label: key, value_present: false }))).map(check => `
          <div class="config-check-item ${check.value_present ? 'is-positive' : 'is-warning'}">
            <div class="item-title">${escapeHtml(check.label || check.key)}</div>
            <div class="item-subtitle">${check.value_present ? 'Configuración detectada' : 'Falta en el proceso web'}</div>
            <code>${escapeHtml(check.key || '—')}</code>
          </div>
        `).join('')}
      </div>
    </div>
  `;
}

function recentOrderItem(order = {}) {
  return `
    <div class="item">
      <div class="item-header">
        <div>
          <div class="item-title">${escapeHtml(order.plan_name || String(order.plan || '').toUpperCase())} · ${escapeHtml(order.days ?? '—')} días</div>
          <div class="item-subtitle">${escapeHtml(formatMoney(order.amount_usdt))} · ${escapeHtml(formatStatusLabel(order.status_label || order.status || '—'))}</div>
        </div>
        <span class="plan-tag">${escapeHtml(order.time_left_label || formatDate(order.updated_at || order.created_at))}</span>
      </div>
      <div class="inline-meta">
        <span>Creada: ${escapeHtml(formatDate(order.created_at))}</span>
        <span>Actualizada: ${escapeHtml(formatDate(order.updated_at))}</span>
      </div>
    </div>
  `;
}

function referralRewardItem(reward = {}) {
  return `
    <div class="item">
      <div class="item-header">
        <div>
          <div class="item-title">Recompensa ${escapeHtml(reward.reward_plan_name || reward.reward_plan || '—')} · ${escapeHtml(reward.reward_days ?? 0)} días</div>
          <div class="item-subtitle">Referido ${escapeHtml(reward.activated_plan_name || reward.activated_plan || '—')} · ${escapeHtml(reward.activated_days ?? 0)} días</div>
        </div>
        <span class="plan-tag">#${escapeHtml(reward.referred_id ?? 0)}</span>
      </div>
      <div class="inline-meta">
        <span>Aplicada: ${escapeHtml(formatDate(reward.created_at))}</span>
      </div>
    </div>
  `;
}

function accountTimelineItem(event = {}) {
  const meta = event.metadata && Object.keys(event.metadata).length
    ? Object.entries(event.metadata).slice(0, 2).map(([key, value]) => `${key}: ${value}`).join(' · ')
    : '';
  return `
    <div class="item">
      <div class="item-header">
        <div>
          <div class="item-title">${escapeHtml(event.event_label || event.event_type || 'Evento')}</div>
          <div class="item-subtitle">${escapeHtml(event.after_plan_name || event.plan_name || event.plan || '—')} ${event.days ? `· ${escapeHtml(event.days)} días` : ''}</div>
        </div>
        <span class="plan-tag">${escapeHtml(formatDate(event.created_at))}</span>
      </div>
      <div class="inline-meta">
        <span>Antes: ${escapeHtml(event.before_plan_name || event.before_plan || '—')}</span>
        <span>Después: ${escapeHtml(event.after_plan_name || event.after_plan || '—')}</span>
        ${event.source ? `<span>Fuente: ${escapeHtml(event.source)}</span>` : ''}
      </div>
      ${meta ? `<div class="item-subtitle" style="margin-top:8px;">${escapeHtml(meta)}</div>` : ''}
    </div>
  `;
}

function planBlock(planKey, items, currentPlan, billing = {}, options = {}) {
  const current = String(currentPlan || '').toLowerCase();
  const featureRows = items[0]?.features || [];
  const isCurrentPlan = items[0]?.is_current_plan || current === planKey;
  const activeOrder = billing.active_order || null;
  const paymentReady = billing.payment_config_ready !== false;
  const hidden = Boolean(options.hidden);
  if (hidden) return '';
  return `
    <div class="card card-span-6" data-plan-block="${escapeHtml(planKey)}">
      <div class="item-header" style="margin-bottom: 14px;">
        <div>
          <h2 style="margin:0;">${escapeHtml(String(planKey).toUpperCase())}</h2>
          <div class="item-subtitle">${isCurrentPlan ? 'Tu plan actual' : 'Disponible para compra o upgrade'}</div>
        </div>
        ${isCurrentPlan ? '<span class="plan-tag">ACTUAL</span>' : ''}
      </div>
      ${featureRows.length ? `<div class="feature-list">${featureRows.map(feature => `<div class="feature-item">• ${escapeHtml(feature)}</div>`).join('')}</div>` : ''}
      <div class="list" style="margin-top: 12px;">
        ${items.map(item => {
          const sameOpenOrder = activeOrder && String(activeOrder.plan || '').toLowerCase() === String(planKey).toLowerCase() && Number(activeOrder.days || 0) === Number(item.days || 0);
          const hasOtherOpenOrder = activeOrder && !sameOpenOrder;
          const disabled = !paymentReady || sameOpenOrder;
          let cta = isCurrentPlan ? 'Renovar' : 'Comprar';
          let tone = isCurrentPlan ? 'button-secondary' : 'button-primary';
          if (!paymentReady) {
            cta = 'Pago no listo';
            tone = 'button-secondary';
          } else if (sameOpenOrder) {
            cta = 'Orden abierta';
            tone = 'button-secondary';
          } else if (hasOtherOpenOrder) {
            cta = 'Reemplazar';
            tone = 'button-secondary';
          }
          return `
            <div class="item">
              <div class="item-header">
                <div>
                  <div class="item-title">${escapeHtml(item.days)} días</div>
                  <div class="item-subtitle">${escapeHtml(formatMoney(item.price_usdt))}${sameOpenOrder ? ' · Ya pendiente' : hasOtherOpenOrder ? ' · Reemplaza orden actual' : ''}</div>
                </div>
                <button class="button ${tone}" data-create-order="${escapeHtml(planKey)}:${escapeHtml(item.days)}" ${disabled ? 'disabled' : ''}>${cta}</button>
              </div>
            </div>
          `;
        }).join('')}
      </div>
    </div>
  `;
}

function detailInfoChip(label, value, extraClass = '') {
  return `
    <div class="detail-info-chip ${extraClass}">
      <span class="detail-info-chip-label">${escapeHtml(label)}</span>
      <span class="detail-info-chip-value">${escapeHtml(value)}</span>
    </div>
  `;
}

function detailStatCard(label, value, valueClass = '') {
  return `
    <div class="detail-stat-card">
      <span class="detail-stat-label">${escapeHtml(label)}</span>
      <span class="detail-stat-value ${valueClass}">${escapeHtml(value)}</span>
    </div>
  `;
}

function scoreListsEqual(a, b) {
  if (!Array.isArray(a) || !Array.isArray(b)) return false;
  if (a.length !== b.length) return false;
  return a.every((item, index) => {
    const other = b[index] || {};
    const leftScore = Number(item?.score);
    const rightScore = Number(other?.score);
    const leftMissing = item?.score === null || item?.score === undefined || Number.isNaN(leftScore);
    const rightMissing = other?.score === null || other?.score === undefined || Number.isNaN(rightScore);
    const scoresEqual = (leftMissing && rightMissing) || (!leftMissing && !rightMissing && leftScore === rightScore);
    return String(item?.label || '') === String(other?.label || '') && scoresEqual;
  });
}

function renderScoreBreakdown(items) {
  if (!items || !items.length) return '<div class="empty-state">Sin desglose disponible.</div>';
  return `<div class="component-list">${items.map(item => {
    const numericScore = Number(item?.score);
    const hasNumericScore = item && item.has_numeric_score !== false && item.score !== null && item.score !== undefined && !Number.isNaN(numericScore);
    const valueText = hasNumericScore
      ? formatNumber(numericScore, 2)
      : (item?.status_label || 'OK');
    const toneClass = hasNumericScore
      ? (numericScore >= 0 ? 'positive-text' : 'negative-text')
      : 'positive-text';
    return `
    <div class="component-row">
      <span>${escapeHtml(item.label)}</span>
      <span class="${toneClass}">${escapeHtml(valueText)}</span>
    </div>
  `;}).join('')}</div>`;
}

function renderRadarDetailModal(payload) {
  const radar = payload?.radar || {};
  const scanner = payload?.scanner || {};
  const signalContext = payload?.signal_context || {};
  const marketContext = payload?.market_context || {};
  const tacticalChecks = payload?.tactical_checks || [];
  const profiles = scanner.profiles || [];
  const components = scanner.components || [];

  els.signalDetailTitle.textContent = `${radar.symbol || payload?.symbol || 'Radar'} · táctica`;
  els.signalDetailBody.innerHTML = `
    <div class="signal-intel-layout">
      <div class="card detail-status-card">
        <div class="detail-status-top">
          <div class="detail-status-copy-block">
            <span class="detail-kicker">Radar táctico</span>
            <div class="item-title">${escapeHtml(radar.execution_state_label || 'Observación')} · ${escapeHtml(radar.direction || '—')}</div>
            <div class="item-subtitle">${escapeHtml(radar.operator_note || radar.action_label || 'Sin lectura táctica disponible')}</div>
          </div>
          <span class="radar-score-chip">Radar ${escapeHtml(formatNumber(radar.final_score, 0))}</span>
        </div>
        <p class="detail-status-copy">${escapeHtml(marketContext.recommendation || 'Sin recomendación general de mercado.')}</p>
      </div>

      <div class="detail-info-grid">
        ${detailInfoChip('Sesgo', marketContext.bias || '—')}
        ${detailInfoChip('Lado', marketContext.preferred_side || '—')}
        ${detailInfoChip('Régimen', marketContext.regime || '—')}
        ${detailInfoChip('Setup', radar.setup_mode_label || '—')}
        ${detailInfoChip('Alineación', radar.alignment_label || '—')}
        ${detailInfoChip('Riesgo', radar.risk_label || '—')}
      </div>

      <div class="pill-row compact-pill-row">
        <span class="watchlist-priority-pill ${radarExecutionClass(radar.execution_state_label)}">${escapeHtml(radar.execution_state_label || 'Observación')}</span>
        <span class="watchlist-priority-pill ${watchlistPriorityClass(radar.priority_label)}">Prioridad ${escapeHtml(radar.priority_label || '—')}</span>
        <span class="watchlist-priority-pill ${watchlistProximityClass(radar.proximity_label)}">Proximidad ${escapeHtml(radar.proximity_label || '—')}</span>
        <span class="watchlist-priority-pill ${radarAlignmentClass(radar.alignment_label)}">${escapeHtml(radar.alignment_label || 'Selectivo')}</span>
        <span class="watchlist-priority-pill ${radarRiskClass(radar.risk_label)}">Riesgo ${escapeHtml(radar.risk_label || '—')}</span>
      </div>

      <div class="detail-stat-grid">
        ${detailStatCard('Ranking', formatNumber(radar.ranking_score, 1))}
        ${detailStatCard('Precio', formatPrice(radar.last_price, 4), sideClassByValue(radar.change_pct || 0))}
        ${detailStatCard('Cambio 24h', formatPercentSigned(radar.change_pct, 2), sideClassByValue(radar.change_pct || 0))}
        ${detailStatCard('Funding', formatPercentSigned(radar.funding_rate_pct, 3), sideClassByValue(radar.funding_rate_pct || 0))}
        ${detailStatCard('Open interest', formatCompactAmount(radar.open_interest))}
        ${detailStatCard('Volumen', formatCompactAmount(radar.quote_volume))}
        ${detailStatCard('Posición rango', watchlistRangePosition(radar.range_position_pct))}
        ${detailStatCard('Ventana', radar.window_label || '—')}
        ${detailStatCard('Convicción', radar.conviction_label || '—')}
        ${detailStatCard('Contexto señal', radar.signal_context_label || '—')}
      </div>

      <div class="card signal-intel-section signal-intel-section-full">
        <h3>Plan táctico</h3>
        <div class="radar-plan-list">
          ${(radar.trade_plan || []).map(step => `<div class="radar-plan-item">${escapeHtml(step)}</div>`).join('')}
        </div>
        ${tacticalChecks.length ? `<div class="feature-list radar-check-list">${tacticalChecks.map(item => `<div class="feature-item">• ${escapeHtml(item)}</div>`).join('')}</div>` : ''}
      </div>

      <div class="card signal-intel-section signal-intel-section-full">
        <h3>Scanner / setup</h3>
        <div class="pill-row compact-pill-row">
          <span class="pill">Estado: ${escapeHtml(scanner.label || '—')}</span>
          ${scanner.direction ? `<span class="pill">Dirección: ${escapeHtml(scanner.direction)}</span>` : ''}
          ${scanner.setup_group ? `<span class="pill">Setup: ${escapeHtml(String(scanner.setup_group).toUpperCase())}</span>` : ''}
          ${scanner.score !== undefined && scanner.score !== null ? `<span class="pill">Score: ${escapeHtml(formatNumber(scanner.score, 1))}</span>` : ''}
          ${scanner.atr_pct !== undefined && scanner.atr_pct !== null ? `<span class="pill">ATR: ${escapeHtml(formatFractionPercent(scanner.atr_pct))}</span>` : ''}
          ${scanner.direction_alignment === true ? `<span class="pill watchlist-pill-active">Alineado con radar</span>` : ''}
          ${scanner.direction_alignment === false ? `<span class="pill watchlist-pill-soft">Dirección distinta al radar</span>` : ''}
        </div>
        <p class="detail-status-copy">${escapeHtml(scanner.summary || 'Sin lectura del scanner por ahora.')}</p>
        <div class="inline-meta">
          ${scanner.score_profile ? `<span>Perfil score: ${escapeHtml(String(scanner.score_profile).toUpperCase())}</span>` : ''}
          ${scanner.score_calibration ? `<span>Calibración: ${escapeHtml(String(scanner.score_calibration))}</span>` : ''}
          ${scanner.timeframes?.length ? `<span>TF: ${escapeHtml(scanner.timeframes.join(' / '))}</span>` : ''}
          ${scanner.strongest_component ? `<span>Más fuerte: ${escapeHtml(scanner.strongest_component.label)} (${escapeHtml(formatNumber(scanner.strongest_component.score, 2))})</span>` : ''}
          ${scanner.weakest_component ? `<span>Más débil: ${escapeHtml(scanner.weakest_component.label)} (${escapeHtml(formatNumber(scanner.weakest_component.score, 2))})</span>` : ''}
        </div>
        ${profiles.length ? `
          <div class="detail-stat-grid radar-profile-grid">
            ${profiles.map(profile => detailStatCard(
              `${profile.label} · SL ${formatPrice(profile.stop_loss, 4)}`,
              `TP1 ${formatPrice(profile.tp1, 4)} · TP2 ${formatPrice(profile.tp2, 4)}`,
              '')) .join('')}
          </div>
        ` : ''}
        ${components.length ? renderScoreBreakdown(components) : '<div class="empty-state">Sin desglose de setup en este momento.</div>'}
      </div>

      <div class="card signal-intel-section signal-intel-section-full">
        <h3>Conexión con señales</h3>
        <div class="pill-row compact-pill-row">
          <span class="pill">Contexto: ${escapeHtml(signalContext.label || radar.signal_context_label || 'Sin señal')}</span>
          ${signalContext.signal?.visibility_name || signalContext.signal?.visibility ? `<span class="pill">Tier: ${escapeHtml(String(signalContext.signal?.visibility_name || signalContext.signal?.visibility).toUpperCase())}</span>` : ''}
          ${signalContext.signal?.score !== undefined && signalContext.signal?.score !== null ? `<span class="pill">Score señal: ${escapeHtml(formatNumber(signalContext.signal.score, 1))}</span>` : ''}
        </div>
        <p class="detail-status-copy">${escapeHtml(signalContext.signal ? watchlistSignalSummary(signalContext.signal) : 'Todavía no tienes una señal reciente enlazada a este activo.')}</p>
        <div class="action-row compact radar-card-actions">
          ${signalContext.signal_detail_available ? `<button class="button button-secondary" data-radar-open-signal="${escapeHtml(signalContext.signal_id || '')}">Ver inteligencia de la señal</button>` : ''}
          ${radar.symbol ? `<button class="button button-primary" data-radar-follow="${escapeHtml(radar.symbol)}">Seguir símbolo</button>` : ''}
        </div>
      </div>
    </div>
  `;
}

async function openRadarDetail(symbol) {
  if (!symbol) return;
  els.signalDetailModal.classList.remove('hidden');
  els.signalDetailModal.setAttribute('aria-hidden', 'false');
  els.signalDetailTitle.textContent = 'Radar táctico';
  els.signalDetailBody.innerHTML = '<div class="loading-inline">Cargando drill-down táctico del radar...</div>';
  try {
    const payload = await api(`/api/miniapp/radar/${encodeURIComponent(symbol)}`);
    state.radarDetail = payload;
    renderRadarDetailModal(payload);
    bindViewButtons();
  } catch (error) {
    els.signalDetailBody.innerHTML = `<div class="error-banner">${escapeHtml(error.message || 'No se pudo cargar el radar táctico.')}</div>`;
  }
}

function closeSignalDetailModal() {
  if (!els.signalDetailModal) return;
  els.signalDetailModal.classList.add('hidden');
  els.signalDetailModal.setAttribute('aria-hidden', 'true');
  stopSignalPriceTicker();
}

// ── Live Price Ticker — reuses existing _marketStream (no extra WebSocket) ────
// Strategy:
//   1. Hook _marketStream.onTick → called by _flushMarketTicks on every RAF flush
//   2. Ensure market stream is running (starts it if user is not on market view)
//   3. REST fallback via fapi.binance.com every 3 s if no WS tick in 5 s
// Docs: https://developers.binance.com/docs/derivatives/usds-margined-futures/websocket-market-streams/Individual-Symbol-Ticker-Streams
const _priceTicker = {
  symbol:      null,
  lastPrice:   null,
  prevPrice:   null,
  mapMin:      null,
  mapMax:      null,
  _pollTimer:  null,   // REST fallback interval id
  _lastTickAt: 0,      // timestamp of last successful tick (WS or REST)
};

function startSignalPriceTicker(symbol) {
  stopSignalPriceTicker();
  if (!symbol) return;

  // Binance tickBuffer uses UPPERCASE symbols (e.g. "PENGUUSDT")
  const pair = symbol.replace('BINANCE:', '').replace('/', '').toUpperCase();
  _priceTicker.symbol = pair;
  _priceTicker._lastTickAt = 0;

  // ── 1. Subscribe to the existing market stream ──────────────────────────────
  _marketStream.onTick = (sym, data) => {
    if (sym !== pair) return;
    _priceTicker.prevPrice = _priceTicker.lastPrice;
    _priceTicker.lastPrice = data.price;
    _priceTicker._lastTickAt = Date.now();
    updateLivePriceDisplay(data.price, data.change);
  };

  // Ensure stream is active (user may not be on the market view)
  startMarketStream();

  // ── 2. REST fallback — only fires when WS has been silent > 5 s ────────────
  // Uses the same Binance Futures REST endpoint the backend already trusts
  _priceTicker._pollTimer = setInterval(async () => {
    if (Date.now() - _priceTicker._lastTickAt < 5000) return; // WS is healthy
    try {
      const res = await fetch(
        `https://fapi.binance.com/fapi/v1/ticker/24hr?symbol=${pair}`
      );
      if (!res.ok) return;
      const d = await res.json();
      const price = parseFloat(d.lastPrice);
      const pct   = parseFloat(d.priceChangePercent);
      if (!isNaN(price) && price > 0) {
        _priceTicker.prevPrice   = _priceTicker.lastPrice;
        _priceTicker.lastPrice   = price;
        _priceTicker._lastTickAt = Date.now();
        updateLivePriceDisplay(price, pct);
      }
    } catch {}
  }, 3000);
}

function stopSignalPriceTicker() {
  // Unsubscribe from market stream
  _marketStream.onTick = null;

  // Clear REST polling timer
  if (_priceTicker._pollTimer) {
    clearInterval(_priceTicker._pollTimer);
    _priceTicker._pollTimer = null;
  }

  // Stop market stream only if we're not currently on the market view
  // (avoids interrupting live market data while browsing)
  if (typeof state !== 'undefined' && state.currentView !== 'market') {
    stopMarketStream();
  }

  _priceTicker.symbol      = null;
  _priceTicker.lastPrice   = null;
  _priceTicker.prevPrice   = null;
  _priceTicker.mapMin      = null;
  _priceTicker.mapMax      = null;
  _priceTicker._lastTickAt = 0;
}

function updateLivePriceDisplay(price, changePct) {
  const priceEl = document.getElementById('signalLivePrice');
  const changeEl = document.getElementById('signalLiveChange');
  const dotEl = document.getElementById('signalLiveDot');

  if (!priceEl) return;

  const prev = _priceTicker.prevPrice;
  const isUp = prev === null || price >= prev;
  const formatted = price < 0.001 ? price.toFixed(8)
    : price < 1 ? price.toFixed(6)
    : price < 100 ? price.toFixed(4)
    : price < 10000 ? price.toFixed(2)
    : price.toFixed(2);

  // Update price text and color silently
  priceEl.textContent = formatted;
  priceEl.classList.remove('positive-text', 'negative-text');
  priceEl.classList.add(isUp ? 'positive-text' : 'negative-text');

  if (changeEl) {
    const sign = changePct >= 0 ? '+' : '';
    changeEl.textContent = `${sign}${changePct.toFixed(2)}%`;
    changeEl.classList.remove('positive-text', 'negative-text');
    changeEl.classList.add(changePct >= 0 ? 'positive-text' : 'negative-text');
  }

  if (dotEl) dotEl.className = `live-dot ${isUp ? 'live-dot-up' : 'live-dot-down'}`;

  // Move the signal level map indicator
  _updateSignalMapIndicator(price, isUp);
}

function _updateSignalMapIndicator(price, isUp) {
  const line  = document.getElementById('signalMapLiveLine');
  const label = document.getElementById('signalMapLivePrice');
  if (!line || !label) return;

  const { mapMin, mapMax } = _priceTicker;
  if (mapMin == null || mapMax == null || mapMax <= mapMin) return;

  const pct = Math.max(0, Math.min(100, (price - mapMin) / (mapMax - mapMin) * 100));
  line.style.bottom = pct + '%';
  label.textContent = price < 0.001 ? price.toFixed(8)
    : price < 1 ? price.toFixed(6)
    : price < 100 ? price.toFixed(4)
    : price.toFixed(2);
  line.className = 'map-live-line ' + (isUp ? 'map-live-up' : 'map-live-down');
}


// ── Signal Level Map ──────────────────────────────────────────────────────────
function _buildSignalMap(tracking, direction, signal) {
  const isLong = !String(direction).toLowerCase().includes('short');
  const entry  = parseFloat(tracking.entry_price) || 0;
  const sl     = parseFloat(tracking.stop_loss)   || 0;
  const tps    = (tracking.take_profits || []).map(v => parseFloat(v)).filter(v => v > 0);
  const tp1    = tps[0] || 0;
  const tp2    = tps[1] || 0;
  const cur    = parseFloat(tracking.current_price) || entry;

  if (!entry || !sl) return '';

  const allLvls = [sl, entry, ...(tp1?[tp1]:[]), ...(tp2?[tp2]:[])];
  const minL = Math.min(...allLvls);
  const maxL = Math.max(...allLvls);
  const range = maxL - minL;
  if (range <= 0) return '';

  // Store bounds for live updates
  _priceTicker.mapMin = minL;
  _priceTicker.mapMax = maxL;

  const pct  = p => ((p - minL) / range * 100).toFixed(3);
  const dist = (p, base) => {
    if (!base || !p) return '—';
    const d = (p - base) / base * 100;
    return (d >= 0 ? '+' : '') + d.toFixed(2) + '%';
  };

  const rr = (tp1 && sl && entry && Math.abs(entry - sl) > 0)
    ? (Math.abs(tp1 - entry) / Math.abs(entry - sl)).toFixed(1) : null;
  const rrLabel = rr ? `R/R 1:${rr}` : '';

  const fmtP = p => p < 0.001 ? p.toFixed(8) : p < 1 ? p.toFixed(6) : p < 100 ? p.toFixed(4) : p.toFixed(2);

  const livePct = Math.max(0, Math.min(100, (cur - minL) / range * 100)).toFixed(3);
  const liveIsUp = cur >= entry;

  const entryPct  = pct(entry);
  const rewardH   = (100 - parseFloat(entryPct)).toFixed(3);
  const riskH     = entryPct;

  return `
    <div class="sig-map-meta">
      <span class="sig-map-pair">${(signal.symbol||'')}</span>
      ${rrLabel ? `<span class="sig-map-rr">${rrLabel}</span>` : ''}
    </div>
    <div class="sig-map-body">
      <!-- Coloured background zones -->
      <div class="sig-map-zone sig-zone-reward" style="bottom:${entryPct}%;height:${rewardH}%"></div>
      <div class="sig-map-zone sig-zone-risk"   style="bottom:0%;height:${riskH}%"></div>

      ${tp2 ? `<div class="sig-map-level level-tp2" style="bottom:${pct(tp2)}%">
        <span class="sig-lvl-tag tag-tp">TP2</span>
        <span class="sig-lvl-price">${fmtP(tp2)}</span>
        <span class="sig-lvl-dist">${dist(tp2, entry)}</span>
      </div>` : ''}

      ${tp1 ? `<div class="sig-map-level level-tp1" style="bottom:${pct(tp1)}%">
        <span class="sig-lvl-tag tag-tp">TP1</span>
        <span class="sig-lvl-price">${fmtP(tp1)}</span>
        <span class="sig-lvl-dist">${dist(tp1, entry)}</span>
      </div>` : ''}

      <div class="sig-map-level level-entry" style="bottom:${entryPct}%">
        <span class="sig-lvl-tag tag-entry">ENTRADA</span>
        <span class="sig-lvl-price">${fmtP(entry)}</span>
        <span class="sig-lvl-dist">base</span>
      </div>

      <div class="sig-map-level level-sl" style="bottom:${pct(sl)}%">
        <span class="sig-lvl-tag tag-sl">SL</span>
        <span class="sig-lvl-price">${fmtP(sl)}</span>
        <span class="sig-lvl-dist">${dist(sl, entry)}</span>
      </div>

      <!-- Live price indicator — moves on every WS tick -->
      <div class="map-live-line ${liveIsUp?'map-live-up':'map-live-down'}" id="signalMapLiveLine" style="bottom:${livePct}%">
        <span class="map-live-tag">▶ LIVE</span>
        <span class="sig-lvl-price" id="signalMapLivePrice">${fmtP(cur)}</span>
      </div>
    </div>
  `;
}

function renderSignalDetailModal(payload) {
  const signal = payload?.signal || {};
  const tracking = payload?.tracking || {};
  const analysis = payload?.analysis || {};
  const selectedProfile = payload?.selected_profile || 'moderado';
  const profileOptions = payload?.profile_options || ['moderado'];
  const tier = payload?.tracking_tier || 'basic';
  const warnings = [...(tracking.warnings || []), ...(analysis.warnings || [])];
  const mainComponents = analysis.components?.length ? analysis.components : (analysis.normalized_components?.length ? analysis.normalized_components : (analysis.raw_components || []));
  const showRaw = analysis.raw_components?.length && !scoreListsEqual(analysis.raw_components, mainComponents);
  const showNormalized = analysis.normalized_components?.length && !scoreListsEqual(analysis.normalized_components, mainComponents) && !scoreListsEqual(analysis.normalized_components, analysis.raw_components || []);
  const statusBadge = signal.result
    ? `<span class="${badgeClassByResult(signal)}">${escapeHtml(resultLabel(signal))}</span>`
    : `<span class="plan-tag">${escapeHtml(tracking.result_label || formatStatusLabel(signal.status || 'active'))}</span>`;
  const strategyLabel = tracking.strategy_label || signal.strategy_label || 'Señal táctica';
  const strategyFamily = tracking.strategy_family || 'Marco táctico';
  const entryModelLabel = tracking.entry_model_label || 'Entrada táctica';
  const liveSummary = tracking.live_summary || tracking.recommendation || 'Sin lectura operativa disponible.';
  const strategySummary = tracking.strategy_summary || 'Sin contexto táctico adicional para esta estrategia.';
  const actionLabel = tracking.action_label || 'Seguimiento';

  els.signalDetailTitle.textContent = `${signal.symbol || 'Señal'} · ${signal.direction || ''}`.trim();

  const currentPriceFormatted = formatPrice(tracking.current_price, 4);
  const directionIsLong = String(signal.direction || '').toLowerCase().includes('long');
  const directionIsShort = String(signal.direction || '').toLowerCase().includes('short');
  const directionClass = directionIsLong ? 'positive-text' : directionIsShort ? 'negative-text' : '';
  const directionGlowClass = directionIsLong ? 'direction-glow-long' : directionIsShort ? 'direction-glow-short' : '';

  els.signalDetailBody.innerHTML = `
    <div class="signal-intel-layout">

      <!-- ═══ HERO: Signal Level Map (only shown when valid level data exists) ═══ -->
      <div class="card signal-intel-section signal-intel-section-full intel-signal-chart-card intel-animate intel-animate-1">
        <div class="intel-chart-header">
          <div class="intel-chart-pair-badge">
            <span class="intel-chart-symbol">${escapeHtml(signal.symbol || '')}</span>
            <span class="intel-chart-dir ${directionClass}">${escapeHtml(String(signal.direction || '').toUpperCase())}</span>
          </div>
          <div class="chart-interval-group" id="signalChartIntervalGroup">
            <button class="chart-interval-btn" data-signal-interval="5">5m</button>
            <button class="chart-interval-btn" data-signal-interval="15">15m</button>
            <button class="chart-interval-btn active" data-signal-interval="30">30m</button>
            <button class="chart-interval-btn" data-signal-interval="60">1h</button>
            <button class="chart-interval-btn" data-signal-interval="240">4h</button>
            <button class="chart-interval-btn" data-signal-interval="D">1D</button>
          </div>
        </div>
        <div id="signalDetailChart" class="intel-chart-body">
          <div class="chart-loading"><div class="spinner"></div><span>Cargando gráfica...</span></div>
        </div>
      </div>

      <!-- ═══ LIVE PRICE HERO ═══ -->
      <div class="card signal-intel-section signal-intel-section-full signal-live-price-card intel-animate intel-animate-2" id="signalPriceHero">
        <div class="live-price-header">
          <div class="live-price-label-block">
            <span id="signalLiveDot" class="live-dot live-dot-up"></span>
            <span class="live-price-label">PRECIO EN VIVO</span>
          </div>
          <div class="live-price-direction-badge ${directionClass}">${escapeHtml(String(signal.direction || '').toUpperCase())}</div>
        </div>
        <div class="live-price-main">
          <span id="signalLivePrice" class="live-price-value">${currentPriceFormatted}</span>
          <span id="signalLiveChange" class="live-price-change ${sideClassByValue(tracking.current_move_pct || 0)}">${tracking.current_move_pct !== undefined && tracking.current_move_pct !== null ? (tracking.current_move_pct >= 0 ? '+' : '') + tracking.current_move_pct.toFixed(2) + '%' : '—'}</span>
        </div>
        <div class="live-price-levels">
          <div class="level-item level-entry">
            <span class="level-label">Entrada</span>
            <span class="level-value">${escapeHtml(formatPrice(tracking.entry_price, 4))}</span>
          </div>
          <div class="level-item level-sl">
            <span class="level-label">SL</span>
            <span class="level-value negative-text">${escapeHtml(formatPrice(tracking.stop_loss, 4))}</span>
          </div>
          <div class="level-item level-tp1">
            <span class="level-label">TP1</span>
            <span class="level-value positive-text">${escapeHtml(formatPrice((tracking.take_profits || [])[0], 4))}</span>
          </div>
          <div class="level-item level-tp2">
            <span class="level-label">TP2</span>
            <span class="level-value positive-text">${escapeHtml(formatPrice((tracking.take_profits || [])[1], 4))}</span>
          </div>
        </div>
        <div class="live-progress-bar-wrap">
          <div class="live-progress-bar-track">
            <div class="live-progress-bar-fill" style="width: ${Math.min(100, Math.max(0, tracking.progress_to_tp1_pct ?? 0))}%"></div>
          </div>
          <span class="live-progress-label">Progreso TP1: ${tracking.progress_to_tp1_pct !== null && tracking.progress_to_tp1_pct !== undefined ? formatPercentSigned(tracking.progress_to_tp1_pct, 1) : '—'}</span>
        </div>
      </div>

      <!-- ═══ STATUS CARD ═══ -->
      <div class="card detail-status-card intel-animate intel-animate-3">
        <div class="detail-status-top">
          <div class="detail-status-copy-block">
            <span class="detail-kicker">Estrategia en vivo</span>
            <div class="item-title">${escapeHtml(strategyLabel)} · ${escapeHtml(tracking.state_label || 'Sin estado')}</div>
            <div class="item-subtitle">${escapeHtml(tracking.entry_state_label || 'Sin lectura operativa')} · ${escapeHtml(actionLabel)}</div>
          </div>
          ${statusBadge}
        </div>
        <p class="detail-status-copy">${escapeHtml(liveSummary)}</p>
      </div>

      <!-- ═══ INFO CHIPS ═══ -->
      <div class="detail-info-grid intel-animate intel-animate-4">
        ${detailInfoChip('Plan', String(payload.viewer_plan || 'free').toUpperCase())}
        ${detailInfoChip('Tier', String(signal.visibility || 'free').toUpperCase())}
        ${detailInfoChip('Estrategia', strategyLabel)}
        ${detailInfoChip('Perfil', profileLabel(selectedProfile))}
        ${detailInfoChip('Setup', String(analysis.setup_group || signal.setup_group || 'legacy').toUpperCase())}
        ${detailInfoChip('Score', formatNumber(analysis.normalized_score ?? analysis.score ?? signal.score, 1))}
      </div>

      <div class="card signal-intel-section signal-intel-section-full intel-animate intel-animate-5">
        <h3>Marco estratégico de la señal</h3>
        <div class="pill-row compact-pill-row">
          <span class="pill">Familia: ${escapeHtml(strategyFamily)}</span>
          <span class="pill">Modelo: ${escapeHtml(entryModelLabel)}</span>
          <span class="pill">Lectura ahora: ${escapeHtml(actionLabel)}</span>
        </div>
        <p class="detail-status-copy">${escapeHtml(strategySummary)}</p>
        <p class="detail-status-copy">${escapeHtml(liveSummary)}</p>
      </div>

      <div class="detail-profile-selector intel-animate intel-animate-6" role="tablist" aria-label="Perfil de lectura">
        ${profileOptions.map(option => `<button class="detail-profile-button ${option === selectedProfile ? 'is-active' : ''}" data-signal-profile="${escapeHtml(option)}" data-signal-id="${escapeHtml(signal.signal_id || '')}" aria-pressed="${option === selectedProfile ? 'true' : 'false'}">${escapeHtml(profileLabel(option))}</button>`).join('')}
      </div>

      <div class="detail-stat-grid intel-animate intel-animate-7">
        ${detailStatCard('Dist. entrada', formatFractionPercent(tracking.distance_to_entry_pct))}
        ${detailStatCard('Dist. SL', formatFractionPercent(tracking.stop_distance_pct))}
        ${detailStatCard('Dist. TP1', formatFractionPercent(tracking.tp1_distance_pct))}
        ${detailStatCard('Dist. TP2', formatFractionPercent(tracking.tp2_distance_pct))}
      </div>

      <div class="card signal-intel-section signal-intel-section-full intel-animate intel-animate-8">
        <h3>Estado operativo en vivo</h3>
        <div class="pill-row compact-pill-row">
          <span class="pill">Entrada ya tocada: ${tracking.entry_touched ? 'Sí' : 'No'}</span>
          <span class="pill">En zona ahora: ${tracking.in_entry_zone ? 'Sí' : 'No'}</span>
          <span class="pill">Operable ahora: ${tracking.is_operable_now ? 'Sí' : 'No'}</span>
          <span class="pill">TP1 ya tocado: ${tracking.tp1_hit_now ? 'Sí' : 'No'}</span>
          <span class="pill">TP2 ya tocado: ${tracking.tp2_hit_now ? 'Sí' : 'No'}</span>
          <span class="pill">SL ya roto: ${tracking.stop_hit_now ? 'Sí' : 'No'}</span>
        </div>
        <div class="inline-meta">
          <span>Creada: ${escapeHtml(formatDate(tracking.created_at || signal.created_at))}</span>
          <span>Visible hasta: ${escapeHtml(formatDate(tracking.telegram_valid_until || signal.telegram_valid_until))}</span>
          <span>Evaluación hasta: ${escapeHtml(formatDate(tracking.evaluation_valid_until))}</span>
        </div>
      </div>

      <div class="card signal-intel-section signal-intel-section-full intel-animate intel-animate-9">
        <h3>Desglose de calidad</h3>
        <div class="pill-row compact-pill-row">
          <span class="pill">ATR: ${escapeHtml(formatFractionPercent(analysis.atr_pct))}</span>
          <span class="pill">TF: ${escapeHtml((analysis.timeframes || []).join(' / ') || '—')}</span>
          ${analysis.leverage ? `<span class="pill">Leverage: ${escapeHtml(String(analysis.leverage))}</span>` : ''}
          ${analysis.market_validity_minutes ? `<span class="pill">Mercado: ${escapeHtml(String(analysis.market_validity_minutes))} min</span>` : ''}
        </div>
        <div class="inline-meta">
          ${analysis.strongest_component ? `<span>Más fuerte: ${escapeHtml(analysis.strongest_component.label)} (${escapeHtml(formatNumber(analysis.strongest_component.score, 2))})</span>` : ''}
          ${analysis.weakest_component ? `<span>Más débil: ${escapeHtml(analysis.weakest_component.label)} (${escapeHtml(formatNumber(analysis.weakest_component.score, 2))})</span>` : ''}
          ${analysis.score_profile ? `<span>Perfil score: ${escapeHtml(String(analysis.score_profile).toUpperCase())}</span>` : ''}
          ${analysis.score_calibration ? `<span>Calibración: ${escapeHtml(String(analysis.score_calibration))}</span>` : ''}
        </div>
        ${renderScoreBreakdown(mainComponents)}
        ${showRaw ? `<h3 class="detail-subheading">Componentes raw</h3>${renderScoreBreakdown(analysis.raw_components)}` : ''}
        ${showNormalized ? `<h3 class="detail-subheading">Componentes normalizados</h3>${renderScoreBreakdown(analysis.normalized_components)}` : ''}
        ${analysis.raw_components?.length && analysis.normalized_components?.length && !showRaw && !showNormalized ? `<div class="detail-note">En esta señal, los valores raw y normalizados coinciden, por eso no se repiten abajo.</div>` : ''}
      </div>

      ${warnings.length ? `<div class="card signal-intel-section signal-intel-section-full intel-animate intel-animate-10"><h3>Notas</h3><div class="feature-list">${warnings.map(item => `<div class="feature-item">• ${escapeHtml(item)}</div>`).join('')}</div></div>` : ''}

      <div class="card signal-intel-section signal-intel-section-full intel-animate intel-animate-11">
        <h3>Gestión de riesgo</h3>
        <p>Lleva esta señal directamente a la calculadora para revisar sizing, margen requerido, pérdida al stop y RR neto.</p>
        <div class="action-row compact">
          <button class="button button-secondary" data-open-risk-signal="${escapeHtml(signal.signal_id)}">Abrir calculadora</button>
        </div>
      </div>
      ${payload.upgrade_hint ? `<div class="card signal-intel-section signal-intel-section-full upgrade-note-card intel-animate intel-animate-12"><h3>Lectura premium</h3><p>${escapeHtml(payload.upgrade_hint)}</p></div>` : ''}
    </div>
  `;

  // Start live price ticker via Binance WebSocket
  const signalSymbol = signal.symbol ? `BINANCE:${signal.symbol}` : 'BINANCE:BTCUSDT';
  startSignalPriceTicker(signalSymbol);

  // Init TradingView chart for this signal's pair
  requestAnimationFrame(() => initSignalChartWidget(signalSymbol, '30'));
}

// ── TradingView chart inside Signal Detail Modal ───────────────────────────────
const _signalChartState = { symbol: 'BINANCE:BTCUSDT', interval: '30' };

function initSignalChartWidget(symbol, interval) {
  if (symbol) _signalChartState.symbol = symbol;
  if (interval) _signalChartState.interval = interval;

  const container = document.getElementById('signalDetailChart');
  if (!container) return;

  // Use a direct <iframe> to TradingView's widgetembed endpoint instead of
  // the tv.js Widget API. In Telegram WebView bottom-sheet modals the Widget
  // API fails silently because the dynamically-created iframe can't measure
  // its container, and on the first signal load tv.js hasn't finished fetching
  // yet. A direct iframe requires no external JS, has no timing dependency,
  // and renders correctly inside any overflow:auto / scrollable container.
  //
  // Studies are joined with the SOH separator (\x01) that widgetembed expects.
  const studies  = ['STD;EMA', 'STD;RSI', 'STD;MACD'].join('\x01');
  const params   = new URLSearchParams({
    symbol:              _signalChartState.symbol,
    interval:            _signalChartState.interval,
    timezone:            'Etc/UTC',
    theme:               'dark',
    style:               '1',
    locale:              'es',
    hide_top_toolbar:    '0',
    hide_legend:         '0',
    allow_symbol_change: '0',
    save_image:          '0',
    calendar:            '0',
    studies,
    support_host:        'https://www.tradingview.com',
  });

  const iframe = document.createElement('iframe');
  iframe.src             = `https://s.tradingview.com/widgetembed/?${params}`;
  iframe.style.cssText   = 'width:100%;height:100%;border:none;display:block;';
  iframe.allowFullscreen = true;
  iframe.setAttribute('scrolling', 'no');
  iframe.setAttribute('frameborder', '0');

  container.innerHTML = '';
  container.appendChild(iframe);

  _bindSignalIntervalBtns();
}

function _bindSignalIntervalBtns() {
  const group = document.getElementById('signalChartIntervalGroup');
  if (!group) return;
  group.querySelectorAll('.chart-interval-btn').forEach(btn => {
    btn.classList.toggle('active', btn.dataset.signalInterval === _signalChartState.interval);
    btn.onclick = () => {
      group.querySelectorAll('.chart-interval-btn').forEach(b => b.classList.remove('active'));
      btn.classList.add('active');
      initSignalChartWidget(_signalChartState.symbol, btn.dataset.signalInterval);
    };
  });
}

async function openSignalDetail(signalId, profile = 'moderado') {
  if (!signalId) return;
  els.signalDetailModal.classList.remove('hidden');
  els.signalDetailModal.setAttribute('aria-hidden', 'false');
  els.signalDetailTitle.textContent = 'Detalle de señal';
  els.signalDetailBody.innerHTML = '<div class="loading-inline">Cargando inteligencia de la señal...</div>';
  try {
    const payload = await api(`/api/miniapp/signals/${encodeURIComponent(signalId)}?profile=${encodeURIComponent(profile)}`);
    state.signalDetail = payload;
    renderSignalDetailModal(payload);
    bindViewButtons();
  } catch (error) {
    els.signalDetailBody.innerHTML = `<div class="error-banner">${escapeHtml(error.message || 'No se pudo cargar el detalle.')}</div>`;
  }
}

function renderAccount() {

  const account = state.payload.account || {};
  const overview = account.overview || {};
  const fallbackMe = state.payload.me || {};
  const me = {
    ...fallbackMe,
    ...overview,
  };
  const fallbackPlans = state.payload.plans || {};
  const plans = {
    plus: (account.plans?.plus && account.plans.plus.length ? account.plans.plus : fallbackPlans.plus) || [],
    premium: (account.plans?.premium && account.plans.premium.length ? account.plans.premium : fallbackPlans.premium) || [],
  };
  const fallbackWatchlistMeta = state.payload.watchlist_meta || {};
  const subscription = {
    ...(account.subscription || {}),
    watchlist: {
      ...fallbackWatchlistMeta,
      ...((account.subscription || {}).watchlist || {}),
    },
  };
  const billing = account.billing || {};
  const referrals = account.referrals || {};
  const botUsername = state.payload.bot_username || 'HADES_ALPHA_bot';
  const referralCode = referrals.ref_code || me.ref_code || '';
  const referralLink = referrals.referral_link || (referralCode ? `https://t.me/${botUsername}?start=${referralCode}` : '');
  const timeline = account.timeline || [];
  const support = account.support || { url: state.payload.support_url || '#' };
  const activeOrder = billing.active_order || state.payload.dashboard?.active_payment_order || null;
  const expiresText = me.expires_at ? formatDate(me.expires_at) : 'Sin vencimiento';
  const currentPlanValue = String(me.plan || subscription.plan || '').toLowerCase();
  const currentStatusValue = String(subscription.status || me.subscription_status || '').toLowerCase();
  const isPremiumActive = currentPlanValue === 'premium' && currentStatusValue === 'active';
  let billingFocus = { ...(billing.focus || {}) };
  const safeDaysLeft = Number.isFinite(Number(me.days_left)) ? Number(me.days_left) : Number(subscription.days_left || 0);
  if (!activeOrder && billing.payment_config_ready !== false && safeDaysLeft > 3 && String(billingFocus.state || '') === 'renew_soon') {
    billingFocus = {
      ...billingFocus,
      state: 'healthy_subscription',
      tone: 'neutral',
      title: 'Billing listo',
      headline: `Tu acceso vence en ${safeDaysLeft} días`,
      message: 'Tu suscripción está activa. Puedes renovar más adelante o dejar una renovación preparada cuando se acerque el vencimiento.',
      primary_cta: 'Listo',
      hint: 'Cuando falten pocos días, aquí se activará la recomendación de renovación.',
    };
  }
  const watchlistMeta = subscription.watchlist || {};
  const billingSummary = billing.summary || {};
  const recentOrders = billing.recent_orders || [];
  const recentRewards = referrals.recent_rewards || [];
  const rewardRules = referrals.reward_rules || [];

  els.account.innerHTML = `
    <div class="section-grid">
      ${accountNoticeCard(state.accountNotice)}
      <div class="card card-span-12">
        <div class="item-header">
          <div>
            <h2 style="margin:0;">Centro de cuenta</h2>
            <div class="item-subtitle">Estado comercial, suscripción, billing y referidos desde la MiniApp.</div>
          </div>
          <span class="plan-tag">${escapeHtml(me.plan_name || 'FREE')}</span>
        </div>
        <div class="pill-row">
          <span class="pill">Estado: ${escapeHtml(me.subscription_status_label || me.subscription_status || 'free')}</span>
          <span class="pill">Vence: ${escapeHtml(expiresText)}</span>
          <span class="pill">Días restantes: ${escapeHtml(me.days_left || 0)}</span>
          <span class="pill">Idioma: ${escapeHtml(me.language || 'es')}</span>
          <span class="pill">Código: ${escapeHtml(me.ref_code || '—')}</span>
        </div>
        <div class="account-metric-grid">
          ${accountMetricCard('Watchlist', `${watchlistMeta.symbols_count ?? me.watchlist_symbols ?? 0}/${watchlistMeta.max_symbols ?? me.watchlist_limit ?? '∞'}`)}
          ${accountMetricCard('Referidos válidos', referrals.valid_referrals_total ?? me.valid_referrals_total ?? 0)}
          ${accountMetricCard('Días ganados', referrals.reward_days_total ?? me.reward_days_total ?? 0)}
          ${accountMetricCard('Órdenes', billingSummary.total ?? 0)}
        </div>
      </div>

      <div class="card card-span-6">
        <h2>Suscripción</h2>
        <p>${escapeHtml(subscription.plan_name || me.plan_name || 'FREE')} · ${escapeHtml(subscription.status_label || me.subscription_status_label || me.subscription_status || 'free')} · ${escapeHtml(expiresText)}</p>
        <div class="inline-meta">
          <span>ID usuario: ${escapeHtml(me.user_id)}</span>
          <span>Inicio: ${escapeHtml(formatDate(subscription.plan_started_at))}</span>
          <span>Última compra: ${escapeHtml(formatDate(subscription.last_purchase_at))}</span>
          <span>Último ciclo: ${escapeHtml(subscription.last_purchase_days || 0)} días</span>
        </div>
        ${subscription.features?.length ? `<div class="feature-list" style="margin-top:12px;">${subscription.features.map(feature => `<div class="feature-item">• ${escapeHtml(feature)}</div>`).join('')}</div>` : '<div class="empty-state">Sin beneficios listados por ahora.</div>'}
      </div>

      <div class="card card-span-6">
        <h2>Gestión de riesgo</h2>
        <p>Configura capital, riesgo por trade, fees, slippage y calcula sizing real desde señales vivas e históricas.</p>
        <div class="pill-row compact-pill-row">
          <span class="pill">Capital: ${escapeHtml(formatMoney((state.riskCenter.payload?.profile || {}).capital_usdt ?? 0))}</span>
          <span class="pill">Riesgo: ${escapeHtml(formatNumber((state.riskCenter.payload?.profile || {}).risk_percent ?? 0, 2))}%</span>
          <span class="pill">Leverage: ${escapeHtml(formatNumber((state.riskCenter.payload?.profile || {}).default_leverage ?? 0, 0))}x</span>
        </div>
        <div class="action-row compact" style="margin-top:12px;">
          <button class="button button-secondary" data-open-risk-center="true">Abrir gestión de riesgo</button>
        </div>
      </div>

      <div class="card card-span-6">
        <h2>Rendimiento</h2>
        <p>Módulo dedicado para revisar 7D / 30D / total, PF por R, expectancy, score buckets y breakdown por plan.</p>
        <div class="pill-row compact-pill-row">
          <span class="pill">7D PF: ${escapeHtml(formatRatioValue((state.payload.dashboard || {}).summary_7d?.profit_factor, Boolean((state.payload.dashboard || {}).summary_7d?.profit_factor_infinite)))}</span>
          <span class="pill">30D PF: ${escapeHtml(formatRatioValue((state.payload.dashboard || {}).summary_30d?.profit_factor, Boolean((state.payload.dashboard || {}).summary_30d?.profit_factor_infinite)))}</span>
          <span class="pill">30D Exp: ${escapeHtml(formatNumber((state.payload.dashboard || {}).summary_30d?.expectancy_r || 0, 4))}R</span>
        </div>
        <div class="action-row compact" style="margin-top:12px;">
          <button class="button button-secondary" data-open-performance-center="true">Abrir rendimiento</button>
        </div>
      </div>

      <div class="card card-span-6">
        <h2>Ajustes y alertas push</h2>
        <p>Configura idioma y qué niveles de señal quieres recibir como aviso push en Telegram.</p>
        <div class="pill-row compact-pill-row">
          <span class="pill">Idioma: ${escapeHtml(account.settings?.language || me.language || 'es')}</span>
          <span class="pill">Push: ${account.settings?.push_alerts?.enabled === false ? 'Silenciado' : 'Activo'}</span>
          <span class="pill">Preferencias: ${escapeHtml((account.settings?.push_alerts?.selected_tiers || []).map(item => String(item).toUpperCase()).join(' / ') || 'Default')}</span>
        </div>
        <div class="action-row compact" style="margin-top:12px;">
          <button class="button button-secondary" data-open-settings-center="true">Abrir ajustes</button>
        </div>
      </div>

      <div class="card card-span-6">
        <h2>Referidos</h2>
        <div class="pill-row">
          <span class="pill">Totales: ${escapeHtml(referrals.total_referred || 0)}</span>
          <span class="pill">PLUS: ${escapeHtml(referrals.plus_referred || 0)}</span>
          <span class="pill">PREMIUM: ${escapeHtml(referrals.premium_referred || 0)}</span>
          <span class="pill">Válidos: ${escapeHtml(referrals.valid_referrals_total || 0)}</span>
        </div>
        <div class="action-row compact">
          <button class="button button-secondary" data-copy-value="${escapeHtml(referralCode)}">Copiar código</button>
          <button class="button button-secondary" data-copy-value="${escapeHtml(referralLink)}">Copiar enlace</button>
        </div>
        ${rewardRules.length ? `<div class="feature-list" style="margin-top:12px;">${rewardRules.map(rule => `<div class="feature-item">• ${escapeHtml(rule)}</div>`).join('')}</div>` : '<div class="empty-state">Sin reglas de recompensa disponibles.</div>'}
      </div>

      ${me.is_admin ? `
      <div class="card card-span-12">
        <div class="item-header">
          <div>
            <h2 style="margin:0;">Administración</h2>
            <div class="item-subtitle">Acceso exclusivo para admins. Aquí vive el panel operativo y el reset con confirmación.</div>
          </div>
          <span class="plan-tag">ADMIN</span>
        </div>
        <div class="action-row">
          <button class="button button-secondary" data-open-admin-panel="true">Abrir panel admin</button>
        </div>
      </div>` : ''}

      ${billingFocusCard(billingFocus, billing)}
      ${paymentConfigDiagnosticsCard(billing)}

      <div class="card card-span-12">
        <h2>Billing</h2>
        <div class="account-metric-grid">
          ${accountMetricCard('Config pago', billing.payment_config_ready ? 'Lista' : 'Incompleta', billing.payment_config_ready ? 'is-positive' : 'is-warning')}
          ${accountMetricCard('Abiertas', billingSummary.open ?? 0)}
          ${accountMetricCard('Completadas', billingSummary.completed ?? 0, 'is-positive')}
          ${accountMetricCard('Expiradas', billingSummary.expired ?? 0, 'is-warning')}
          ${accountMetricCard('Canceladas', billingSummary.cancelled ?? 0)}
          ${accountMetricCard('Último cobro', formatDate(billing.latest_completed_at))}
        </div>
      </div>

      ${paymentInstructions(activeOrder, billingFocus) || '<div class="card card-span-12"><h2>Pago actual</h2><div class="empty-state">No tienes una orden de pago pendiente.</div></div>'}
      ${planBlock('plus', plans.plus || [], me.plan, billing, { hidden: isPremiumActive })}
      ${planBlock('premium', plans.premium || [], me.plan, billing)}

      <div class="card card-span-6">
        <h2>Órdenes recientes</h2>
        <div class="list">
          ${recentOrders.length ? recentOrders.map(recentOrderItem).join('') : '<div class="empty-state">Todavía no hay órdenes registradas.</div>'}
        </div>
      </div>

      <div class="card card-span-6">
        <h2>Recompensas recientes</h2>
        <div class="list">
          ${recentRewards.length ? recentRewards.map(referralRewardItem).join('') : '<div class="empty-state">Todavía no tienes recompensas aplicadas.</div>'}
        </div>
      </div>

      <div class="card card-span-12">
        <h2>Timeline comercial</h2>
        <div class="list">
          ${timeline.length ? timeline.map(accountTimelineItem).join('') : '<div class="empty-state">Sin eventos comerciales recientes.</div>'}
        </div>
      </div>

      <div class="card card-span-12 oraculum-bridge-card ${isPremiumActive ? 'is-active' : 'is-locked'}">
        <div class="item-header">
          <div>
            <h2 style="margin:0;">Oraculum AI Terminal</h2>
            <div class="item-subtitle">Vincula tu acceso PREMIUM de HADES para abrir Oraculum con sesión activa hasta tu vencimiento actual.</div>
          </div>
          <span class="plan-tag">${isPremiumActive ? 'PREMIUM ACTIVO' : 'PREMIUM REQUERIDO'}</span>
        </div>
        <div class="pill-row compact-pill-row">
          <span class="pill">Plan: ${escapeHtml(me.plan_name || subscription.plan_name || 'FREE')}</span>
          <span class="pill">Vence: ${escapeHtml(expiresText)}</span>
          <span class="pill">Días: ${escapeHtml(safeDaysLeft || 0)}</span>
        </div>
        <p>${isPremiumActive
          ? 'Al tocar el botón se crea un acceso temporal de un solo uso y Oraculum guarda una sesión segura hasta tu plan_end premium.'
          : 'Activa PREMIUM para poder vincular Oraculum. Sin premium no se entregan datos de mercado.'}</p>
        <div class="action-row">
          <button class="button button-primary" data-open-oraculum ${isPremiumActive ? '' : 'disabled'}>🔮 Vincular / Abrir Oraculum</button>
          <button class="button button-secondary" data-billing-focus-action="refresh-account">Actualizar estado</button>
        </div>
      </div>


      <div class="card card-span-12 sentinel-bridge-card ${isPremiumActive ? 'is-active' : 'is-locked'}">
        <div class="item-header">
          <div>
            <h2 style="margin:0;">HADES Sentinel</h2>
            <div class="item-subtitle">Inteligencia on-chain premium: ballenas, exchange flows, desbloqueos y alertas críticas sin duplicar señales ni watchlist.</div>
          </div>
          <span class="plan-tag">${isPremiumActive ? 'PREMIUM ACTIVO' : 'PREMIUM REQUERIDO'}</span>
        </div>
        <div class="pill-row compact-pill-row">
          <span class="pill">Plan: ${escapeHtml(me.plan_name || subscription.plan_name || 'FREE')}</span>
          <span class="pill">Vence: ${escapeHtml(expiresText)}</span>
          <span class="pill">Días: ${escapeHtml(safeDaysLeft || 0)}</span>
        </div>
        <p>${isPremiumActive
          ? 'Al tocar el botón se crea un acceso temporal de un solo uso y Sentinel guarda una sesión segura hasta tu plan_end premium.'
          : 'Activa PREMIUM para poder vincular Sentinel. Sin premium no se entregan eventos de inteligencia on-chain.'}</p>
        <div class="action-row">
          <button class="button button-primary" data-open-sentinel ${isPremiumActive ? '' : 'disabled'}>🛰️ Vincular / Abrir Sentinel</button>
          <button class="button button-secondary" data-billing-focus-action="refresh-account">Actualizar estado</button>
        </div>
      </div>

      <div class="card card-span-12">
        <h2>Soporte</h2>
        <div class="action-row">
          <a class="button button-secondary" target="_blank" rel="noopener" href="${escapeHtml(support.url || state.payload.support_url || '#')}">Abrir grupo de soporte</a>
        </div>
      </div>

      <div class="card card-span-12">
        <h2>App instalada</h2>
        <p style="font-size:0.85rem;color:var(--text-muted,#8899aa);margin-bottom:12px">
          Si instalaste HADES como app, toca este botón para conectar tu sesión.
        </p>
        <div class="action-row">
          <button class="button button-primary" id="btnConnectPWA">📲 Conectar sesión a la App</button>
        </div>
      </div>
    </div>
  `;

  const btnPWA = els.account.querySelector('#btnConnectPWA');
  if (btnPWA) {
    btnPWA.addEventListener('click', () => {
      const token = state.token;
      if (!token) { alert('Sesión no disponible.'); return; }
      const base = window.location.origin + '/miniapp/static/index.html';
      const pwaUrl = base + '?pwa_token=' + encodeURIComponent(token);
      if (window.Telegram?.WebApp?.openLink) {
        window.Telegram.WebApp.openLink(pwaUrl);
      } else {
        window.open(pwaUrl, '_blank');
      }
    });
  }
}

function renderView(view) {
  if (view === 'signals') return renderSignals();
  if (view === 'market') return renderMarket();
  if (view === 'history') return renderHistory();
  if (view === 'account') return renderAccount();
  if (view === 'performance') return renderPerformance();
  if (view === 'risk') return renderRisk();
  if (view === 'settings') return renderSettings();
  if (view === 'admin') return renderAdmin();
  return renderHome();
}

function ensureViewData(view, options = {}) {
  const force = Boolean(options.force);
  if (!state.token) return;
  if (view === 'home') {
    Promise.resolve(refreshDashboardState(force)).catch(error => console.warn('MiniApp dashboard refresh failed', error));
    queueLiveSignalsRefresh(force ? 'home-bootstrap' : 'home-view');
    return;
  }
  if (view === 'signals') {
    queueLiveSignalsRefresh(force ? 'signals-bootstrap' : 'signals-view');
    return;
  }
  if (view === 'market') {
    Promise.resolve(refreshMarketState(force)).catch(error => console.warn('MiniApp market refresh failed', error));
    return;
  }
  if (view === 'history') {
    Promise.resolve(refreshHistoryState(force)).catch(error => console.warn('MiniApp history refresh failed', error));
    return;
  }
  if (view === 'account') {
    Promise.resolve(refreshAccountState(force)).catch(error => console.warn('MiniApp account refresh failed', error));
  }
}

function renderAll() {
  ensurePayloadShell();
  setTopSummary();
  renderView(state.currentView || 'home');
  bindViewButtons();
  els.loading.classList.add('hidden');
  els.content.classList.remove('hidden');
  els.bottomNav.classList.remove('hidden');
}

function setView(view) {
  // Stop live stream when leaving market view
  if (state.currentView === 'market' && view !== 'market') {
    stopMarketStream();
  }
  state.currentView = view;
  document.querySelectorAll('.view').forEach(node => node.classList.remove('active'));
  document.getElementById(`view-${view}`).classList.add('active');
  document.querySelectorAll('.nav-item').forEach(node => node.classList.toggle('active', node.dataset.view === view));
  els.titleMain.textContent = labels[view] || 'HADES';
  renderView(view);
  bindViewButtons();
  ensureViewData(view);
  syncLiveSignalsPolling();
  // Start live stream when entering market view
  if (view === 'market') {
    startMarketStream();
  }
}

async function copyValue(value, successMessage = 'Copiado correctamente.') {
  const normalized = String(value || '').trim();
  if (!normalized) {
    tg?.showAlert('No hay valor para copiar.');
    return;
  }
  try {
    await navigator.clipboard.writeText(normalized);
    tg?.showAlert(successMessage);
    return;
  } catch {}

  try {
    const textarea = document.createElement('textarea');
    textarea.value = normalized;
    textarea.setAttribute('readonly', 'readonly');
    textarea.style.position = 'fixed';
    textarea.style.opacity = '0';
    textarea.style.pointerEvents = 'none';
    document.body.appendChild(textarea);
    textarea.focus();
    textarea.select();
    textarea.setSelectionRange(0, textarea.value.length);
    const copied = document.execCommand('copy');
    document.body.removeChild(textarea);
    if (copied) {
      tg?.showAlert(successMessage);
      return;
    }
  } catch {}

  tg?.showAlert('No se pudo copiar.');
}

function bindViewButtons() {
  document.querySelectorAll('[data-open-oraculum]').forEach(button => {
    button.onclick = () => {
      if (button.disabled) return;
      createAndOpenOraculumLink(button);
    };
  });

  document.querySelectorAll('[data-open-sentinel]').forEach(button => {
    button.onclick = () => {
      if (button.disabled) return;
      createAndOpenSentinelLink(button);
    };
  });

  document.querySelectorAll('[data-billing-focus-action]').forEach(button => {
    button.onclick = async () => {
      const action = String(button.dataset.billingFocusAction || '').trim();
      if (!action) return;
      if (action === 'open-plans') {
        setView('account');
        setAccountNotice('Selecciona la duración que quieres comprar o renovar en los bloques de planes.', 'accent');
        renderAccount();
        bindViewButtons();
        focusPlanBlock();
        return;
      }
      if (action === 'focus-order') {
        setView('account');
        setAccountNotice('Revisa el bloque de pago actual para copiar la wallet, el monto exacto o confirmar la orden.', 'accent');
        renderAccount();
        bindViewButtons();
        focusPaymentCard();
        return;
      }
      if (action === 'refresh-account') {
        setAccountNotice('Actualizando el estado comercial...', 'accent');
        renderAccount();
        bindViewButtons();
        try {
          await refreshAccountState();
          setView('account');
          setAccountNotice('Cuenta actualizada correctamente.', 'positive');
          renderAccount();
          bindViewButtons();
        } catch (error) {
          setAccountNotice(paymentReasonMessage(error.message, error.message || 'No se pudo refrescar la cuenta.'), 'warning');
          renderAccount();
          bindViewButtons();
        }
      }
    };
  });
  document.querySelectorAll('[data-open-performance-center]').forEach(button => {
    button.onclick = () => openPerformanceCenter({ days: button.dataset.performanceDays || 30 });
  });
  document.querySelectorAll('[data-performance-window]').forEach(button => {
    button.onclick = () => openPerformanceCenter({ days: button.dataset.performanceWindow || 30 });
  });
  document.querySelectorAll('[data-performance-refresh]').forEach(button => {
    button.onclick = async () => {
      setPerformanceNotice('Actualizando rendimiento...', 'accent');
      renderPerformance();
      bindViewButtons();
      try {
        await refreshPerformanceCenter(true, state.performanceCenter.query || {});
        setPerformanceNotice('Rendimiento actualizado correctamente.', 'positive');
      } catch (_) {}
    };
  });
  document.querySelectorAll('[data-open-settings-center]').forEach(button => {
    button.onclick = () => openSettingsCenter(false);
  });
  document.querySelectorAll('[data-settings-refresh]').forEach(button => {
    button.onclick = async () => {
      setSettingsNotice('Actualizando ajustes...', 'accent');
      renderSettings();
      bindViewButtons();
      try {
        await refreshSettingsCenter(true);
        setSettingsNotice('Ajustes actualizados correctamente.', 'positive');
      } catch (_) {}
    };
  });
  document.querySelectorAll('[data-settings-save]').forEach(button => {
    button.onclick = async () => {
      if (button.disabled) return;
      const original = button.textContent;
      button.disabled = true;
      button.textContent = 'Guardando...';
      try {
        const patch = collectSettingsPatch();
        setSettingsNotice('Guardando ajustes...', 'accent');
        renderSettings();
        bindViewButtons();
        state.settingsCenter.payload = await api('/api/miniapp/settings', {
          method: 'POST',
          body: JSON.stringify(patch),
        });
        await refreshAccountState();
        setSettingsNotice('Ajustes guardados correctamente.', 'positive');
        renderAll();
        setView('settings');
      } catch (error) {
        setSettingsNotice(`No se pudieron guardar los ajustes: ${error.message || 'error'}`, 'warning');
        renderSettings();
        bindViewButtons();
        tg?.showAlert(`No se pudieron guardar los ajustes: ${error.message || 'error'}`);
      } finally {
        if (button.isConnected) {
          button.disabled = false;
          button.textContent = original;
        }
      }
    };
  });
  document.querySelectorAll('[data-open-risk-center]').forEach(button => {
    button.onclick = () => openRiskCenter({});
  });
  document.querySelectorAll('[data-open-risk-signal]').forEach(button => {
    button.onclick = () => openRiskCenter({
      signalId: button.dataset.openRiskSignal,
      profile: state.riskCenter.payload?.signals?.selected_profile || state.riskCenter.payload?.profile?.default_profile || null,
      leverage: state.riskCenter.query?.leverage || null,
    });
  });
  document.querySelectorAll('[data-risk-refresh]').forEach(button => {
    button.onclick = async () => {
      setRiskNotice('Actualizando gestión de riesgo...', 'accent');
      renderRisk();
      bindViewButtons();
      try {
        await refreshRiskCenter(true, state.riskCenter.query || {});
        setRiskNotice('Gestión de riesgo actualizada correctamente.', 'positive');
      } catch (_) {}
    };
  });
  document.querySelectorAll('[data-risk-apply-preset]').forEach(button => {
    button.onclick = () => {
      const applied = applyRiskPresetToInputs();
      setRiskNotice(applied ? 'Preset de exchange cargado en fee y slippage.' : 'No pude aplicar el preset para esa combinación.', applied ? 'accent' : 'warning');
      tg?.showAlert(applied ? 'Preset de exchange cargado en fee y slippage.' : 'No pude aplicar el preset para esa combinación.');
    };
  });
  document.querySelectorAll('[data-risk-save-profile]').forEach(button => {
    button.onclick = async () => {
      if (button.disabled) return;
      const original = button.textContent;
      button.disabled = true;
      button.textContent = 'Guardando...';
      try {
        const patch = collectRiskProfilePatch();
        setRiskNotice('Guardando perfil de riesgo...', 'accent');
        renderRisk();
        bindViewButtons();
        await api('/api/miniapp/risk/profile', {
          method: 'POST',
          body: JSON.stringify(patch),
        });
        await refreshRiskCenter(true, state.riskCenter.query || {});
        setRiskNotice('Perfil de riesgo guardado correctamente.', 'positive');
        renderRisk();
        bindViewButtons();
      } catch (error) {
        setRiskNotice(`No se pudo guardar el perfil: ${error.message || 'error'}`, 'warning');
        renderRisk();
        bindViewButtons();
        tg?.showAlert(`No se pudo guardar el perfil: ${error.message || 'error'}`);
      } finally {
        if (button.isConnected) {
          button.disabled = false;
          button.textContent = original;
        }
      }
    };
  });
  document.querySelectorAll('[data-risk-select-profile]').forEach(button => {
    button.onclick = () => openRiskCenter({
      signalId: state.riskCenter.payload?.signals?.selected_signal_id || state.riskCenter.query?.signalId || null,
      profile: button.dataset.riskSelectProfile || null,
      leverage: document.getElementById('riskPreviewLeverageInput')?.value || state.riskCenter.query?.leverage || null,
    });
  });
  document.querySelectorAll('[data-risk-preview-run]').forEach(button => {
    button.onclick = () => openRiskCenter({
      signalId: state.riskCenter.payload?.signals?.selected_signal_id || state.riskCenter.query?.signalId || null,
      profile: state.riskCenter.payload?.signals?.selected_profile || state.riskCenter.query?.profile || state.riskCenter.payload?.profile?.default_profile || null,
      leverage: document.getElementById('riskPreviewLeverageInput')?.value || null,
    });
  });
  document.querySelectorAll('[data-risk-clear-selection]').forEach(button => {
    button.onclick = () => openRiskCenter({ signalId: null, profile: null, leverage: null });
  });
  document.querySelectorAll('[data-open-admin-panel]').forEach(button => {
    button.onclick = () => openAdminPanel(false);
  });
  document.querySelectorAll('[data-admin-refresh]').forEach(button => {
    button.onclick = async () => {
      setAdminNotice('Actualizando panel admin...', 'accent');
      renderAdmin();
      bindViewButtons();
      try {
        await refreshAdminOverview(true);
        setAdminNotice('Panel admin actualizado correctamente.', 'positive');
      } catch (_) {}
    };
  });
  document.querySelectorAll('[data-admin-plan-select]').forEach(button => {
    button.onclick = () => {
      state.adminPanel.manualActivation.draft.plan = String(button.dataset.adminPlanSelect || 'plus').toLowerCase();
      renderAdmin();
      bindViewButtons();
    };
  });
  document.querySelectorAll('[data-admin-plan-lookup]').forEach(button => {
    button.onclick = async () => {
      const input = document.getElementById('adminManualPlanUserIdInput');
      const rawUserId = String(input?.value || state.adminPanel.manualActivation.draft.userId || '').trim();
      if (!rawUserId) {
        setAdminNotice('Introduce el ID de Telegram del usuario que quieres gestionar.', 'warning');
        renderAdmin();
        bindViewButtons();
        tg?.showAlert('Introduce el ID de Telegram del usuario.');
        return;
      }
      state.adminPanel.manualActivation.draft.userId = rawUserId;
      state.adminPanel.manualActivation.lookupLoading = true;
      setAdminNotice('Buscando usuario para activación manual...', 'accent');
      renderAdmin();
      bindViewButtons();
      try {
        const payload = await api(`/api/miniapp/admin/user-lookup?user_id=${encodeURIComponent(rawUserId)}`);
        state.adminPanel.manualActivation.lookup = payload;
        state.adminPanel.moderation.confirmAction = null;
        const defaultPlan = (payload.plan_options || []).find(item => item.available)?.key || 'plus';
        state.adminPanel.manualActivation.draft.plan = String(defaultPlan || 'plus').toLowerCase();
        if (!String(state.adminPanel.manualActivation.draft.days || '').trim()) {
          state.adminPanel.manualActivation.draft.days = '30';
        }
        setAdminNotice('Usuario cargado. Selecciona plan y días antes de activar.', 'positive');
      } catch (error) {
        state.adminPanel.manualActivation.lookup = null;
        setAdminNotice(`No se pudo cargar el usuario: ${error.message || 'error'}`, 'warning');
        tg?.showAlert(`No se pudo cargar el usuario: ${error.message || 'error'}`);
      } finally {
        state.adminPanel.manualActivation.lookupLoading = false;
        renderAdmin();
        bindViewButtons();
      }
    };
  });
  document.querySelectorAll('[data-admin-plan-activate]').forEach(button => {
    button.onclick = async () => {
      const lookup = state.adminPanel.manualActivation.lookup;
      const target = lookup?.target;
      const rawUserId = String(document.getElementById('adminManualPlanUserIdInput')?.value || state.adminPanel.manualActivation.draft.userId || target?.user_id || '').trim();
      const rawDays = String(document.getElementById('adminManualPlanDaysInput')?.value || state.adminPanel.manualActivation.draft.days || '').trim();
      const selectedPlan = String(state.adminPanel.manualActivation.draft.plan || '').toLowerCase() || 'plus';
      if (!rawUserId || !target) {
        setAdminNotice('Busca primero el usuario antes de intentar activar un plan.', 'warning');
        renderAdmin();
        bindViewButtons();
        tg?.showAlert('Busca primero el usuario.');
        return;
      }
      if (!rawDays) {
        setAdminNotice('Introduce la cantidad exacta de días para la activación manual.', 'warning');
        renderAdmin();
        bindViewButtons();
        tg?.showAlert('Introduce la cantidad de días.');
        return;
      }
      state.adminPanel.manualActivation.draft.userId = rawUserId;
      state.adminPanel.manualActivation.draft.days = rawDays;
      state.adminPanel.manualActivation.activationLoading = true;
      setAdminNotice(`Aplicando ${selectedPlan.toUpperCase()} por ${rawDays} días al usuario ${rawUserId}...`, 'accent');
      renderAdmin();
      bindViewButtons();
      try {
        const result = await api('/api/miniapp/admin/manual-plan-activation', {
          method: 'POST',
          body: JSON.stringify({ user_id: Number(rawUserId), plan: selectedPlan, days: Number(rawDays) }),
        });
        state.adminPanel.manualActivation.lookup = {
          ...(lookup || {}),
          target: result.target || lookup?.target || null,
          plan_options: result.plan_options || lookup?.plan_options || [],
          moderation: result.moderation || lookup?.moderation || {},
        };
        state.adminPanel.manualActivation.draft.plan = String(result.activation?.plan || selectedPlan).toLowerCase();
        state.adminPanel.manualActivation.draft.days = String(result.activation?.days || rawDays);
        state.adminPanel.overview = null;
        setAdminNotice(`Plan ${String(result.activation?.plan_name || selectedPlan).toUpperCase()} activado por ${result.activation?.days || rawDays} días para el usuario ${rawUserId}.`, 'positive');
        await refreshAdminOverview(true);
      } catch (error) {
        setAdminNotice(`No se pudo activar el plan manual: ${error.message || 'error'}`, 'warning');
        tg?.showAlert(`No se pudo activar el plan manual: ${error.message || 'error'}`);
      } finally {
        state.adminPanel.manualActivation.activationLoading = false;
        renderAdmin();
        bindViewButtons();
      }
    };
  });


  document.querySelectorAll('[data-admin-moderation-action]').forEach(button => {
    button.onclick = () => {
      const lookup = state.adminPanel.manualActivation.lookup;
      const target = lookup?.target;
      if (!target) {
        setAdminNotice('Busca primero el usuario antes de aplicar moderación.', 'warning');
        renderAdmin();
        bindViewButtons();
        return;
      }
      const action = String(button.dataset.adminModerationAction || '').trim();
      const durationValue = String(document.getElementById('adminModerationDurationValueInput')?.value || state.adminPanel.moderation.draft.durationValue || '').trim();
      const durationUnit = String(document.getElementById('adminModerationDurationUnitSelect')?.value || state.adminPanel.moderation.draft.durationUnit || 'days').trim();
      state.adminPanel.moderation.draft.durationValue = durationValue || state.adminPanel.moderation.draft.durationValue;
      state.adminPanel.moderation.draft.durationUnit = durationUnit || 'days';
      const messages = {
        ban_temporary: `Vas a banear temporalmente al usuario ${target.user_id} por ${durationValue || '?'} ${durationUnit}.`,
        ban_permanent: `Vas a aplicar baneo permanente al usuario ${target.user_id}.`,
        unban: `Vas a levantar el baneo del usuario ${target.user_id}.`,
        delete: `Vas a eliminar el usuario ${target.user_id} y sus datos operativos asociados.`,
      };
      if (action === 'ban_temporary' && !durationValue) {
        setAdminNotice('Introduce la duración del baneo temporal antes de continuar.', 'warning');
        renderAdmin();
        bindViewButtons();
        return;
      }
      state.adminPanel.moderation.confirmAction = {
        action,
        userId: Number(target.user_id),
        durationValue: durationValue ? Number(durationValue) : null,
        durationUnit,
        message: messages[action] || 'Confirma la acción administrativa.',
      };
      setAdminNotice('Confirma la acción administrativa antes de ejecutarla.', 'warning');
      renderAdmin();
      bindViewButtons();
    };
  });
  document.querySelectorAll('[data-admin-moderation-cancel]').forEach(button => {
    button.onclick = () => {
      state.adminPanel.moderation.confirmAction = null;
      setAdminNotice('Acción de moderación cancelada.', 'neutral');
      renderAdmin();
      bindViewButtons();
    };
  });
  document.querySelectorAll('[data-admin-moderation-confirm]').forEach(button => {
    button.onclick = async () => {
      const confirm = state.adminPanel.moderation.confirmAction;
      if (!confirm) return;
      state.adminPanel.moderation.actionLoading = true;
      setAdminNotice('Ejecutando acción de moderación...', 'accent');
      renderAdmin();
      bindViewButtons();
      try {
        const result = await api('/api/miniapp/admin/user-moderation', {
          method: 'POST',
          body: JSON.stringify({
            user_id: Number(confirm.userId),
            action: confirm.action,
            duration_value: confirm.durationValue,
            duration_unit: confirm.durationUnit,
            confirm: true,
          }),
        });
        state.adminPanel.moderation.confirmAction = null;
        const target = result.target || null;
        state.adminPanel.manualActivation.lookup = target ? {
          ...(state.adminPanel.manualActivation.lookup || {}),
          target,
          plan_options: result.plan_options || (state.adminPanel.manualActivation.lookup?.plan_options || []),
          moderation: result.moderation || {},
        } : null;
        state.adminPanel.overview = null;
        const messages = {
          ban_temporary: `Baneo temporal aplicado al usuario ${confirm.userId}.`,
          ban_permanent: `Baneo permanente aplicado al usuario ${confirm.userId}.`,
          unban: `Baneo levantado para el usuario ${confirm.userId}.`,
          delete: `Usuario ${confirm.userId} eliminado correctamente.`,
        };
        setAdminNotice(messages[confirm.action] || 'Acción ejecutada correctamente.', 'positive');
        await refreshAdminOverview(true);
      } catch (error) {
        setAdminNotice(`No se pudo ejecutar la moderación: ${error.message || 'error'}`, 'warning');
        tg?.showAlert(`No se pudo ejecutar la moderación: ${error.message || 'error'}`);
      } finally {
        state.adminPanel.moderation.actionLoading = false;
        renderAdmin();
        bindViewButtons();
      }
    };
  });

  document.querySelectorAll('[data-admin-reset-request]').forEach(button => {
    button.onclick = () => {
      state.adminPanel.confirmReset = true;
      setAdminNotice('Confirma el reset antes de ejecutar la limpieza total de resultados.', 'warning');
      renderAdmin();
      bindViewButtons();
    };
  });
  document.querySelectorAll('[data-admin-reset-cancel]').forEach(button => {
    button.onclick = () => {
      state.adminPanel.confirmReset = false;
      setAdminNotice('Reset cancelado. No se tocó ningún dato.', 'neutral');
      renderAdmin();
      bindViewButtons();
    };
  });
  document.querySelectorAll('[data-admin-reset-confirm]').forEach(button => {
    button.onclick = async () => {
      if (button.disabled) return;
      const original = button.textContent;
      button.disabled = true;
      button.textContent = 'Reseteando...';
      try {
        setAdminNotice('Ejecutando reset total de resultados...', 'accent');
        renderAdmin();
        bindViewButtons();
        const result = await api('/api/miniapp/admin/reset-results', {
          method: 'POST',
          body: JSON.stringify({ confirm: true }),
        });
        state.adminPanel.confirmReset = false;
        state.adminPanel.lastResetSummary = result.summary || null;
        state.adminPanel.overview = null;
        setAdminNotice('Reset ejecutado correctamente. El histórico y las estadísticas activas arrancan desde cero.', 'positive');
        await Promise.allSettled([refreshAccountState(), refreshAdminOverview(true)]);
        renderAll();
        setView('admin');
      } catch (error) {
        setAdminNotice(`No se pudo ejecutar el reset: ${error.message || 'error'}`, 'warning');
        renderAdmin();
        bindViewButtons();
        tg?.showAlert(`No se pudo ejecutar el reset: ${error.message || 'error'}`);
      } finally {
        if (button.isConnected) {
          button.disabled = false;
          button.textContent = original;
        }
      }
    };
  });
  document.querySelectorAll('[data-hx-filter]').forEach(btn => {
    btn.onclick = () => {
      state.historyFilter = btn.dataset.hxFilter || 'all';
      renderHistory();
      bindViewButtons();
    };
  });
  document.querySelectorAll('[data-goto]').forEach(button => {
    button.onclick = () => setView(button.dataset.goto);
  });
  document.querySelectorAll('[data-copy-value]').forEach(button => {
    button.onclick = () => copyValue(button.dataset.copyValue, 'Copiado correctamente.');
  });
  document.querySelectorAll('[data-signal-detail]').forEach(button => {
    button.onclick = () => openSignalDetail(button.dataset.signalDetail, 'moderado');
  });
  document.querySelectorAll('[data-radar-detail]').forEach(button => {
    button.onclick = () => openRadarDetail(button.dataset.radarDetail);
  });
  document.querySelectorAll('[data-radar-open-signal]').forEach(button => {
    button.onclick = () => openSignalDetail(button.dataset.radarOpenSignal, 'moderado');
  });
  document.querySelectorAll('[data-signal-profile]').forEach(button => {
    button.onclick = () => openSignalDetail(button.dataset.signalId, button.dataset.signalProfile);
  });
  if (els.signalDetailClose) {
    els.signalDetailClose.onclick = () => closeSignalDetailModal();
  }
  if (els.signalDetailModal) {
    els.signalDetailModal.onclick = (event) => {
      if (event.target === els.signalDetailModal) closeSignalDetailModal();
    };
  }
  const watchlistInput = document.getElementById('watchlistInput');
  document.querySelectorAll('[data-watchlist-add]').forEach(button => {
    button.onclick = async () => {
      const raw = (watchlistInput?.value || '').trim();
      if (!raw) {
        tg?.showAlert('Escribe al menos un símbolo.');
        return;
      }
      try {
        const symbols = raw.split(/[\s,;]+/).map(item => item.trim()).filter(Boolean);
        if (symbols.length > 1) {
          await mutateWatchlist('/api/miniapp/watchlist/replace', { symbols: [...new Set([...(state.payload.watchlist_meta?.symbols || []), ...symbols])] });
        } else {
          await mutateWatchlist('/api/miniapp/watchlist/add', { symbol: raw });
        }
        if (watchlistInput) watchlistInput.value = '';
      } catch (error) {
        tg?.showAlert(error.message || 'No se pudo añadir a watchlist.');
      }
    };
  });
  document.querySelectorAll('[data-watchlist-replace]').forEach(button => {
    button.onclick = async () => {
      const raw = (watchlistInput?.value || '').trim();
      try {
        await mutateWatchlist('/api/miniapp/watchlist/replace', { raw });
        if (watchlistInput) watchlistInput.value = '';
      } catch (error) {
        tg?.showAlert(error.message || 'No se pudo reemplazar la watchlist.');
      }
    };
  });
  document.querySelectorAll('[data-watchlist-clear]').forEach(button => {
    button.onclick = async () => {
      try {
        await mutateWatchlist('/api/miniapp/watchlist/clear');
      } catch (error) {
        tg?.showAlert(error.message || 'No se pudo limpiar la watchlist.');
      }
    };
  });
  document.querySelectorAll('[data-watchlist-remove]').forEach(button => {
    button.onclick = async () => {
      try {
        await mutateWatchlist('/api/miniapp/watchlist/remove', { symbol: button.dataset.watchlistRemove });
      } catch (error) {
        tg?.showAlert(error.message || 'No se pudo eliminar el símbolo.');
      }
    };
  });

  const radarSearchInput = document.getElementById('radarSearchInput');
  if (radarSearchInput) {
    radarSearchInput.oninput = () => {
      state.radarView = { ...state.radarView, search: radarSearchInput.value || '', offset: 0 };
      renderMarket();
      bindViewButtons();
    };
  }
  const radarDirectionFilter = document.getElementById('radarDirectionFilter');
  if (radarDirectionFilter) {
    radarDirectionFilter.onchange = () => {
      state.radarView = { ...state.radarView, direction: radarDirectionFilter.value || 'all', offset: 0 };
      renderMarket();
      bindViewButtons();
    };
  }
  const radarPriorityFilter = document.getElementById('radarPriorityFilter');
  if (radarPriorityFilter) {
    radarPriorityFilter.onchange = () => {
      state.radarView = { ...state.radarView, priority: radarPriorityFilter.value || 'all', offset: 0 };
      renderMarket();
      bindViewButtons();
    };
  }
  const radarProximityFilter = document.getElementById('radarProximityFilter');
  if (radarProximityFilter) {
    radarProximityFilter.onchange = () => {
      state.radarView = { ...state.radarView, proximity: radarProximityFilter.value || 'all', offset: 0 };
      renderMarket();
      bindViewButtons();
    };
  }
  const radarSignalFilter = document.getElementById('radarSignalFilter');
  if (radarSignalFilter) {
    radarSignalFilter.onchange = () => {
      state.radarView = { ...state.radarView, signal: radarSignalFilter.value || 'all', offset: 0 };
      renderMarket();
      bindViewButtons();
    };
  }
  const radarExecutionFilter = document.getElementById('radarExecutionFilter');
  if (radarExecutionFilter) {
    radarExecutionFilter.onchange = () => {
      state.radarView = { ...state.radarView, execution: radarExecutionFilter.value || 'all', offset: 0 };
      renderMarket();
      bindViewButtons();
    };
  }
  const radarAlignmentFilter = document.getElementById('radarAlignmentFilter');
  if (radarAlignmentFilter) {
    radarAlignmentFilter.onchange = () => {
      state.radarView = { ...state.radarView, alignment: radarAlignmentFilter.value || 'all', offset: 0 };
      renderMarket();
      bindViewButtons();
    };
  }
  const radarSortFilter = document.getElementById('radarSortFilter');
  if (radarSortFilter) {
    radarSortFilter.onchange = () => {
      state.radarView = { ...state.radarView, sort: radarSortFilter.value || 'ranking', offset: 0 };
      renderMarket();
      bindViewButtons();
    };
  }
  document.querySelectorAll('[data-market-refresh]').forEach(button => {
    button.onclick = async () => {
      if (button.disabled) return;
      button.disabled = true;
      try {
        await refreshMarketState(true);
      } finally {
        if (button.isConnected) button.disabled = false;
      }
    };
  });
  document.querySelectorAll('[data-radar-reset]').forEach(button => {
    button.onclick = () => {
      resetRadarView();
      renderMarket();
      bindViewButtons();
    };
  });
  // ── Radar chip filters (toggle chips) ────────────────────────────────────
  document.querySelectorAll('[data-radar-chip]').forEach(chip => {
    chip.onclick = () => {
      const filter = chip.dataset.filter;
      const value  = chip.dataset.value;
      if (!filter) return;
      state.radarView = { ...state.radarView, [filter]: value, offset: 0 };
      renderMarket();
      bindViewButtons();
    };
  });
  // ── Radar card expand/collapse ────────────────────────────────────────────
  document.querySelectorAll('[data-radar-toggle]').forEach(toggle => {
    toggle.onclick = (e) => {
      e.stopPropagation();
      const card = toggle.closest('.radar-card-v2');
      if (!card) return;
      const expanded = card.dataset.radarExpanded === 'true';
      card.dataset.radarExpanded = expanded ? 'false' : 'true';
      const content = card.querySelector('.radar-card-expanded-content');
      const btn     = card.querySelector('.radar-card-expand-btn');
      if (content) content.style.display = expanded ? 'none' : 'block';
      if (btn)     btn.textContent        = expanded ? 'Expandir' : 'Colapsar';
    };
  });
  document.querySelectorAll('[data-radar-rotate]').forEach(button => {
    button.onclick = () => {
      const market = state.payload?.market || {};
      const radarItems = getRadarPresentation(market.radar || [], state.radarView || DEFAULT_RADAR_VIEW);
      rotateRadarWindow(radarItems, RADAR_VISIBLE_COUNT);
      renderMarket();
      bindViewButtons();
    };
  });
  document.querySelectorAll('[data-radar-follow]').forEach(button => {
    button.onclick = async () => {
      try {
        await mutateWatchlist('/api/miniapp/watchlist/add', { symbol: button.dataset.radarFollow }, 'Añadido a watchlist.');
      } catch (error) {
        tg?.showAlert(error.message || 'No se pudo seguir el símbolo.');
      }
    };
  });

  document.querySelectorAll('[data-create-order]').forEach(button => {
    button.onclick = async () => {
      if (button.disabled) return;
      const [plan, days] = button.dataset.createOrder.split(':');
      const original = button.textContent;
      button.disabled = true;
      button.textContent = 'Procesando...';
      try {
        setAccountNotice('Generando orden de pago...', 'accent');
        renderAccount();
        bindViewButtons();
        const result = await api('/api/miniapp/payment-order', {
          method: 'POST',
          body: JSON.stringify({ plan, days: Number(days) }),
        });
        applyPaymentOrderPreview(result.order || null);
        setAccountNotice('Orden de pago lista. Revisa el bloque de pago para copiar la wallet, el monto exacto y confirmar.', 'positive');
        renderAll();
        setView('account');
        focusPaymentCard();
        Promise.resolve(refreshAccountState())
          .then(() => {
            setView('account');
            focusPaymentCard();
          })
          .catch(refreshError => {
            console.warn('MiniApp account refresh after create order failed', refreshError);
          });
        tg?.showAlert('Orden de pago lista. Revisa el bloque de pago para copiar la wallet, el monto exacto y confirmar.');
      } catch (error) {
        setAccountNotice(`No se pudo generar la orden: ${paymentReasonMessage(error.message, error.message)}`, 'warning');
        renderAccount();
        bindViewButtons();
        tg?.showAlert(`No se pudo generar la orden: ${paymentReasonMessage(error.message, error.message)}`);
      } finally {
        if (button.isConnected) {
          button.disabled = false;
          button.textContent = original;
        }
      }
    };
  });
  document.querySelectorAll('[data-confirm-order]').forEach(button => {
    button.onclick = async () => {
      if (button.disabled) return;
      const original = button.textContent;
      button.disabled = true;
      button.textContent = 'Verificando...';
      try {
        setAccountNotice('Verificando el pago...', 'accent');
        renderAccount();
        bindViewButtons();
        const result = await api('/api/miniapp/payment-order/confirm', {
          method: 'POST',
          body: JSON.stringify({ order_id: button.dataset.confirmOrder }),
        });
        applyPaymentOrderPreview(result.order || null);
        setAccountNotice(paymentReasonMessage(result.reason, result.ok ? 'Estado de pago actualizado.' : 'Pago pendiente.'), result.ok ? 'positive' : 'warning');
        renderAll();
        setView('account');
        focusPaymentCard();
        Promise.resolve(refreshAccountState())
          .then(() => {
            setView('account');
            focusPaymentCard();
          })
          .catch(refreshError => {
            console.warn('MiniApp account refresh after confirm payment failed', refreshError);
          });
        tg?.showAlert(paymentReasonMessage(result.reason, result.ok ? 'Estado de pago actualizado.' : 'Pago pendiente.'));
      } catch (error) {
        setAccountNotice(`No se pudo confirmar: ${paymentReasonMessage(error.message, error.message)}`, 'warning');
        renderAccount();
        bindViewButtons();
        tg?.showAlert(`No se pudo confirmar: ${paymentReasonMessage(error.message, error.message)}`);
      } finally {
        if (button.isConnected) {
          button.disabled = false;
          button.textContent = original;
        }
      }
    };
  });
  document.querySelectorAll('[data-cancel-order]').forEach(button => {
    button.onclick = async () => {
      if (button.disabled) return;
      const original = button.textContent;
      button.disabled = true;
      button.textContent = 'Cancelando...';
      try {
        setAccountNotice('Cancelando orden de pago...', 'accent');
        renderAccount();
        bindViewButtons();
        await api('/api/miniapp/payment-order/cancel', {
          method: 'POST',
          body: JSON.stringify({ order_id: button.dataset.cancelOrder }),
        });
        applyPaymentOrderPreview(null);
        setAccountNotice('Orden cancelada correctamente.', 'positive');
        renderAll();
        setView('account');
        Promise.resolve(refreshAccountState())
          .then(() => {
            setView('account');
          })
          .catch(refreshError => {
            console.warn('MiniApp account refresh after cancel order failed', refreshError);
          });
        tg?.showAlert('Orden cancelada correctamente.');
      } catch (error) {
        setAccountNotice(`No se pudo cancelar: ${paymentReasonMessage(error.message, error.message)}`, 'warning');
        renderAccount();
        bindViewButtons();
        tg?.showAlert(`No se pudo cancelar: ${paymentReasonMessage(error.message, error.message)}`);
      } finally {
        if (button.isConnected) {
          button.disabled = false;
          button.textContent = original;
        }
      }
    };
  });
}

document.addEventListener('visibilitychange', () => {
  if (document.hidden) {
    stopLiveSignalsPolling();
    return;
  }
  syncLiveSignalsPolling();
  queueLiveSignalsRefresh('visibility');
});

window.addEventListener('focus', () => {
  if (document.hidden) return;
  syncLiveSignalsPolling();
  queueLiveSignalsRefresh('window-focus');
});

const SPLASH_FEATURES = [
  {
    icon: '⚡',
    name: 'Señales en Vivo',
    desc: 'Alertas de alta precisión en tiempo real',
    detail: 'El motor de HADES detecta setups de alta probabilidad en criptomonedas. Cada señal incluye precio de entrada, stop loss y dos take profits calculados por perfil: conservador, moderado y agresivo. Se envían por Telegram y se siguen en vivo con tracking intrabar desde la mini app.',
  },
  {
    icon: '🎯',
    name: 'Cazador de Liquidez',
    desc: 'Estrategia de barridas institucionales',
    detail: 'Identifica zonas donde se acumula liquidez institucional. Detecta barridas de stops (liquidity sweeps) y espera el pullback ideal para entrar en la dirección real del mercado. Incluye zona de entrada precisa, nivel de invalidación y objetivos calculados por ratio riesgo/recompensa.',
  },
  {
    icon: '📡',
    name: 'Radar de Mercado',
    desc: 'Escaneo continuo del universo cripto',
    detail: 'El radar escanea múltiples pares de criptomonedas en tiempo real y asigna puntuaciones de setup. Muestra qué activos están en zona de oportunidad, cuáles tienen momentum alcista o bajista, y el grado de alineación con la estrategia activa.',
  },
  {
    icon: '📊',
    name: 'Historial',
    desc: 'Registro completo de resultados',
    detail: 'Accede a todas las señales emitidas con sus resultados finales: TP1, TP2, SL o expirada. Incluye múltiplo R conseguido, tiempo de resolución y progreso máximo alcanzado. Filtra por estrategia, dirección y ventana temporal.',
  },
  {
    icon: '📈',
    name: 'Rendimiento',
    desc: 'Métricas reales de la plataforma',
    detail: 'Panel de métricas avanzadas: tasa de acierto por estrategia, R promedio, drawdown, rachas ganadoras y perdedoras, y comparativa en ventanas de 7, 14 y 30 días. Todo con datos reales de la plataforma, sin trampa.',
  },
  {
    icon: '🛡️',
    name: 'Gestión de Riesgo',
    desc: 'Calculadora de sizing y capital',
    detail: 'Configura tu capital total, riesgo máximo por operación en %, exchange, fees y slippage. HADES calcula automáticamente el tamaño de posición exacto para cada señal activa según tu perfil de riesgo, incluyendo el apalancamiento sugerido.',
  },
  {
    icon: '👁️',
    name: 'Watchlist',
    desc: 'Vigilancia de tus activos favoritos',
    detail: 'Crea tu lista de criptomonedas a vigilar. HADES los monitoriza de forma prioritaria y te alerta antes que al resto cuando se forma un setup válido en alguno de tus pares favoritos.',
  },
  {
    icon: '🔮',
    name: 'Oraculum',
    desc: 'Predicciones filtradas con objetivo operativo',
    detail: 'Oraculum es la plataforma de inteligencia predictiva del ecosistema HADES. Analiza pares de alto interés, filtra señales LONG y SHORT con objetivo coherente, contexto de mercado y horizonte operativo definido. Está pensada para consultar oportunidades accionables sin ruido, con acceso premium desde tu cuenta.',
  },
  {
    icon: '🛡️',
    name: 'HADES Sentinel',
    desc: 'Terminal de riesgo y contexto antes de operar',
    detail: 'HADES Sentinel es la capa defensiva del ecosistema. Resume riesgo operativo, anomalías de mercado, estrés de futuros, presión, noticias críticas y alertas internas para que revises el contexto antes de abrir una operación. Está diseñada como panel de validación previa para usuarios premium.',
  },
  {
    icon: '⚙️',
    name: 'Cuenta',
    desc: 'Suscripción y preferencias',
    detail: 'Gestiona tu plan (Free, Plus, Premium), consulta tu estado de cuenta, configura el idioma de la interfaz y ajusta las preferencias de notificación. Desde aquí también puedes vincular y abrir Oraculum y HADES Sentinel si tu plan premium está activo.',
  },
];

function _createSplashParticles() {
  const container = document.createElement('div');
  container.className = 'splash-particles';
  const colors = ['#FFD700', '#FF8C00', '#FF4500', '#FFA500', '#FFEC00'];
  const sizes  = [3, 4, 5, 3, 6, 4, 3, 5, 4, 6, 3, 5, 4, 3, 6, 5];
  for (let i = 0; i < 18; i++) {
    const s = document.createElement('div');
    s.className = 'spark';
    const size = sizes[i % sizes.length];
    s.style.cssText = `
      width:${size}px; height:${size}px;
      background:${colors[i % colors.length]};
      left:${Math.random() * 100}%;
      bottom:${Math.random() * 20}%;
      animation-duration:${4 + Math.random() * 6}s;
      animation-delay:${Math.random() * 5}s;
    `;
    container.appendChild(s);
  }
  return container;
}

function _createSplashCards() {
  const grid = document.createElement('div');
  grid.className = 'splash-cards-grid';
  SPLASH_FEATURES.forEach((f, i) => {
    const card = document.createElement('div');
    card.className = 'splash-card';
    card.style.animationDelay = `${0.7 + i * 0.08}s`;
    card.innerHTML = `
      <div class="splash-card-icon">${f.icon}</div>
      <div class="splash-card-name">${f.name}</div>
      <div class="splash-card-desc">${f.desc}</div>
    `;
    card.addEventListener('click', () => openSplashModal(i));
    grid.appendChild(card);
  });
  return grid;
}

function openSplashModal(index) {
  const f = SPLASH_FEATURES[index];
  const overlay = document.createElement('div');
  overlay.className = 'splash-modal-overlay';
  overlay.id = 'splash-feature-modal';
  overlay.innerHTML = `
    <div class="splash-modal-sheet">
      <div class="splash-modal-header">
        <div class="splash-modal-icon">${f.icon}</div>
        <button class="splash-modal-close" aria-label="Cerrar">✕</button>
      </div>
      <div class="splash-modal-name">${f.name}</div>
      <div class="splash-modal-short">${f.desc}</div>
      <div class="splash-modal-body">${f.detail}</div>
    </div>
  `;
  overlay.querySelector('.splash-modal-close').addEventListener('click', closeSplashModal);
  overlay.addEventListener('click', (e) => { if (e.target === overlay) closeSplashModal(); });
  document.body.appendChild(overlay);
}

function closeSplashModal() {
  const modal = document.getElementById('splash-feature-modal');
  if (modal) {
    modal.style.transition = 'opacity 0.2s';
    modal.style.opacity = '0';
    setTimeout(() => modal.remove(), 200);
  }
}

function showSplash() {
  const splash = document.createElement('div');
  splash.id = 'hades-splash';

  // Ocultar la pantalla de loading original
  const loadingEl = document.getElementById('loading');
  if (loadingEl) loadingEl.classList.add('hidden');

  splash.appendChild(_createSplashParticles());

  const inner = document.createElement('div');
  inner.className = 'splash-inner';
  inner.innerHTML = `
    <div class="splash-logo-wrap">
      <img class="splash-logo" src="/miniapp/static/logo.png" alt="HADES ALPHA V2" draggable="false">
      <div class="splash-title">HADES ALPHA V2</div>
      <div class="splash-tagline">El sistema que opera en las sombras</div>
    </div>
    <button class="splash-enter-btn" id="splashEnterBtn">⚔️&nbsp;&nbsp;ENTRAR A HADES</button>
    <div class="splash-arsenal-title">🔱 Arsenal de HADES</div>
    <div class="splash-arsenal-line"></div>
  `;
  inner.appendChild(_createSplashCards());
  splash.appendChild(inner);
  document.body.appendChild(splash);

  document.getElementById('splashEnterBtn').addEventListener('click', dismissSplash);
}

function dismissSplash() {
  const splash = document.getElementById('hades-splash');
  if (!splash) return;
  splash.style.transition = 'opacity 0.4s ease';
  splash.style.opacity = '0';
  setTimeout(() => {
    splash.remove();
    // Si el bootstrap ya terminó, renderizar normalmente
    primePayloadShell(window._hadesAuthMe || {});
    renderAll();
    const restored = Boolean(window._hadesRestoredFromCache);
    ensureViewData(state.currentView || 'home', { force: !restored });
    syncLiveSignalsPolling();
    if (shouldPollLiveSignals()) {
      setTimeout(() => {
        queueLiveSignalsRefresh(restored ? 'startup-cached-focus' : 'startup-focus');
      }, LIVE_SIGNALS_FOCUS_DEBOUNCE_MS);
    }
  }, 400);
}
document.querySelectorAll('.nav-item').forEach(button => {
  button.addEventListener('click', () => setView(button.dataset.view));
});

(async () => {
  const _needsSplash = true;
  showSplash();

  let restoredFromCache = false;
  try {
    primePayloadShell();
    if (!_needsSplash) renderAll();

    const auth = await authenticate();
    window._hadesAuthMe = auth?.me || {};
    primePayloadShell(auth?.me || {});
    if (!_needsSplash) renderAll();
    restoredFromCache = restoreCachedPayload();
    window._hadesRestoredFromCache = restoredFromCache;

    try {
      await bootstrap();
    } catch (error) {
      console.warn('MiniApp bootstrap refresh failed', error);
      if (!restoredFromCache && !_needsSplash) {
        showError(error.message || 'No se pudo abrir la mini-app.');
        return;
      }
    }

    if (!_needsSplash) {
      ensureViewData(state.currentView || 'home', { force: !restoredFromCache });
      syncLiveSignalsPolling();
      if (shouldPollLiveSignals()) {
        setTimeout(() => {
          queueLiveSignalsRefresh(restoredFromCache ? 'startup-cached-focus' : 'startup-focus');
        }, LIVE_SIGNALS_FOCUS_DEBOUNCE_MS);
      }
    }
  } catch (error) {
    if (!_needsSplash) showError(error.message || 'No se pudo abrir la mini-app.');
  }
})();

/* ============================================================
   HADES SPLASH SCREEN
   ============================================================ */
