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
  currentView: 'home',
  signalDetail: null,
  radarDetail: null,
  radarView: { ...DEFAULT_RADAR_VIEW },
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
};

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
  const pf = Number(summary?.profit_factor || 0);
  const exp = Number(summary?.expectancy_r || 0);
  const dd = Number(summary?.max_drawdown_r || 0);
  if (pf >= 1.5 && exp > 0 && dd <= 5) {
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

async function authenticate() {
  const params = new URLSearchParams(window.location.search);
  const devUserId = params.get('dev_user_id');
  const initData = tg?.initData || '';
  const auth = await api('/api/miniapp/auth', {
    method: 'POST',
    body: JSON.stringify({ init_data: initData, dev_user_id: devUserId ? Number(devUserId) : null }),
  });
  state.token = auth.session_token;
}

async function bootstrap() {
  state.payload = await api('/api/miniapp/bootstrap');
  renderAll();
}

function ensurePayloadShell() {
  if (!state.payload || typeof state.payload !== 'object') state.payload = {};
  if (!state.payload.account || typeof state.payload.account !== 'object') state.payload.account = {};
  if (!state.payload.account.billing || typeof state.payload.account.billing !== 'object') state.payload.account.billing = {};
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

async function refreshAccountState() {
  const [account, me] = await Promise.all([
    api('/api/miniapp/account'),
    api('/api/miniapp/me'),
  ]);
  ensurePayloadShell();
  if (me && typeof me === 'object') state.payload.me = me;
  if (account && typeof account === 'object') state.payload.account = account;
  renderAll();
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

function setTopSummary() {
  const me = state.payload?.me || {};
  els.planBadge.textContent = String(me.plan_name || 'FREE').toUpperCase();
  els.daysBadge.textContent = `${Number(me.days_left || 0)} días`;
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
        <div class="feature-item">Entrada <strong>${escapeHtml(formatNumber(preview.entry_price, 6))}</strong></div>
        <div class="feature-item">Stop <strong>${escapeHtml(formatNumber(preview.stop_loss, 6))}</strong></div>
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
              <span>Precio: ${escapeHtml(formatNumber(tp.price, 6))}</span>
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
          <div class="item-subtitle">Scanner ${escapeHtml(activity.signals_total ?? 0)} · Score ${escapeHtml(activity.avg_score === null ? '—' : formatNumber(activity.avg_score, 2))}</div>
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
        <span class="pill">TP1 ${escapeHtml(summary.tp1 ?? 0)}</span>
        <span class="pill">TP2 ${escapeHtml(summary.tp2 ?? 0)}</span>
        <span class="pill">SL ${escapeHtml(summary.sl ?? 0)}</span>
        <span class="pill">Exp ${escapeHtml(summary.expired ?? 0)}</span>
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
      </div>
    </div>
  `;
}

function performanceSetupItem(item) {
  return `
    <div class="item compact-item">
      <div class="item-header">
        <div>
          <div class="item-title">${escapeHtml(item.setup_group || '—')}</div>
          <div class="item-subtitle">Resueltas ${escapeHtml(item.resolved ?? 0)} · Exp ${escapeHtml(item.expired ?? 0)}</div>
        </div>
        <span class="plan-tag ${metricToneClass('winrate', item.winrate || 0)}">${escapeHtml(formatNumber(item.winrate || 0))}%</span>
      </div>
      <div class="inline-meta">
        <span>PF ${escapeHtml(formatRatioValue(item.profit_factor, item.profit_factor_infinite))}</span>
        <span>Expectancy ${escapeHtml(formatNumber(item.expectancy_r || 0, 4))}R</span>
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
  const payload = state.performanceCenter.payload || {};
  const overview = payload.overview || {};
  const focus = payload.focus || {};
  const summary = focus.summary || {};
  const activity = focus.activity || {};
  const diagnostics = payload.diagnostics_30d || {};
  const windows = Array.isArray(payload.windows) ? payload.windows : [];
  const planBreakdown = Array.isArray(payload.plan_breakdown_30d) ? payload.plan_breakdown_30d : [];
  const directions = Array.isArray(payload.direction_30d) ? payload.direction_30d : [];
  const setupGroups = Array.isArray(payload.setup_groups_30d) ? payload.setup_groups_30d : [];
  const weakSymbols = Array.isArray(payload.weak_symbols_30d) ? payload.weak_symbols_30d : [];
  const scoreBuckets = Array.isArray(payload.score_buckets_30d) ? payload.score_buckets_30d : [];

  const loadingBanner = state.performanceCenter.loading
    ? '<div class="card card-span-12"><div class="empty-state">Actualizando rendimiento...</div></div>'
    : '';

  if (!state.performanceCenter.payload && !state.performanceCenter.loading) {
    els.performance.innerHTML = `
      <div class="section-grid">
        ${performanceNoticeCard(state.performanceCenter.notice)}
        <div class="card card-span-12">
          <h2>Rendimiento serio</h2>
          <p>Módulo dedicado para revisar PF por R, expectancy, TP1/TP2/SL, actividad del scanner y breakdown por plan.</p>
          <div class="action-row"><button class="button button-primary" data-open-performance-center="true">Abrir rendimiento</button></div>
        </div>
      </div>
    `;
    return;
  }

  els.performance.innerHTML = `
    <div class="section-grid">
      ${performanceNoticeCard(state.performanceCenter.notice)}
      ${loadingBanner}

      <div class="card card-span-12">
        <div class="item-header">
          <div>
            <h2 style="margin:0;">Rendimiento serio</h2>
            <div class="item-subtitle">Lectura consolidada del bot en R, por ventanas de tiempo y con diagnóstico operativo real.</div>
          </div>
          <div class="action-row compact">
            <button class="button button-secondary" data-goto="home">Volver al dashboard</button>
            <button class="button button-secondary" data-performance-refresh="true">Refrescar</button>
          </div>
        </div>
        <div class="action-row compact" style="margin-top:12px;">
          ${windows.map(item => `<button class="button ${normalizePerformanceDays(item.days) === normalizePerformanceDays(overview.focus_days) ? 'button-primary' : 'button-secondary'}" data-performance-window="${escapeHtml(item.days)}">${escapeHtml(item.label)}</button>`).join('')}
        </div>
        <div class="pill-row compact-pill-row" style="margin-top:12px;">
          <span class="pill">Ventana activa: ${escapeHtml(overview.focus_label || performanceWindowLabel(overview.focus_days || 30))}</span>
          <span class="pill">Actividad scanner: ${escapeHtml(activity.signals_total ?? 0)}</span>
          <span class="pill">Score medio: ${escapeHtml(activity.avg_score === null ? '—' : formatNumber(activity.avg_score, 2))}</span>
          <span class="pill">Generado: ${escapeHtml(formatDate(overview.generated_at))}</span>
        </div>
      </div>

      ${performanceMetricCard('Evaluadas', summary.total ?? 0, 'Dentro de la ventana activa')}
      ${performanceMetricCard('Win rate', `${formatNumber(summary.winrate || 0)}%`, 'Solo resueltas', metricToneClass('winrate', summary.winrate || 0))}
      ${performanceMetricCard('PF señales (R)', formatRatioValue(summary.profit_factor, summary.profit_factor_infinite), 'TP1 / TP2 / SL', metricToneClass('pf', summary.profit_factor_infinite ? 999 : summary.profit_factor || 0))}
      ${performanceMetricCard('Expectancy R', formatNumber(summary.expectancy_r || 0, 4), 'Promedio por resuelta', metricToneClass('expectancy', summary.expectancy_r || 0))}
      ${performanceMetricCard('Net R', formatNumber(summary.net_r || 0, 4), 'Resultado neto del periodo', metricToneClass('expectancy', summary.net_r || 0))}
      ${performanceMetricCard('Max DD (R)', formatNumber(summary.max_drawdown_r || 0, 4), 'Peor racha en R', metricToneClass('drawdown', summary.max_drawdown_r || 0))}

      <div class="card card-span-12">
        <h2>Modelo R</h2>
        <div class="resolution-grid">
          ${resolutionCard('TP1', summary.tp1 ?? 0, '+1R por señal resuelta', 'metric-positive')}
          ${resolutionCard('TP2', summary.tp2 ?? 0, '+2R por señal resuelta', 'metric-positive')}
          ${resolutionCard('SL', summary.sl ?? 0, '-1R por señal resuelta', 'metric-negative')}
          ${resolutionCard('Exp limpias', summary.expired ?? 0, 'Fuera del PF y la expectancy', 'metric-neutral')}
        </div>
        <div class="pill-row compact-pill-row" style="margin-top:12px;">
          <span class="pill">Resueltas ${escapeHtml(summary.resolved ?? 0)}</span>
          <span class="pill">Gross +${escapeHtml(formatNumber(summary.gross_profit_r || 0, 4))}R</span>
          <span class="pill">Gross -${escapeHtml(formatNumber(summary.gross_loss_r || 0, 4))}R</span>
          <span class="pill">Tiempo medio ${escapeHtml(summary.avg_resolution_minutes === null ? '—' : formatNumber(summary.avg_resolution_minutes, 2))} min</span>
        </div>
      </div>

      <div class="card card-span-12">
        <h2>Diagnóstico 30D</h2>
        <div class="account-metric-grid">
          ${accountMetricCard('Pendientes', diagnostics.pending_to_evaluate ?? 0)}
          ${accountMetricCard('Loss rate', `${formatNumber(diagnostics.loss_rate || 0)}%`, '', metricToneClass('drawdown', -(diagnostics.loss_rate || 0)))}
          ${accountMetricCard('Expiry rate', `${formatNumber(diagnostics.expiry_rate || 0)}%`)}
          ${accountMetricCard('Score resultados', diagnostics.avg_result_score === null ? '—' : formatNumber(diagnostics.avg_result_score, 2))}
          ${accountMetricCard('PF 30D', formatRatioValue(diagnostics.profit_factor, diagnostics.profit_factor_infinite), '', metricToneClass('pf', diagnostics.profit_factor_infinite ? 999 : diagnostics.profit_factor || 0))}
          ${accountMetricCard('DD 30D', formatNumber(diagnostics.max_drawdown_r || 0, 4), 'R', metricToneClass('drawdown', diagnostics.max_drawdown_r || 0))}
        </div>
      </div>

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
        <h2>Win rate por raw score (30D)</h2>
        <div class="list">
          ${scoreBuckets.length ? scoreBuckets.map(performanceScoreBucketItem).join('') : '<div class="empty-state">Sin buckets de score disponibles.</div>'}
        </div>
      </div>

      <div class="card card-span-6">
        <h2>Setup groups (30D)</h2>
        <div class="list">
          ${setupGroups.length ? setupGroups.map(performanceSetupItem).join('') : '<div class="empty-state">Sin setup groups calculados.</div>'}
        </div>
      </div>

      <div class="card card-span-6">
        <h2>Símbolos más débiles (30D)</h2>
        <div class="list">
          ${weakSymbols.length ? weakSymbols.map(performanceWeakSymbolItem).join('') : '<div class="empty-state">No hay suficientes señales resueltas para diagnosticar símbolos.</div>'}
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
  const manualActivation = state.adminPanel.manualActivation || {};
  const activationDraft = manualActivation.draft || { userId: '', plan: 'plus', days: '30' };
  const lookupPayload = manualActivation.lookup || null;
  const target = lookupPayload?.target || null;
  const planOptions = Array.isArray(lookupPayload?.plan_options) ? lookupPayload.plan_options : [];
  const selectedPlan = planOptions.some(item => item.available && String(item.key || '').toLowerCase() === String(activationDraft.plan || '').toLowerCase())
    ? String(activationDraft.plan || '').toLowerCase()
    : (planOptions.find(item => item.available)?.key || String(activationDraft.plan || 'plus').toLowerCase());
  const selectedPlanOption = planOptions.find(item => String(item.key || '').toLowerCase() === selectedPlan) || null;
  const adminBusy = Boolean(state.adminPanel.loading || manualActivation.lookupLoading || manualActivation.activationLoading);
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
      ${adminOverviewMetricCard('Usuarios', formatInteger(users.total || 0), `${formatInteger(users.active_paid || 0)} pagos · ${formatInteger(users.banned || 0)} bloqueados`)}
      ${adminOverviewMetricCard('Señales 24h', formatInteger(signals.created_last_24h || 0), `${formatInteger(signals.pending_evaluation || 0)} pendientes`)}
      ${adminOverviewMetricCard('Pagos', payments.configuration_ready ? 'Configurado' : 'Incompleto', `${formatInteger(payments.pending_orders || 0)} pendientes · ${formatInteger(payments.awaiting_confirmation || 0)} por confirmar`, payments.configuration_ready ? 'is-positive' : 'is-warning')}

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

function signalCard(item) {
  const statusRaw = item.result || item.status || 'active';
  const statusBadge = item.result
    ? `<span class="${badgeClassByResult(item)}">${escapeHtml(resultLabel(item))}</span>`
    : `<span class="plan-tag">${escapeHtml(formatStatusLabel(statusRaw))}</span>`;

  return `
    <div class="item signal-card-item">
      <div class="item-header">
        <div>
          <div class="item-title">${escapeHtml(item.symbol)} <span class="${dirClass(item.direction)}">${escapeHtml(item.direction)}</span></div>
          <div class="item-subtitle">${escapeHtml(item.setup_group || 'setup')} · Score ${escapeHtml(formatNumber(item.score || 0, 1))}</div>
        </div>
        ${statusBadge}
      </div>
      <div class="pill-row compact-pill-row">
        <span class="pill">Tier ${escapeHtml(String(item.visibility || '').toUpperCase())}</span>
        ${item.entry_price ? `<span class="pill">Entrada ${escapeHtml(formatNumber(item.entry_price, 4))}</span>` : ''}
      </div>
      <div class="inline-meta">
        <span>Emitida: ${escapeHtml(formatDate(item.created_at))}</span>
        ${item.telegram_valid_until ? `<span>Visible hasta: ${escapeHtml(formatDate(item.telegram_valid_until))}</span>` : ''}
      </div>
      <div class="action-row compact">
        <button class="button button-secondary" data-signal-detail="${escapeHtml(item.signal_id)}" data-signal-source="signals">Ver inteligencia</button>
        <button class="button button-secondary" data-open-risk-signal="${escapeHtml(item.signal_id)}">Calcular riesgo</button>
      </div>
    </div>
  `;
}

function historyCard(item) {
  return `
    <div class="item">
      <div class="item-header">
        <div>
          <div class="item-title">${escapeHtml(item.symbol)} <span class="${dirClass(item.direction)}">${escapeHtml(item.direction)}</span></div>
          <div class="item-subtitle">${escapeHtml(item.setup_group || 'setup')} · Score ${escapeHtml(formatNumber(item.score || 0, 1))}</div>
        </div>
        <span class="${badgeClassByResult(item)}">${escapeHtml(resultLabel(item))}</span>
      </div>
      <div class="inline-meta">
        <span>Fecha: ${escapeHtml(formatDate(item.signal_created_at))}</span>
        <span>Resolución: ${escapeHtml(item.resolution_minutes ?? '—')} min</span>
        <span>R múltiple: ${escapeHtml(item.r_multiple ?? '—')}</span>
      </div>
      <div class="action-row compact">
        <button class="button button-secondary" data-signal-detail="${escapeHtml(item.signal_id)}" data-signal-source="history">Ver inteligencia</button>
        <button class="button button-secondary" data-open-risk-signal="${escapeHtml(item.signal_id)}">Calcular riesgo</button>
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
  const summary = dashboard.summary_7d || {};
  const market = state.payload.market || {};
  const activeOrder = dashboard.active_payment_order;
  const recentSignals = dashboard.recent_signals || [];
  const recentHistory = dashboard.recent_history || [];
  const generatedAt = state.payload.generated_at;
  const diagnosis = summaryDiagnosis(summary);

  els.home.innerHTML = `
    <div class="section-grid">
      <div class="card card-span-12 hero-card">
        <div class="hero-topline">HADES MINI APP</div>
        <div class="hero-grid">
          <div>
            <h2 class="hero-title">${escapeHtml(me.plan_name || 'FREE')} · ${escapeHtml(me.subscription_status_label || me.subscription_status || 'free')}</h2>
            <p class="hero-subtitle">Controla tus señales, revisa tu histórico y ejecuta pagos sin salir de Telegram.</p>
            <div class="pill-row">
              <span class="pill">Días restantes: ${escapeHtml(me.days_left || 0)}</span>
              <span class="pill">Watchlist: ${escapeHtml(dashboard.watchlist_count || 0)}</span>
              <span class="pill">Sesgo mercado: ${escapeHtml(market.bias || '—')}</span>
            </div>
          </div>
          <div class="hero-side ${metricToneClass('winrate', summary.winrate || 0)}">
            <div class="hero-side-value">${escapeHtml(formatNumber(summary.winrate || 0))}%</div>
            <div class="hero-side-label">Win rate 7D resuelto</div>
            <div class="hero-side-meta">Actualizado: ${escapeHtml(formatDate(generatedAt))}</div>
          </div>
        </div>
      </div>

      ${metricCard('Señales activas', dashboard.active_signals_count || 0, 'Visibles ahora mismo')}
      ${metricCard('PF señales (R)', formatNumber(summary.profit_factor || 0), 'Solo resueltas: TP1 / TP2 / SL', '', metricToneClass('pf', summary.profit_factor || 0))}
      ${metricCard('Expectancy R', formatNumber(summary.expectancy_r || 0), 'Promedio por señal resuelta', '', metricToneClass('expectancy', summary.expectancy_r || 0))}
      ${metricCard('Max DD (R)', formatNumber(summary.max_drawdown_r || 0), 'Peor racha reciente en R', '', metricToneClass('drawdown', summary.max_drawdown_r || 0))}

      <div class="card card-span-12 ${escapeHtml(diagnosis.tone)}">
        <div class="diagnostic-title">${escapeHtml(diagnosis.title)}</div>
        <div class="diagnostic-text">${escapeHtml(diagnosis.text)}</div>
      </div>

      <div class="card card-span-12">
        <h2>Lectura rápida del modelo R</h2>
        <div class="resolution-grid">
          ${resolutionCard('TP1', summary.tp1 ?? 0, '+1R por señal resuelta', 'metric-positive')}
          ${resolutionCard('TP2', summary.tp2 ?? 0, '+2R por señal resuelta', 'metric-positive')}
          ${resolutionCard('SL', summary.sl ?? 0, '-1R por señal resuelta', 'metric-negative')}
          ${resolutionCard('Exp limpias', summary.expired ?? 0, 'Fuera del PF y la expectancy', 'metric-neutral')}
        </div>
        <div class="pill-row compact-pill-row" style="margin-top: 12px;">
          <span class="pill">Evaluadas: ${escapeHtml(summary.total ?? 0)}</span>
          <span class="pill">Resueltas: ${escapeHtml(summary.resolved ?? 0)}</span>
          <span class="pill">Win rate resuelto: ${escapeHtml(formatNumber(summary.winrate || 0))}%</span>
        </div>
      </div>

      <div class="card card-span-12">
        <h2>Acciones rápidas</h2>
        <div class="action-row">
          <button class="button button-primary" data-goto="signals">Ver señales</button>
          <button class="button button-secondary" data-goto="market">Mercado</button>
          <button class="button button-secondary" data-goto="history">Ver historial</button>
          <button class="button button-secondary" data-open-performance-center="true">Rendimiento</button>
          <button class="button button-secondary" data-goto="account">Mi cuenta</button>
        </div>
      </div>

      <div class="card card-span-6">
        <h2>Estado operativo</h2>
        <div class="pill-row">
          <span class="pill">Bias: ${escapeHtml(market.bias || '—')}</span>
          <span class="pill">Régimen: ${escapeHtml(market.regime || '—')}</span>
          <span class="pill">Volatilidad: ${escapeHtml(market.volatility || '—')}</span>
          <span class="pill">Entorno: ${escapeHtml(market.environment || '—')}</span>
          <span class="pill">Exp. limpias 7D: ${escapeHtml(summary.expired ?? 0)}</span>
        </div>
        <p style="margin-top:12px;">${escapeHtml(market.recommendation || 'Sin lectura operativa disponible por ahora.')}</p>
      </div>

      <div class="card card-span-6">
        <h2>Distribución reciente</h2>
        ${mixPills('Últimas señales entregadas', dashboard.signal_mix || {})}
        ${mixPills('Activas ahora mismo', dashboard.active_mix || {})}
      </div>

      <div class="card card-span-12">
        <div class="item-header">
          <div>
            <h2 style="margin:0;">Rendimiento serio</h2>
            <div class="item-subtitle">Abre el módulo dedicado para revisar PF por R, expectancy, setup groups, score buckets y breakdown por plan.</div>
          </div>
          <span class="plan-tag">30D</span>
        </div>
        <div class="account-metric-grid">
          ${accountMetricCard('PF 30D', formatRatioValue(dashboard.summary_30d?.profit_factor, dashboard.summary_30d?.profit_factor === null && false), '', metricToneClass('pf', dashboard.summary_30d?.profit_factor || 0))}
          ${accountMetricCard('Expectancy 30D', formatNumber(dashboard.summary_30d?.expectancy_r || 0, 4), 'R por resuelta', metricToneClass('expectancy', dashboard.summary_30d?.expectancy_r || 0))}
          ${accountMetricCard('Win rate 30D', `${formatNumber(dashboard.summary_30d?.winrate || 0)}%`, '', metricToneClass('winrate', dashboard.summary_30d?.winrate || 0))}
          ${accountMetricCard('Resueltas 30D', dashboard.summary_30d?.resolved || 0)}
        </div>
        <div class="action-row compact" style="margin-top:12px;">
          <button class="button button-secondary" data-open-performance-center="true">Abrir rendimiento</button>
        </div>
      </div>

      <div class="card card-span-12">
        <h2>Señales recientes</h2>
        <div class="list">
          ${recentSignals.length ? recentSignals.slice(0, 3).map(signalCard).join('') : '<div class="empty-state">Todavía no hay señales recientes para mostrar.</div>'}
        </div>
      </div>

      <div class="card card-span-12">
        <h2>Historial reciente</h2>
        <div class="list">
          ${recentHistory.length ? recentHistory.slice(0, 3).map(historyCard).join('') : '<div class="empty-state">No hay histórico reciente todavía.</div>'}
        </div>
      </div>

      ${activeOrder ? paymentInstructions(activeOrder) : ''}
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

  els.signals.innerHTML = `
    <div class="section-grid">
      ${metricCard('Total recientes', signals.length, 'Últimas señales visibles')}
      ${metricCard('Activas', counts.active, 'Pendientes o en curso')}
      ${metricCard('Premium', counts.premium, 'Tier premium')}
      ${metricCard('Cerradas', counts.closed, 'Con resultado persistido')}

      <div class="card card-span-12">
        <h2>Distribución por tier</h2>
        <div class="pill-row">
          <span class="pill">Free: ${escapeHtml(counts.free)}</span>
          <span class="pill">Plus: ${escapeHtml(counts.plus)}</span>
          <span class="pill">Premium: ${escapeHtml(counts.premium)}</span>
        </div>
      </div>

      <div class="card card-span-12">
        <h2>Señales recientes</h2>
        <div class="list">
          ${signals.length ? signals.map(signalCard).join('') : '<div class="empty-state">No hay señales disponibles todavía.</div>'}
        </div>
      </div>
    </div>
  `;
}

function renderMarket() {
  const market = state.payload.market || {};
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

  const movementList = (items, type) => items.length ? items.map(item => `
    <div class="item compact-item">
      <div class="item-header">
        <div class="item-title">${escapeHtml(item.symbol)}</div>
        <span class="${Number(item.change || 0) >= 0 ? 'positive-text' : 'negative-text'}">${escapeHtml(formatPercentSigned(item.change, 2))}</span>
      </div>
      <div class="inline-meta">
        ${item.quote_volume ? `<span>Vol: ${escapeHtml(formatCompactAmount(item.quote_volume))}</span>` : ''}
        ${item.last_price ? `<span>Px: ${escapeHtml(formatNumber(item.last_price, 4))}</span>` : ''}
      </div>
    </div>
  `).join('') : `<div class="empty-state">Sin ${type} disponibles.</div>`;

  els.market.innerHTML = `
    <div class="section-grid">
      <div class="card card-span-12 market-hero-card">
        <div class="hero-topline">PULSO DEL MERCADO</div>
        <div class="hero-grid">
          <div>
            <h2 class="hero-title">${escapeHtml(market.bias || 'Neutral')} · ${escapeHtml(market.preferred_side || 'Selectivo')}</h2>
            <p class="hero-subtitle">${escapeHtml(market.recommendation || 'Sin recomendación disponible por ahora.')}</p>
          </div>
          <div class="pill-row">
            <span class="pill">Régimen: ${escapeHtml(market.regime || '—')}</span>
            <span class="pill">Volatilidad: ${escapeHtml(market.volatility || '—')}</span>
            <span class="pill">Participación: ${escapeHtml(market.participation || '—')}</span>
            <span class="pill">Advance ratio: ${escapeHtml(formatNumber(market.adv_ratio_pct || 0, 1))}%</span>
          </div>
        </div>
      </div>

      <div class="card card-span-6">
        <h2>Mayores subidas</h2>
        <div class="list">${movementList(gainers, 'gainers')}</div>
      </div>

      <div class="card card-span-6">
        <h2>Mayores caídas</h2>
        <div class="list">${movementList(losers, 'losers')}</div>
      </div>

      <div class="card card-span-6">
        <h2>BTC / ETH</h2>
        <div class="list">
          <div class="item compact-item">
            <div class="item-header">
              <div class="item-title">BTCUSDT</div>
              <span class="${sideClassByValue(btc.change)}">${escapeHtml(formatPercentSigned(btc.change, 2))}</span>
            </div>
            <div class="inline-meta">
              <span>Funding: ${escapeHtml(formatPercentSigned(btc.funding_rate_pct, 3))}</span>
              <span>OI: ${escapeHtml(formatCompactAmount(btc.open_interest))}</span>
            </div>
          </div>
          <div class="item compact-item">
            <div class="item-header">
              <div class="item-title">ETHUSDT</div>
              <span class="${sideClassByValue(eth.change)}">${escapeHtml(formatPercentSigned(eth.change, 2))}</span>
            </div>
            <div class="inline-meta">
              <span>Funding: ${escapeHtml(formatPercentSigned(eth.funding_rate_pct, 3))}</span>
              <span>OI: ${escapeHtml(formatCompactAmount(eth.open_interest))}</span>
            </div>
          </div>
        </div>
      </div>

      <div class="card card-span-6">
        <h2>Top volumen</h2>
        <div class="list">${movementList(topVolume, 'volumen')}</div>
      </div>

      <div class="card card-span-12">
        <div class="item-header radar-section-header">
          <div>
            <h2>Radar V2</h2>
            <div class="item-subtitle">Filtra, ordena y prioriza con contexto de ejecución, alineación y seguimiento.</div>
          </div>
          <div class="pill-row compact-pill-row radar-summary-row">
            <span class="pill">Hot: ${escapeHtml(radarSummary.hot ?? 0)}</span>
            <span class="pill">Inmediatos: ${escapeHtml(radarSummary.immediate ?? 0)}</span>
            <span class="pill">Focus now: ${escapeHtml(radarSummary.focus_now ?? 0)}</span>
            <span class="pill">A favor: ${escapeHtml(radarSummary.aligned_now ?? 0)}</span>
            <span class="pill">Con señal: ${escapeHtml(radarSummary.active_signals ?? 0)}</span>
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
            <span>Volatilidad ${escapeHtml(market.volatility || '—')} · Participación ${escapeHtml(market.participation || '—')}</span>
          </div>
        </div>

        <div class="radar-toolbar">
          <input id="radarSearchInput" class="text-input radar-search-input" placeholder="Buscar símbolo, motivo o acción" value="${escapeHtml(radarView.search || '')}" />
          <div class="radar-filter-grid radar-filter-grid-extended">
            <label class="radar-filter-field">
              <span>Dirección</span>
              <select id="radarDirectionFilter" class="text-input compact-select">
                <option value="all" ${radarView.direction === 'all' ? 'selected' : ''}>Todas (${escapeHtml(radar.length)})</option>
                <option value="LONG" ${radarView.direction === 'LONG' ? 'selected' : ''}>Long (${escapeHtml(radarSummary.longs ?? radarFilterCount(radar, item => item.direction === 'LONG'))})</option>
                <option value="SHORT" ${radarView.direction === 'SHORT' ? 'selected' : ''}>Short (${escapeHtml(radarSummary.shorts ?? radarFilterCount(radar, item => item.direction === 'SHORT'))})</option>
              </select>
            </label>
            <label class="radar-filter-field">
              <span>Prioridad</span>
              <select id="radarPriorityFilter" class="text-input compact-select">
                <option value="all" ${radarView.priority === 'all' ? 'selected' : ''}>Todas</option>
                <option value="Máxima" ${radarView.priority === 'Máxima' ? 'selected' : ''}>Máxima (${escapeHtml(radarSummary.priority_mix?.maxima ?? radarFilterCount(radar, item => item.priority_label === 'Máxima'))})</option>
                <option value="Alta" ${radarView.priority === 'Alta' ? 'selected' : ''}>Alta (${escapeHtml(radarSummary.priority_mix?.alta ?? radarFilterCount(radar, item => item.priority_label === 'Alta'))})</option>
                <option value="Media" ${radarView.priority === 'Media' ? 'selected' : ''}>Media (${escapeHtml(radarSummary.priority_mix?.media ?? radarFilterCount(radar, item => item.priority_label === 'Media'))})</option>
                <option value="Vigilancia" ${radarView.priority === 'Vigilancia' ? 'selected' : ''}>Vigilancia (${escapeHtml(radarSummary.priority_mix?.vigilancia ?? radarFilterCount(radar, item => item.priority_label === 'Vigilancia'))})</option>
              </select>
            </label>
            <label class="radar-filter-field">
              <span>Proximidad</span>
              <select id="radarProximityFilter" class="text-input compact-select">
                <option value="all" ${radarView.proximity === 'all' ? 'selected' : ''}>Todas</option>
                <option value="Activa" ${radarView.proximity === 'Activa' ? 'selected' : ''}>Activa (${escapeHtml(radarSummary.proximity_mix?.activa ?? radarFilterCount(radar, item => item.proximity_label === 'Activa'))})</option>
                <option value="Inmediata" ${radarView.proximity === 'Inmediata' ? 'selected' : ''}>Inmediata (${escapeHtml(radarSummary.proximity_mix?.inmediata ?? radarFilterCount(radar, item => item.proximity_label === 'Inmediata'))})</option>
                <option value="Cercana" ${radarView.proximity === 'Cercana' ? 'selected' : ''}>Cercana (${escapeHtml(radarSummary.proximity_mix?.cercana ?? radarFilterCount(radar, item => item.proximity_label === 'Cercana'))})</option>
                <option value="Preparando" ${radarView.proximity === 'Preparando' ? 'selected' : ''}>Preparando (${escapeHtml(radarSummary.proximity_mix?.preparando ?? radarFilterCount(radar, item => item.proximity_label === 'Preparando'))})</option>
              </select>
            </label>
            <label class="radar-filter-field">
              <span>Estado</span>
              <select id="radarExecutionFilter" class="text-input compact-select">
                <option value="all" ${radarView.execution === 'all' ? 'selected' : ''}>Todos</option>
                <option value="Seguimiento" ${radarView.execution === 'Seguimiento' ? 'selected' : ''}>Seguimiento (${escapeHtml(radarSummary.execution_mix?.seguimiento ?? radarFilterCount(radar, item => item.execution_state_label === 'Seguimiento'))})</option>
                <option value="Ejecutable" ${radarView.execution === 'Ejecutable' ? 'selected' : ''}>Ejecutable (${escapeHtml(radarSummary.execution_mix?.ejecutable ?? radarFilterCount(radar, item => item.execution_state_label === 'Ejecutable'))})</option>
                <option value="Preparación" ${radarView.execution === 'Preparación' ? 'selected' : ''}>Preparación (${escapeHtml(radarSummary.execution_mix?.preparacion ?? radarFilterCount(radar, item => item.execution_state_label === 'Preparación'))})</option>
                <option value="Observación" ${radarView.execution === 'Observación' ? 'selected' : ''}>Observación (${escapeHtml(radarSummary.execution_mix?.observacion ?? radarFilterCount(radar, item => item.execution_state_label === 'Observación'))})</option>
              </select>
            </label>
            <label class="radar-filter-field">
              <span>Alineación</span>
              <select id="radarAlignmentFilter" class="text-input compact-select">
                <option value="all" ${radarView.alignment === 'all' ? 'selected' : ''}>Todas</option>
                <option value="A favor" ${radarView.alignment === 'A favor' ? 'selected' : ''}>A favor (${escapeHtml(radarSummary.alignment_mix?.a_favor ?? radarFilterCount(radar, item => item.alignment_label === 'A favor'))})</option>
                <option value="Con flujo" ${radarView.alignment === 'Con flujo' ? 'selected' : ''}>Con flujo (${escapeHtml(radarSummary.alignment_mix?.con_flujo ?? radarFilterCount(radar, item => item.alignment_label === 'Con flujo'))})</option>
                <option value="Selectivo" ${radarView.alignment === 'Selectivo' ? 'selected' : ''}>Selectivo (${escapeHtml(radarSummary.alignment_mix?.selectivo ?? radarFilterCount(radar, item => item.alignment_label === 'Selectivo'))})</option>
                <option value="Contratendencia" ${radarView.alignment === 'Contratendencia' ? 'selected' : ''}>Contratendencia (${escapeHtml(radarSummary.alignment_mix?.contratendencia ?? radarFilterCount(radar, item => item.alignment_label === 'Contratendencia'))})</option>
              </select>
            </label>
            <label class="radar-filter-field">
              <span>Señal</span>
              <select id="radarSignalFilter" class="text-input compact-select">
                <option value="all" ${radarView.signal === 'all' ? 'selected' : ''}>Todas</option>
                <option value="active" ${radarView.signal === 'active' ? 'selected' : ''}>Activa (${escapeHtml(radarSummary.signal_mix?.activa ?? radarFilterCount(radar, item => item.signal_context_label === 'Activa'))})</option>
                <option value="recent" ${radarView.signal === 'recent' ? 'selected' : ''}>Reciente (${escapeHtml(radarSummary.signal_mix?.reciente ?? radarFilterCount(radar, item => item.signal_context_label === 'Reciente'))})</option>
                <option value="none" ${radarView.signal === 'none' ? 'selected' : ''}>Sin señal (${escapeHtml(radarSummary.signal_mix?.sin_senal ?? radarFilterCount(radar, item => item.signal_context_label === 'Sin señal'))})</option>
              </select>
            </label>
            <label class="radar-filter-field">
              <span>Orden</span>
              <select id="radarSortFilter" class="text-input compact-select">
                <option value="ranking" ${radarView.sort === 'ranking' ? 'selected' : ''}>Ranking</option>
                <option value="execution" ${radarView.sort === 'execution' ? 'selected' : ''}>Estado operativo</option>
                <option value="priority" ${radarView.sort === 'priority' ? 'selected' : ''}>Prioridad</option>
                <option value="proximity" ${radarView.sort === 'proximity' ? 'selected' : ''}>Proximidad</option>
                <option value="score" ${radarView.sort === 'score' ? 'selected' : ''}>Score radar</option>
                <option value="volume" ${radarView.sort === 'volume' ? 'selected' : ''}>Volumen</option>
                <option value="change" ${radarView.sort === 'change' ? 'selected' : ''}>Movimiento 24h</option>
              </select>
            </label>
          </div>
          <div class="radar-toolbar-footer">
            <div class="pill-row compact-pill-row radar-results-row">
              <span class="pill">${escapeHtml(radarWindowMeta(radarWindow, radar.length))}</span>
              <span class="pill">Orden: ${escapeHtml(radarSortLabel(radarView.sort))}</span>
              ${radarView.search ? `<span class="pill">Búsqueda: ${escapeHtml(radarView.search)}</span>` : ''}
            </div>
            <div class="action-row compact">
              <button class="button button-secondary" data-radar-rotate ${radarWindow.canRotate ? '' : 'disabled'}>Actualizar radar</button>
              <button class="button button-secondary radar-reset-button" data-radar-reset>Reset filtros</button>
            </div>
          </div>
        </div>

        <div class="radar-card-grid">
          ${radarCards.length ? radarCards.map(item => {
            const inWatchlist = watchlistSymbols.has(String(item.symbol || '').toUpperCase());
            return `
            <div class="item compact-item watchlist-item-card radar-item-card">
              <div class="item-header radar-item-header">
                <div>
                  <div class="item-title">${escapeHtml(item.symbol)}</div>
                  <div class="item-subtitle ${watchlistBiasClass(item.range_bias_label)}">${escapeHtml(item.reason_short || item.range_bias_label || 'Radar operativo')}</div>
                  <div class="watchlist-opportunity-copy">${escapeHtml(item.operator_note || item.action_label || 'Sin gatillo operativo claro')}</div>
                </div>
                <div class="radar-header-side">
                  <span class="${dirClass(item.direction)}">${escapeHtml(item.direction || '—')}</span>
                  <span class="radar-score-chip">Radar ${escapeHtml(formatNumber(item.final_score, 0))}</span>
                </div>
              </div>
              <div class="pill-row compact-pill-row watchlist-priority-row radar-pill-row">
                <span class="watchlist-priority-pill ${radarExecutionClass(item.execution_state_label)}">${escapeHtml(item.execution_state_label || 'Observación')}</span>
                <span class="watchlist-priority-pill ${radarAlignmentClass(item.alignment_label)}">${escapeHtml(item.alignment_label || 'Selectivo')}</span>
                <span class="watchlist-priority-pill ${watchlistPriorityClass(item.priority_label)}">Prioridad ${escapeHtml(item.priority_label || '—')}</span>
                <span class="watchlist-priority-pill ${watchlistProximityClass(item.proximity_label)}">Proximidad ${escapeHtml(item.proximity_label || '—')}</span>
                <span class="watchlist-priority-pill ${radarRiskClass(item.risk_label)}">Riesgo ${escapeHtml(item.risk_label || '—')}</span>
              </div>
              <div class="watchlist-metric-grid radar-metric-grid">
                <div class="watchlist-metric-box">
                  <span class="watchlist-metric-label">Ranking</span>
                  <span class="watchlist-metric-value">${escapeHtml(formatNumber(item.ranking_score, 1))}</span>
                </div>
                <div class="watchlist-metric-box">
                  <span class="watchlist-metric-label">Estado</span>
                  <span class="watchlist-metric-value">${escapeHtml(item.execution_state_label || '—')}</span>
                </div>
                <div class="watchlist-metric-box">
                  <span class="watchlist-metric-label">Setup</span>
                  <span class="watchlist-metric-value">${escapeHtml(item.setup_mode_label || '—')}</span>
                </div>
                <div class="watchlist-metric-box">
                  <span class="watchlist-metric-label">Convicción</span>
                  <span class="watchlist-metric-value">${escapeHtml(item.conviction_label || '—')}</span>
                </div>
                <div class="watchlist-metric-box">
                  <span class="watchlist-metric-label">Cambio 24h</span>
                  <span class="watchlist-metric-value ${sideClassByValue(item.change_pct)}">${escapeHtml(formatPercentSigned(item.change_pct, 2))}</span>
                </div>
                <div class="watchlist-metric-box">
                  <span class="watchlist-metric-label">Posición</span>
                  <span class="watchlist-metric-value">${escapeHtml(watchlistRangePosition(item.range_position_pct))}</span>
                </div>
                <div class="watchlist-metric-box">
                  <span class="watchlist-metric-label">Funding</span>
                  <span class="watchlist-metric-value ${sideClassByValue(item.funding_rate_pct)}">${escapeHtml(formatPercentSigned(item.funding_rate_pct, 3))}</span>
                </div>
                <div class="watchlist-metric-box">
                  <span class="watchlist-metric-label">Open interest</span>
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
              <div class="radar-plan-list">
                ${(item.trade_plan || []).map(step => `<div class="radar-plan-item">${escapeHtml(step)}</div>`).join('')}
              </div>
              <div class="watchlist-reason-list radar-reason-list">
                ${(item.reasons || []).map(reason => `<span class="watchlist-reason-chip">${escapeHtml(reason)}</span>`).join('')}
              </div>
              <div class="inline-meta watchlist-inline-meta radar-inline-meta">
                <span>Precio: ${escapeHtml(formatNumber(item.last_price, 4))}</span>
                <span>Trades: ${escapeHtml(formatInteger(item.trade_count))}</span>
                <span>Momentum: ${escapeHtml(item.momentum || '—')}</span>
                <span>Volatilidad: ${escapeHtml(item.volatility_label || '—')}</span>
              </div>
              <div class="action-row compact watchlist-card-actions radar-card-actions">
                <button class="button button-secondary" data-radar-detail="${escapeHtml(item.symbol)}">Radar táctico</button>
                ${item.latest_signal?.signal_id ? `<button class="button button-secondary" data-signal-detail="${escapeHtml(item.latest_signal.signal_id)}" data-signal-source="radar">Ver inteligencia</button>` : ''}
                ${inWatchlist
                  ? `<button class="button button-secondary" disabled>En watchlist</button>`
                  : `<button class="button button-primary" data-radar-follow="${escapeHtml(item.symbol)}">Seguir</button>`}
              </div>
            </div>
          `; }).join('') : '<div class="empty-state">No hay activos que cumplan ese filtro ahora mismo.</div>'}
        </div>
      </div>

      <div class="card card-span-6">
        <h2>Watchlist</h2>
        <div class="pill-row compact-pill-row">
          <span class="pill">Límite: ${escapeHtml(watchlistLimitText(watchlistMeta))}</span>
          <span class="pill">Slots libres: ${escapeHtml(watchlistMeta.slots_left ?? '∞')}</span>
          <span class="pill">Plan: ${escapeHtml(String(watchlistMeta.plan_name || watchlistMeta.plan || 'FREE').toUpperCase())}</span>
        </div>
        <div class="watchlist-controls">
          <input id="watchlistInput" class="text-input" placeholder="BTC, ETH, SOL o BTCUSDT" />
          <div class="action-row compact">
            <button class="button button-primary" data-watchlist-add>Agregar</button>
            <button class="button button-secondary" data-watchlist-replace>Reemplazar</button>
            <button class="button button-danger" data-watchlist-clear>Limpiar</button>
          </div>
        </div>
        <div class="symbol-chip-row">
          ${(watchlistMeta.symbols || []).length ? (watchlistMeta.symbols || []).map(symbol => `
            <button class="symbol-chip" data-watchlist-remove="${escapeHtml(symbol)}">${escapeHtml(symbol)} ✕</button>
          `).join('') : '<div class="empty-state">Todavía no tienes símbolos guardados.</div>'}
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
                  <span class="radar-score-chip">${escapeHtml(item.radar_direction || '—')}</span>
                </div>
              </div>
              <div class="pill-row compact-pill-row watchlist-priority-row">
                <span class="watchlist-priority-pill ${watchlistPriorityClass(item.setup_priority_label)}">Prioridad ${escapeHtml(item.setup_priority_label || '—')} · ${escapeHtml(formatNumber(item.setup_priority_score, 0))}</span>
                <span class="watchlist-priority-pill ${watchlistProximityClass(item.setup_proximity_label)}">Proximidad ${escapeHtml(item.setup_proximity_label || '—')}</span>
                <span class="watchlist-priority-pill ${watchlistBiasClass(item.range_bias_label) ? 'watchlist-pill-strong' : 'watchlist-pill-soft'}">${escapeHtml(item.range_bias_label || 'Sin rango')}</span>
                ${item.active_signal ? `<span class="watchlist-priority-pill watchlist-pill-active">Señal activa · ${escapeHtml(item.active_signal?.visibility_name || 'HADES')}</span>` : (item.latest_signal ? `<span class="watchlist-priority-pill watchlist-pill-soft">Última señal · ${escapeHtml(item.latest_signal.visibility_name || item.latest_signal.visibility || '—')}</span>` : '')}
              </div>
              <div class="watchlist-metric-grid">
                <div class="watchlist-metric-box">
                  <span class="watchlist-metric-label">Prioridad</span>
                  <span class="watchlist-metric-value">${escapeHtml(formatNumber(item.setup_priority_score, 1))}</span>
                </div>
                <div class="watchlist-metric-box">
                  <span class="watchlist-metric-label">Proximidad</span>
                  <span class="watchlist-metric-value">${escapeHtml(formatNumber(item.setup_proximity_score, 1))}</span>
                </div>
                <div class="watchlist-metric-box">
                  <span class="watchlist-metric-label">Precio</span>
                  <span class="watchlist-metric-value">${escapeHtml(formatNumber(item.last_price, 4))}</span>
                </div>
                <div class="watchlist-metric-box">
                  <span class="watchlist-metric-label">Rango 24h</span>
                  <span class="watchlist-metric-value">${escapeHtml(formatPercentSigned(item.range_pct_24h, 2))}</span>
                </div>
                <div class="watchlist-metric-box">
                  <span class="watchlist-metric-label">Posición</span>
                  <span class="watchlist-metric-value">${escapeHtml(watchlistRangePosition(item.range_position_pct))}</span>
                </div>
                <div class="watchlist-metric-box">
                  <span class="watchlist-metric-label">Radar</span>
                  <span class="watchlist-metric-value">${escapeHtml(item.radar_score ? formatNumber(item.radar_score, 0) : '—')}</span>
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
              <div class="watchlist-reason-list">
                ${(item.priority_reasons || []).map(reason => `<span class="watchlist-reason-chip">${escapeHtml(reason)}</span>`).join('')}
              </div>
              <div class="inline-meta watchlist-inline-meta">
                <span>Cambio abs: ${escapeHtml(formatNumber(item.price_change_abs, 4))}</span>
                <span>Trades: ${escapeHtml(formatInteger(item.trade_count))}</span>
                <span>Volatilidad: ${escapeHtml(item.volatility_label || '—')}</span>
                ${item.radar_momentum ? `<span>Momentum radar: ${escapeHtml(item.radar_momentum)}</span>` : ''}
              </div>
              ${item.latest_signal?.signal_id ? `
                <div class="action-row compact watchlist-card-actions">
                  <button class="button button-secondary" data-signal-detail="${escapeHtml(item.latest_signal.signal_id)}" data-signal-source="watchlist">Ver inteligencia</button>
                  <button class="button button-secondary" data-open-risk-signal="${escapeHtml(item.latest_signal.signal_id)}">Calcular riesgo</button>
                </div>
              ` : ''}
            </div>
          `).join('') : '<div class="empty-state">Tu watchlist está vacía.</div>'}
        </div>
      </div>
    </div>
  `;
}

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
  const items = state.payload.history || [];
  els.history.innerHTML = `
    <div class="card"><h2>Historial verificable</h2><p>Señales cerradas y resultados persistidos desde HADES.</p></div>
    <div class="list" style="margin-top:12px;">
      ${items.length ? items.map(historyCard).join('') : '<div class="empty-state">No hay historial disponible por ahora.</div>'}
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
    return String(item?.label || '') === String(other?.label || '') && Number(item?.score || 0) === Number(other?.score || 0);
  });
}

function renderScoreBreakdown(items) {
  if (!items || !items.length) return '<div class="empty-state">Sin desglose disponible.</div>';
  return `<div class="component-list">${items.map(item => `
    <div class="component-row">
      <span>${escapeHtml(item.label)}</span>
      <span class="${Number(item.score || 0) >= 0 ? 'positive-text' : 'negative-text'}">${escapeHtml(formatNumber(item.score, 2))}</span>
    </div>
  `).join('')}</div>`;
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
        ${detailStatCard('Precio', formatNumber(radar.last_price, 4), sideClassByValue(radar.change_pct || 0))}
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
              `${profile.label} · SL ${formatNumber(profile.stop_loss, 4)}`,
              `TP1 ${formatNumber(profile.tp1, 4)} · TP2 ${formatNumber(profile.tp2, 4)}`,
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

  els.signalDetailTitle.textContent = `${signal.symbol || 'Señal'} · ${signal.direction || ''}`.trim();
  els.signalDetailBody.innerHTML = `
    <div class="signal-intel-layout">
      <div class="card detail-status-card">
        <div class="detail-status-top">
          <div class="detail-status-copy-block">
            <span class="detail-kicker">Resumen operativo</span>
            <div class="item-title">${escapeHtml(tracking.state_label || 'Sin estado')}</div>
            <div class="item-subtitle">${escapeHtml(tracking.entry_state_label || 'Sin lectura operativa')}</div>
          </div>
          ${statusBadge}
        </div>
        <p class="detail-status-copy">${escapeHtml(tracking.recommendation || 'Sin recomendación operativa disponible.')}</p>
      </div>

      <div class="detail-info-grid">
        ${detailInfoChip('Plan', String(payload.viewer_plan || 'free').toUpperCase())}
        ${detailInfoChip('Tier', String(signal.visibility || 'free').toUpperCase())}
        ${detailInfoChip('Perfil', profileLabel(selectedProfile))}
        ${detailInfoChip('Tracking', String(tier).toUpperCase())}
        ${detailInfoChip('Setup', String(analysis.setup_group || signal.setup_group || 'legacy').toUpperCase())}
        ${detailInfoChip('Score', formatNumber(analysis.normalized_score ?? analysis.score ?? signal.score, 1))}
      </div>

      <div class="detail-profile-selector" role="tablist" aria-label="Perfil de lectura">
        ${profileOptions.map(option => `<button class="detail-profile-button ${option === selectedProfile ? 'is-active' : ''}" data-signal-profile="${escapeHtml(option)}" data-signal-id="${escapeHtml(signal.signal_id || '')}" aria-pressed="${option === selectedProfile ? 'true' : 'false'}">${escapeHtml(profileLabel(option))}</button>`).join('')}
      </div>

      <div class="detail-stat-grid">
        ${detailStatCard('Precio actual', formatNumber(tracking.current_price, 4), sideClassByValue(tracking.current_move_pct || 0))}
        ${detailStatCard('Entrada', formatNumber(tracking.entry_price, 4))}
        ${detailStatCard('SL', formatNumber(tracking.stop_loss, 4), 'negative-text')}
        ${detailStatCard('TP1', formatNumber((tracking.take_profits || [])[0], 4), 'positive-text')}
        ${detailStatCard('TP2', formatNumber((tracking.take_profits || [])[1], 4), 'positive-text')}
        ${detailStatCard('Dist. entrada', formatFractionPercent(tracking.distance_to_entry_pct))}
        ${detailStatCard('Dist. SL', formatFractionPercent(tracking.stop_distance_pct))}
        ${detailStatCard('Dist. TP1', formatFractionPercent(tracking.tp1_distance_pct))}
        ${detailStatCard('Dist. TP2', formatFractionPercent(tracking.tp2_distance_pct))}
        ${detailStatCard('Progreso TP1', tracking.progress_to_tp1_pct === null || tracking.progress_to_tp1_pct === undefined ? '—' : formatPercentSigned(tracking.progress_to_tp1_pct, 1))}
      </div>

      <div class="card signal-intel-section signal-intel-section-full">
        <h3>Lectura operativa</h3>
        <div class="pill-row compact-pill-row">
          <span class="pill">En entrada: ${tracking.in_entry_zone ? 'Sí' : 'No'}</span>
          <span class="pill">Operable ahora: ${tracking.is_operable_now ? 'Sí' : 'No'}</span>
          <span class="pill">TP1 tocado: ${tracking.tp1_hit_now ? 'Sí' : 'No'}</span>
          <span class="pill">TP2 tocado: ${tracking.tp2_hit_now ? 'Sí' : 'No'}</span>
          <span class="pill">SL roto: ${tracking.stop_hit_now ? 'Sí' : 'No'}</span>
        </div>
        <div class="inline-meta">
          <span>Creada: ${escapeHtml(formatDate(tracking.created_at || signal.created_at))}</span>
          <span>Visible hasta: ${escapeHtml(formatDate(tracking.telegram_valid_until || signal.telegram_valid_until))}</span>
          <span>Evaluación hasta: ${escapeHtml(formatDate(tracking.evaluation_valid_until))}</span>
        </div>
      </div>

      <div class="card signal-intel-section signal-intel-section-full">
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

      ${warnings.length ? `<div class="card signal-intel-section signal-intel-section-full"><h3>Notas</h3><div class="feature-list">${warnings.map(item => `<div class="feature-item">• ${escapeHtml(item)}</div>`).join('')}</div></div>` : ''}
      <div class="card signal-intel-section signal-intel-section-full">
        <h3>Gestión de riesgo</h3>
        <p>Lleva esta señal directamente a la calculadora para revisar sizing, margen requerido, pérdida al stop y RR neto.</p>
        <div class="action-row compact">
          <button class="button button-secondary" data-open-risk-signal="${escapeHtml(signal.signal_id)}">Abrir calculadora</button>
        </div>
      </div>
      ${payload.upgrade_hint ? `<div class="card signal-intel-section signal-intel-section-full upgrade-note-card"><h3>Lectura premium</h3><p>${escapeHtml(payload.upgrade_hint)}</p></div>` : ''}
    </div>
  `;
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
          <span class="pill">7D PF: ${escapeHtml(formatRatioValue((state.payload.dashboard || {}).summary_7d?.profit_factor, false))}</span>
          <span class="pill">30D PF: ${escapeHtml(formatRatioValue((state.payload.dashboard || {}).summary_30d?.profit_factor, false))}</span>
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

      <div class="card card-span-12">
        <h2>Soporte</h2>
        <div class="action-row">
          <a class="button button-secondary" target="_blank" rel="noopener" href="${escapeHtml(support.url || state.payload.support_url || '#')}">Abrir grupo de soporte</a>
        </div>
      </div>
    </div>
  `;
}

function renderAll() {
  setTopSummary();
  renderHome();
  renderSignals();
  renderMarket();
  renderHistory();
  renderAccount();
  renderPerformance();
  renderRisk();
  renderSettings();
  renderAdmin();
  bindViewButtons();
  els.loading.classList.add('hidden');
  els.content.classList.remove('hidden');
  els.bottomNav.classList.remove('hidden');
}

function setView(view) {
  state.currentView = view;
  document.querySelectorAll('.view').forEach(node => node.classList.remove('active'));
  document.getElementById(`view-${view}`).classList.add('active');
  document.querySelectorAll('.nav-item').forEach(node => node.classList.toggle('active', node.dataset.view === view));
  els.titleMain.textContent = labels[view] || 'HADES';
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
  document.querySelectorAll('[data-radar-reset]').forEach(button => {
    button.onclick = () => {
      resetRadarView();
      renderMarket();
      bindViewButtons();
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

document.querySelectorAll('.nav-item').forEach(button => {
  button.addEventListener('click', () => setView(button.dataset.view));
});

(async () => {
  try {
    await authenticate();
    await bootstrap();
    setView('home');
  } catch (error) {
    showError(error.message || 'No se pudo abrir la mini-app.');
  }
})();
