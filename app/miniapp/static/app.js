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
};

const state = {
  token: null,
  payload: null,
  currentView: 'home',
  signalDetail: null,
  radarDetail: null,
  radarView: { ...DEFAULT_RADAR_VIEW },
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

function setTopSummary() {
  const me = state.payload?.me || {};
  els.planBadge.textContent = String(me.plan_name || 'FREE').toUpperCase();
  els.daysBadge.textContent = `${Number(me.days_left || 0)} días`;
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
  const radarSummary = market.radar_summary || {};
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
              <span class="pill">Mostrando: ${escapeHtml(visibleRadar.length)} / ${escapeHtml(radar.length)}</span>
              <span class="pill">Orden: ${escapeHtml(radarSortLabel(radarView.sort))}</span>
              ${radarView.search ? `<span class="pill">Búsqueda: ${escapeHtml(radarView.search)}</span>` : ''}
            </div>
            <button class="button button-secondary radar-reset-button" data-radar-reset>Reset filtros</button>
          </div>
        </div>

        <div class="radar-card-grid">
          ${visibleRadar.length ? visibleRadar.map(item => {
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
  const primaryAction = primaryCta
    ? (primaryCta.toLowerCase() === 'soporte'
        ? `<a class="button button-secondary" target="_blank" rel="noopener" href="${escapeHtml(supportUrl)}">${escapeHtml(primaryCta)}</a>`
        : `<span class="button button-secondary" aria-disabled="true">${escapeHtml(primaryCta)}</span>`)
    : '';
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
    <div class="card card-span-6">
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
      state.radarView = { ...state.radarView, search: radarSearchInput.value || '' };
      renderMarket();
      bindViewButtons();
    };
  }
  const radarDirectionFilter = document.getElementById('radarDirectionFilter');
  if (radarDirectionFilter) {
    radarDirectionFilter.onchange = () => {
      state.radarView = { ...state.radarView, direction: radarDirectionFilter.value || 'all' };
      renderMarket();
      bindViewButtons();
    };
  }
  const radarPriorityFilter = document.getElementById('radarPriorityFilter');
  if (radarPriorityFilter) {
    radarPriorityFilter.onchange = () => {
      state.radarView = { ...state.radarView, priority: radarPriorityFilter.value || 'all' };
      renderMarket();
      bindViewButtons();
    };
  }
  const radarProximityFilter = document.getElementById('radarProximityFilter');
  if (radarProximityFilter) {
    radarProximityFilter.onchange = () => {
      state.radarView = { ...state.radarView, proximity: radarProximityFilter.value || 'all' };
      renderMarket();
      bindViewButtons();
    };
  }
  const radarSignalFilter = document.getElementById('radarSignalFilter');
  if (radarSignalFilter) {
    radarSignalFilter.onchange = () => {
      state.radarView = { ...state.radarView, signal: radarSignalFilter.value || 'all' };
      renderMarket();
      bindViewButtons();
    };
  }
  const radarExecutionFilter = document.getElementById('radarExecutionFilter');
  if (radarExecutionFilter) {
    radarExecutionFilter.onchange = () => {
      state.radarView = { ...state.radarView, execution: radarExecutionFilter.value || 'all' };
      renderMarket();
      bindViewButtons();
    };
  }
  const radarAlignmentFilter = document.getElementById('radarAlignmentFilter');
  if (radarAlignmentFilter) {
    radarAlignmentFilter.onchange = () => {
      state.radarView = { ...state.radarView, alignment: radarAlignmentFilter.value || 'all' };
      renderMarket();
      bindViewButtons();
    };
  }
  const radarSortFilter = document.getElementById('radarSortFilter');
  if (radarSortFilter) {
    radarSortFilter.onchange = () => {
      state.radarView = { ...state.radarView, sort: radarSortFilter.value || 'ranking' };
      renderMarket();
      bindViewButtons();
    };
  }
  document.querySelectorAll('[data-radar-reset]').forEach(button => {
    button.onclick = () => {
      state.radarView = { ...DEFAULT_RADAR_VIEW };
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
        const result = await api('/api/miniapp/payment-order', {
          method: 'POST',
          body: JSON.stringify({ plan, days: Number(days) }),
        });
        applyPaymentOrderPreview(result.order || null);
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
        const result = await api('/api/miniapp/payment-order/confirm', {
          method: 'POST',
          body: JSON.stringify({ order_id: button.dataset.confirmOrder }),
        });
        applyPaymentOrderPreview(result.order || null);
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
        await api('/api/miniapp/payment-order/cancel', {
          method: 'POST',
          body: JSON.stringify({ order_id: button.dataset.cancelOrder }),
        });
        applyPaymentOrderPreview(null);
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
