const tg = window.Telegram?.WebApp;
if (tg) {
  tg.ready();
  tg.expand();
}

const state = {
  token: null,
  payload: null,
  currentView: 'home',
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
  const normalized = typeof itemOrResult === 'object' && itemOrResult !== null ? String(itemOrResult.result || '').toLowerCase() : String(itemOrResult || '').toLowerCase();
  if (normalized === 'won') return 'result-badge result-won';
  if (normalized === 'lost') return 'result-badge result-lost';
  return 'result-badge result-expired';
}

function dirClass(direction) {
  return String(direction).toUpperCase() === 'SHORT' ? 'dir-badge dir-short' : 'dir-badge dir-long';
}

function sideClassByValue(value) {
  return Number(value || 0) >= 0 ? 'positive-text' : 'negative-text';
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

function setTopSummary() {
  const me = state.payload?.me || {};
  els.planBadge.textContent = String(me.plan_name || 'FREE').toUpperCase();
  els.daysBadge.textContent = `${Number(me.days_left || 0)} días`;
}

function metricCard(label, value, subtitle = '', extraClass = '') {
  return `
    <div class="card metric-card card-span-3 ${extraClass}">
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
    ? `<span class="${badgeClassByResult(item.result)}">${escapeHtml(String(item.result).toUpperCase())}</span>`
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
    </div>
  `;
}

function paymentInstructions(order) {
  if (!order) return '';
  const address = order.deposit_address || '';
  const addressHref = address ? `https://bscscan.com/address/${encodeURIComponent(address)}` : '#';
  const uniqueExtra = order.amount_unique_delta ? `(+${formatMoney(order.amount_unique_delta)} único)` : 'Monto único por orden';
  return `
    <div class="card payment-card card-span-12">
      <h2>Pago actual</h2>
      <div class="item">
        <div class="item-header">
          <div>
            <div class="item-title">${escapeHtml(order.plan_name || String(order.plan || '').toUpperCase())} · ${escapeHtml(order.days)} días</div>
            <div class="item-subtitle">Red ${escapeHtml(String(order.network || '').toUpperCase())} · ${escapeHtml(order.token_symbol || 'USDT')}</div>
          </div>
          <span class="plan-tag">${escapeHtml(formatStatusLabel(order.status_label || order.status))}</span>
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
          <div class="notice-item">Envía exactamente el monto indicado.</div>
          <div class="notice-item">Usa únicamente la red BEP-20.</div>
          <div class="notice-item">Expira: ${escapeHtml(formatDate(order.expires_at))}</div>
        </div>

        <div class="action-row">
          <button class="button button-success" data-confirm-order="${escapeHtml(order.order_id)}">Confirmar pago</button>
          <button class="button button-danger" data-cancel-order="${escapeHtml(order.order_id)}">Cancelar orden</button>
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
          <div class="hero-side">
            <div class="hero-side-value">${escapeHtml(formatNumber(summary.winrate || 0))}%</div>
            <div class="hero-side-label">Win rate 7D</div>
            <div class="hero-side-meta">Actualizado: ${escapeHtml(formatDate(generatedAt))}</div>
          </div>
        </div>
      </div>

      ${metricCard('Señales activas', dashboard.active_signals_count || 0, 'Visibles ahora mismo')}
      ${metricCard('PF señales (R)', formatNumber(summary.profit_factor || 0), 'Solo resueltas: TP1 / TP2 / SL')}
      ${metricCard('Expectancy R', formatNumber(summary.expectancy_r || 0), 'Resueltas, sin expiradas limpias')}
      ${metricCard('Max DD (R)', formatNumber(summary.max_drawdown_r || 0), 'Peor racha reciente en R')}

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
  const gainers = market.top_gainers || [];
  const losers = market.top_losers || [];
  const radar = market.radar || [];
  const topVolume = market.top_volume || [];
  const btc = market.btc || {};
  const eth = market.eth || {};

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

      <div class="card card-span-6">
        <h2>Radar</h2>
        <div class="list">
          ${radar.length ? radar.map(item => `
            <div class="item compact-item">
              <div class="item-header">
                <div class="item-title">${escapeHtml(item.symbol)}</div>
                <span class="${dirClass(item.direction)}">${escapeHtml(item.direction)}</span>
              </div>
              <div class="inline-meta">
                <span>Score ${escapeHtml(item.score)}</span>
                <span class="${sideClassByValue(item.change_pct)}">${escapeHtml(formatPercentSigned(item.change_pct, 2))}</span>
                <span>${escapeHtml(item.momentum || 'Momentum')}</span>
              </div>
            </div>
          `).join('') : '<div class="empty-state">Sin radar disponible.</div>'}
        </div>
      </div>

      <div class="card card-span-6">
        <h2>Watchlist</h2>
        <div class="list">
          ${watchlist.length ? watchlist.map(item => `
            <div class="item compact-item">
              <div class="item-header">
                <div class="item-title">${escapeHtml(item.symbol)}</div>
                <span class="${sideClassByValue(item.change_pct)}">${escapeHtml(formatPercentSigned(item.change_pct, 2))}</span>
              </div>
              <div class="inline-meta">
                <span>Precio ${escapeHtml(formatNumber(item.last_price, 4))}</span>
                <span>Vol ${escapeHtml(formatCompactAmount(item.quote_volume))}</span>
              </div>
            </div>
          `).join('') : '<div class="empty-state">Tu watchlist está vacía.</div>'}
        </div>
      </div>
    </div>
  `;
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

function planBlock(planKey, items, currentPlan) {
  const current = String(currentPlan || '').toLowerCase();
  const featureRows = items[0]?.features || [];
  const isCurrentPlan = items[0]?.is_current_plan || current === planKey;
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
          const cta = isCurrentPlan ? 'Renovar' : 'Comprar';
          return `
            <div class="item">
              <div class="item-header">
                <div>
                  <div class="item-title">${escapeHtml(item.days)} días</div>
                  <div class="item-subtitle">${escapeHtml(formatMoney(item.price_usdt))}</div>
                </div>
                <button class="button ${isCurrentPlan ? 'button-secondary' : 'button-primary'}" data-create-order="${escapeHtml(planKey)}:${escapeHtml(item.days)}">${cta}</button>
              </div>
            </div>
          `;
        }).join('')}
      </div>
    </div>
  `;
}

function renderAccount() {
  const me = state.payload.me || {};
  const plans = state.payload.plans || {};
  const activeOrder = state.payload.dashboard?.active_payment_order || null;
  const expiresText = me.expires_at ? formatDate(me.expires_at) : 'Sin vencimiento';
  els.account.innerHTML = `
    <div class="section-grid">
      <div class="card card-span-12">
        <h2>Mi cuenta</h2>
        <div class="pill-row">
          <span class="pill">Plan: ${escapeHtml(me.plan_name || 'FREE')}</span>
          <span class="pill">Estado: ${escapeHtml(me.subscription_status_label || me.subscription_status || 'free')}</span>
          <span class="pill">Vence: ${escapeHtml(expiresText)}</span>
          <span class="pill">Días restantes: ${escapeHtml(me.days_left || 0)}</span>
          <span class="pill">Idioma: ${escapeHtml(me.language || 'es')}</span>
          <span class="pill">Referidos válidos: ${escapeHtml(me.valid_referrals_total || 0)}</span>
          <span class="pill">Días ganados: ${escapeHtml(me.reward_days_total || 0)}</span>
        </div>
      </div>

      <div class="card card-span-12">
        <h2>Suscripción actual</h2>
        <p>${escapeHtml(me.plan_name || 'FREE')} · ${escapeHtml(me.subscription_status_label || me.subscription_status || 'free')} · ${escapeHtml(expiresText)}</p>
        <div class="inline-meta">
          <span>ID usuario: ${escapeHtml(me.user_id)}</span>
          <span>Código referido: ${escapeHtml(me.ref_code || '—')}</span>
        </div>
      </div>

      ${planBlock('plus', plans.plus || [], me.plan)}
      ${planBlock('premium', plans.premium || [], me.plan)}
      ${paymentInstructions(activeOrder) || '<div class="card card-span-12"><h2>Pago actual</h2><div class="empty-state">No tienes una orden de pago pendiente.</div></div>'}
      <div class="card card-span-12">
        <h2>Soporte</h2>
        <div class="action-row">
          <a class="button button-secondary" target="_blank" rel="noopener" href="${escapeHtml(state.payload.support_url || '#')}">Abrir grupo de soporte</a>
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
  try {
    await navigator.clipboard.writeText(String(value || ''));
    tg?.showAlert(successMessage);
  } catch {
    tg?.showAlert('No se pudo copiar.');
  }
}

function bindViewButtons() {
  document.querySelectorAll('[data-goto]').forEach(button => {
    button.onclick = () => setView(button.dataset.goto);
  });
  document.querySelectorAll('[data-copy-value]').forEach(button => {
    button.onclick = () => copyValue(button.dataset.copyValue, 'Copiado correctamente.');
  });
  document.querySelectorAll('[data-create-order]').forEach(button => {
    button.onclick = async () => {
      const [plan, days] = button.dataset.createOrder.split(':');
      try {
        const result = await api('/api/miniapp/payment-order', {
          method: 'POST',
          body: JSON.stringify({ plan, days: Number(days) }),
        });
        state.payload.dashboard.active_payment_order = result.order;
        renderAccount();
        renderHome();
        bindViewButtons();
        setView('account');
        tg?.showAlert('Orden de pago generada correctamente.');
      } catch (error) {
        tg?.showAlert(`No se pudo generar la orden: ${error.message}`);
      }
    };
  });
  document.querySelectorAll('[data-confirm-order]').forEach(button => {
    button.onclick = async () => {
      try {
        const result = await api('/api/miniapp/payment-order/confirm', {
          method: 'POST',
          body: JSON.stringify({ order_id: button.dataset.confirmOrder }),
        });
        await bootstrap();
        tg?.showAlert(result.ok ? 'Pago confirmado correctamente.' : `Pago pendiente: ${result.reason || 'no_match'}`);
      } catch (error) {
        tg?.showAlert(`No se pudo confirmar: ${error.message}`);
      }
    };
  });
  document.querySelectorAll('[data-cancel-order]').forEach(button => {
    button.onclick = async () => {
      try {
        await api('/api/miniapp/payment-order/cancel', {
          method: 'POST',
          body: JSON.stringify({ order_id: button.dataset.cancelOrder }),
        });
        await bootstrap();
        tg?.showAlert('Orden cancelada correctamente.');
      } catch (error) {
        tg?.showAlert(`No se pudo cancelar: ${error.message}`);
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
