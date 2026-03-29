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

function badgeClassByResult(result) {
  if (result === 'won') return 'result-badge result-won';
  if (result === 'lost') return 'result-badge result-lost';
  return 'result-badge result-expired';
}

function dirClass(direction) {
  return String(direction).toUpperCase() === 'SHORT' ? 'dir-badge dir-short' : 'dir-badge dir-long';
}

function showError(message) {
  els.home.innerHTML = `<div class="error-banner">${escapeHtml(message)}</div>`;
  els.loading.classList.add('hidden');
  els.content.classList.remove('hidden');
  els.bottomNav.classList.remove('hidden');
}

async function api(path, options = {}) {
  const headers = Object.assign({}, options.headers || {});
  if (state.token) {
    headers.Authorization = `Bearer ${state.token}`;
  }
  if (options.body && !headers['Content-Type']) {
    headers['Content-Type'] = 'application/json';
  }
  const response = await fetch(path, {
    ...options,
    headers,
  });
  const data = await response.json().catch(() => ({}));
  if (!response.ok) {
    throw new Error(data.detail || data.message || 'request_failed');
  }
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

function metricCard(label, value, subtitle = '') {
  return `
    <div class="card metric-card card-span-3">
      <div class="metric-label">${escapeHtml(label)}</div>
      <div class="metric-value">${escapeHtml(value)}</div>
      <div class="metric-subtitle">${escapeHtml(subtitle)}</div>
    </div>
  `;
}

function renderHome() {
  const me = state.payload.me;
  const dashboard = state.payload.dashboard || {};
  const summary = dashboard.summary_7d || {};
  const activeOrder = dashboard.active_payment_order;
  const recentSignals = dashboard.recent_signals || [];
  const recentHistory = dashboard.recent_history || [];

  els.home.innerHTML = `
    <div class="section-grid">
      ${metricCard('Plan', me.plan_name || 'FREE', me.subscription_status || 'free')}
      ${metricCard('Señales activas', dashboard.active_signals_count || 0, 'Últimas señales del usuario')}
      ${metricCard('Win rate 7D', `${formatNumber(summary.winrate || 0)}%`, 'Base operativa global')}
      ${metricCard('Profit Factor', formatNumber(summary.profit_factor || 0), 'Últimos 7 días')}

      <div class="card card-span-12">
        <h2>Acciones rápidas</h2>
        <div class="action-row">
          <button class="button button-primary" data-goto="signals">Ver señales</button>
          <button class="button button-secondary" data-goto="history">Ver historial</button>
          <button class="button button-secondary" data-goto="market">Mercado</button>
          <button class="button button-secondary" data-goto="account">Mi cuenta</button>
        </div>
      </div>

      <div class="card card-span-12">
        <h2>Actividad reciente</h2>
        <div class="list">
          ${(recentSignals.length ? recentSignals.slice(0, 3).map(item => `
            <div class="item">
              <div class="item-header">
                <div>
                  <div class="item-title">${escapeHtml(item.symbol)} <span class="${dirClass(item.direction)}">${escapeHtml(item.direction)}</span></div>
                  <div class="item-subtitle">Score ${escapeHtml(formatNumber(item.score || 0, 1))} · ${escapeHtml(item.setup_group || 'setup')}</div>
                </div>
                <span class="plan-tag">${escapeHtml(String(item.visibility || '').toUpperCase())}</span>
              </div>
              <div class="inline-meta"><span>${escapeHtml(formatDate(item.created_at))}</span></div>
            </div>
          `).join('') : '<div class="empty-state">Todavía no hay señales recientes para mostrar.</div>')}
        </div>
      </div>

      <div class="card card-span-12">
        <h2>Historial reciente</h2>
        <div class="list">
          ${(recentHistory.length ? recentHistory.slice(0, 3).map(item => `
            <div class="item">
              <div class="item-header">
                <div>
                  <div class="item-title">${escapeHtml(item.symbol)} <span class="${dirClass(item.direction)}">${escapeHtml(item.direction)}</span></div>
                  <div class="item-subtitle">${escapeHtml(item.setup_group || 'setup')} · Score ${escapeHtml(formatNumber(item.score || 0, 1))}</div>
                </div>
                <span class="${badgeClassByResult(item.result)}">${escapeHtml(String(item.result || 'unknown').toUpperCase())}</span>
              </div>
              <div class="inline-meta"><span>${escapeHtml(formatDate(item.signal_created_at))}</span><span>${escapeHtml(item.r_multiple !== null && item.r_multiple !== undefined ? `R ${formatNumber(item.r_multiple, 2)}` : '')}</span></div>
            </div>
          `).join('') : '<div class="empty-state">No hay histórico reciente todavía.</div>')}
        </div>
      </div>

      ${activeOrder ? `
      <div class="card card-span-12">
        <h2>Orden de pago activa</h2>
        <div class="item">
          <div class="item-header">
            <div>
              <div class="item-title">${escapeHtml(String(activeOrder.plan).toUpperCase())} · ${escapeHtml(activeOrder.days)} días</div>
              <div class="item-subtitle">${escapeHtml(formatMoney(activeOrder.amount_usdt))} · ${escapeHtml(String(activeOrder.network || '').toUpperCase())}</div>
            </div>
            <span class="plan-tag">${escapeHtml(String(activeOrder.status || '').toUpperCase())}</span>
          </div>
          <div class="inline-meta"><span>Expira: ${escapeHtml(formatDate(activeOrder.expires_at))}</span></div>
          <div class="action-row">
            <button class="button button-success" data-confirm-order="${escapeHtml(activeOrder.order_id)}">Confirmar pago</button>
            <button class="button button-danger" data-cancel-order="${escapeHtml(activeOrder.order_id)}">Cancelar orden</button>
          </div>
        </div>
      </div>
      ` : ''}
    </div>
  `;
}

function renderSignals() {
  const signals = state.payload.signals || [];
  els.signals.innerHTML = `
    <div class="card"><h2>Señales recientes</h2><p>Vista rápida de las señales entregadas a tu usuario dentro del bot.</p></div>
    <div class="list" style="margin-top:12px;">
      ${signals.length ? signals.map(item => `
        <div class="item">
          <div class="item-header">
            <div>
              <div class="item-title">${escapeHtml(item.symbol)} <span class="${dirClass(item.direction)}">${escapeHtml(item.direction)}</span></div>
              <div class="item-subtitle">${escapeHtml(item.setup_group || 'setup')} · Score ${escapeHtml(formatNumber(item.score || 0, 1))}</div>
            </div>
            <span class="plan-tag">${escapeHtml(String(item.visibility || '').toUpperCase())}</span>
          </div>
          <div class="inline-meta">
            <span>Emitida: ${escapeHtml(formatDate(item.created_at))}</span>
            <span>Estado: ${escapeHtml(String(item.status || 'active').toUpperCase())}</span>
            <span>Entrada: ${escapeHtml(item.entry_price ?? '—')}</span>
          </div>
        </div>
      `).join('') : '<div class="empty-state">No hay señales disponibles todavía.</div>'}
    </div>
  `;
}

function renderMarket() {
  const market = state.payload.market || {};
  const movers = market.top_gainers || [];
  const radar = market.radar || [];
  const watchlist = state.payload.watchlist || [];
  els.market.innerHTML = `
    <div class="section-grid">
      <div class="card card-span-6">
        <h2>Market Pulse</h2>
        <div class="pill-row">
          <span class="pill">Bias: ${escapeHtml(market.bias || '—')}</span>
          <span class="pill">Régimen: ${escapeHtml(market.regime || '—')}</span>
          <span class="pill">Volatilidad: ${escapeHtml(market.volatility || '—')}</span>
          <span class="pill">Entorno: ${escapeHtml(market.environment || '—')}</span>
        </div>
        <p style="margin-top:12px;">${escapeHtml(market.recommendation || 'Sin recomendación disponible por ahora.')}</p>
      </div>
      <div class="card card-span-6">
        <h2>Movers</h2>
        <div class="list">
          ${movers.length ? movers.slice(0, 5).map(item => `
            <div class="item">
              <div class="item-header">
                <div class="item-title">${escapeHtml(item.symbol)}</div>
                <span class="plan-tag">${escapeHtml(formatNumber(item.change, 2))}%</span>
              </div>
            </div>
          `).join('') : '<div class="empty-state">Sin movers disponibles.</div>'}
        </div>
      </div>
      <div class="card card-span-6">
        <h2>Radar</h2>
        <div class="list">
          ${radar.length ? radar.map(item => `
            <div class="item">
              <div class="item-header">
                <div class="item-title">${escapeHtml(item.symbol)}</div>
                <span class="${dirClass(item.direction)}">${escapeHtml(item.direction)}</span>
              </div>
              <div class="inline-meta"><span>Score ${escapeHtml(item.score)}</span><span>${escapeHtml(formatNumber(item.change_pct, 2))}%</span></div>
            </div>
          `).join('') : '<div class="empty-state">Sin radar disponible.</div>'}
        </div>
      </div>
      <div class="card card-span-6">
        <h2>Watchlist</h2>
        <div class="list">
          ${watchlist.length ? watchlist.map(item => `
            <div class="item">
              <div class="item-header">
                <div class="item-title">${escapeHtml(item.symbol)}</div>
                <span class="plan-tag">${escapeHtml(formatNumber(item.change_pct, 2))}%</span>
              </div>
              <div class="inline-meta"><span>Precio ${escapeHtml(item.last_price)}</span></div>
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
      ${items.length ? items.map(item => `
        <div class="item">
          <div class="item-header">
            <div>
              <div class="item-title">${escapeHtml(item.symbol)} <span class="${dirClass(item.direction)}">${escapeHtml(item.direction)}</span></div>
              <div class="item-subtitle">${escapeHtml(item.setup_group || 'setup')} · Score ${escapeHtml(formatNumber(item.score || 0, 1))}</div>
            </div>
            <span class="${badgeClassByResult(item.result)}">${escapeHtml(String(item.result || 'unknown').toUpperCase())}</span>
          </div>
          <div class="inline-meta">
            <span>Fecha: ${escapeHtml(formatDate(item.signal_created_at))}</span>
            <span>Resolución: ${escapeHtml(item.resolution_minutes ?? '—')} min</span>
            <span>R múltiple: ${escapeHtml(item.r_multiple ?? '—')}</span>
          </div>
        </div>
      `).join('') : '<div class="empty-state">No hay historial disponible por ahora.</div>'}
    </div>
  `;
}

function planBlock(planKey, items) {
  return `
    <div class="card card-span-6">
      <h2>${escapeHtml(String(planKey).toUpperCase())}</h2>
      <div class="list">
        ${items.map(item => `
          <div class="item">
            <div class="item-header">
              <div>
                <div class="item-title">${escapeHtml(item.days)} días</div>
                <div class="item-subtitle">${escapeHtml(formatMoney(item.price_usdt))}</div>
              </div>
              <button class="button button-primary" data-create-order="${escapeHtml(planKey)}:${escapeHtml(item.days)}">Comprar</button>
            </div>
          </div>
        `).join('')}
      </div>
    </div>
  `;
}

function renderAccount() {
  const me = state.payload.me;
  const plans = state.payload.plans || {};
  const activeOrder = state.payload.dashboard?.active_payment_order || null;
  els.account.innerHTML = `
    <div class="section-grid">
      <div class="card card-span-12">
        <h2>Mi cuenta</h2>
        <div class="pill-row">
          <span class="pill">Plan: ${escapeHtml(me.plan_name || 'FREE')}</span>
          <span class="pill">Estado: ${escapeHtml(me.subscription_status || 'free')}</span>
          <span class="pill">Idioma: ${escapeHtml(me.language || 'es')}</span>
          <span class="pill">Referidos válidos: ${escapeHtml(me.valid_referrals_total || 0)}</span>
        </div>
      </div>
      ${planBlock('plus', plans.plus || [])}
      ${planBlock('premium', plans.premium || [])}
      <div class="card card-span-12">
        <h2>Pago actual</h2>
        ${activeOrder ? `
          <div class="item">
            <div class="item-header">
              <div>
                <div class="item-title">${escapeHtml(String(activeOrder.plan).toUpperCase())} · ${escapeHtml(activeOrder.days)} días</div>
                <div class="item-subtitle">${escapeHtml(formatMoney(activeOrder.amount_usdt))} · ${escapeHtml(String(activeOrder.network || '').toUpperCase())}</div>
              </div>
              <span class="plan-tag">${escapeHtml(String(activeOrder.status || '').toUpperCase())}</span>
            </div>
            <div class="action-row">
              <a href="#" class="code-chip">${escapeHtml(activeOrder.deposit_address || '')}</a>
            </div>
            <div class="inline-meta"><span>Expira: ${escapeHtml(formatDate(activeOrder.expires_at))}</span></div>
            <div class="action-row">
              <button class="button button-success" data-confirm-order="${escapeHtml(activeOrder.order_id)}">Confirmar pago</button>
              <button class="button button-danger" data-cancel-order="${escapeHtml(activeOrder.order_id)}">Cancelar orden</button>
            </div>
          </div>
        ` : '<div class="empty-state">No tienes una orden de pago pendiente.</div>'}
      </div>
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

function bindViewButtons() {
  document.querySelectorAll('[data-goto]').forEach(button => {
    button.onclick = () => setView(button.dataset.goto);
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
