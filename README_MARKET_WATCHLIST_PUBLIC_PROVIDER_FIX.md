# Market / Watchlist public provider fix

This patch removes the remaining Binance-only assumptions from Market, Watchlist and Radar UI flows.

## What changed

- `app.market_data_public` now normalizes 24h ticker high/low/price-change fields from Bybit and OKX.
- Watchlist and Radar no longer show user-facing `Sin datos de Binance` when data is missing.
- Missing symbols now show provider-neutral wording: public provider coverage is unavailable for that symbol.
- MiniApp browser no longer opens Binance WebSocket directly.
- Telegram market/movers copy now says public providers instead of Binance-only.
- Existing `app.binance_api` compatibility functions still work, but data is sourced through public provider failover.

## Why this matters

The scanner had already moved to public providers, but Market/Watchlist/Radar still had old Binance assumptions in labels and browser-side streaming. This caused cards to show stale Binance-specific messages even when the backend was using Bybit/OKX.

## Production note

Some symbols may genuinely be unsupported by the configured providers. If a watchlist symbol exists only on Binance and Binance is blocked from the cloud region, the app should now report it as no public-provider coverage instead of implying a Binance system failure.
