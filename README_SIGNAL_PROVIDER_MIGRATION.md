# Signal provider migration

This patch removes the legacy direct Binance dependency from `app.signals`.

## What changed

- `app.signals.get_current_price()` now uses `app.market_data_public.get_public_current_price()`.
- `app.signals._fetch_klines_between()` now uses `app.market_data_public.get_public_klines_between_rows()`.
- Signal lifecycle evaluation now follows the same public provider priority as the scanner: `MARKET_DATA_PROVIDERS`, default `bybit,okx,binance`.
- Telegram/httpx bot token URLs are redacted from logs via a lightweight logging filter in `app.bot`.

## Why

The scanner had already been migrated to public market providers, but active signal evaluation still called Binance Futures directly. That caused HTTP 451 errors from cloud regions even when Bybit/OKX were healthy.

## Consumption profile

No extra scanner market requests were added. The new lifecycle range fetch uses short TTL caching and only runs when signal evaluation needs lifecycle candles.
