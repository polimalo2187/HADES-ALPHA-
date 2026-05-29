# Watchlist add feedback fix

Fixes MiniApp watchlist add failures that appeared only as `POST /api/miniapp/watchlist/add 400 Bad Request` in Railway logs.

## Changes

- Validation rejections from `/api/miniapp/watchlist/add` now return a business payload with `ok=false` and the current watchlist context instead of an HTTP 400.
- The MiniApp now shows the backend message using Telegram `showAlert` when available and falls back to `window.alert` outside Telegram.
- Railway logs stop hiding the real validation reason behind generic 400 access logs.

Transport/auth/malformed request errors can still return HTTP errors. Symbol-not-supported, plan-limit, duplicate, and invalid-symbol style outcomes now produce visible user feedback.
