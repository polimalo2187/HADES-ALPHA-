# Market Provider Merged Coverage Fix

## Problema

La capa pública de mercado ya tenía failover por proveedor (`bybit,okx,binance`), pero el endpoint de tickers 24h devolvía el primer proveedor exitoso. Eso era correcto para disponibilidad, pero no para cobertura completa: si Bybit respondía bien, símbolos que solo existían en OKX o Binance no aparecían en Mercado/Watchlist/Radar.

Síntomas vistos:

- símbolos como `SIRENUSDT` o `PLAYUSDT` mostraban `Sin cobertura pública`;
- el scanner podía tener datos, pero el mercado/watchlist quedaba limitado al universo del primer proveedor exitoso;
- al mover el backend a una región donde Binance ya no da 451, Binance seguía sin aportar cobertura al listado porque Bybit/OKX respondían primero.

## Solución

`app.market_data_public.get_public_24h_tickers()` ahora consulta todos los proveedores configurados y une sus universos:

```env
MARKET_DATA_PROVIDERS=bybit,okx,binance
```

Reglas:

1. Si el símbolo existe en el primer proveedor, se conserva ese dato como fuente canónica.
2. Si el símbolo no existe en el primer proveedor pero sí existe en el segundo o tercero, se agrega.
3. Cada ticker conserva su `provider` real.
4. Cada ticker incluye `providers_available` para diagnóstico.
5. La lista final queda ordenada por `quoteVolume` descendente.

## Impacto de consumo

La implementación sigue siendo ligera:

- solo agrega hasta 3 llamadas bulk de tickers cada TTL;
- no agrega llamadas por símbolo;
- reutiliza cache `PUBLIC_MARKET_TICKERS_TTL_SECONDS`;
- no afecta el path de klines, señales o tracking intrabar.

## Resultado esperado

En una región donde Binance, Bybit y OKX respondan, Mercado/Watchlist/Radar deben cubrir símbolos de los tres proveedores. Binance ya no es solo fallback cuando fallan los demás; también aporta cobertura de símbolos que los proveedores anteriores no tienen.
