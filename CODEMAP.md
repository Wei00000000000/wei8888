# Wei8888 Code Map

Use this map before editing so fixes stay scoped and token usage stays low.

## Frontend Shell

- `app.html`
  - Main single-page app, cards, navigation, stats, backtests, live UI updates.
  - API base: `CLOUD_API_BASE`, `apiJson()`.
  - Live prices: `refreshPrices()`, `cloudLivePrices()`, `staticPrices()`, `fetchBingxTickers()`, `fetchOkxTickers()`, `renderVisibleNumbers()`.
  - Signal rendering: `signalCard()`, `renderSignals()`, `renderStats()`, `displayState()`, `pnlPct()`, `currentPrice()`.
  - Contract radar: `renderContractRadar()`, `renderContractVisualScreener()`, `contractDirectionGroup()`.
  - High quality: `renderHighQuality()`, `highQualitySignals()`.
  - Notifications: `notifySignalChanges()`, `renderNotifications()`, notification memory keys.
- `index.html`
  - Copy of `app.html` for static hosting fallback.
- `login.html`
  - Password gate and Zeabur login flow.
- `api-config.js`
  - GitHub Pages uses Zeabur API; Zeabur-hosted frontend uses same-origin `/api/v1`.

## Backend API

- `backend/app/main.py`
  - FastAPI app, API routers, Zeabur static frontend serving.
- `backend/app/routers/signals.py`
  - Signal listing and active signal API.
- `backend/app/routers/market.py`
  - Latest stored market snapshots.
- `backend/app/routers/backtest.py`
  - Backtest summaries and trade history.
- `backend/app/routers/positions.py`
  - Position history and CSV export.
- `backend/app/routers/notifications.py`
  - Notification logs and Telegram test.
- `backend/app/security_headers.py`
  - CSP/security headers for combined frontend + API hosting.

## Scanner / Data Jobs

- `backend/app/worker.py`
  - 24h background worker, currently scans every 5 minutes.
- `backend/app/jobs.py`
  - Runs scanner script, imports data into DB, syncs positions.
- `scripts/update_seed_signals.py`
  - Main strategy scan and signal generation logic.
  - Entry locking: `lock_signal_to_latest_price()`.
  - Market provider routing: `market_client()`, `provider_name()`.
- `scripts/export_github_pages.py`
  - Static export fallback for GitHub Pages data.

## Market Data Providers

- `sentiment_scanner/market_data.py`
  - Mixed provider router.
  - Price/K-line: BingX first, OKX second.
  - OI: Bybit.
- `sentiment_scanner/bingx.py`
  - BingX futures prices/tickers/K-lines.
- `sentiment_scanner/okx.py`
  - OKX futures prices/tickers/K-lines.
- `sentiment_scanner/bybit.py`
  - Bybit OI and fallback market data.
- `sentiment_scanner/coinglass.py`
  - CoinGlass contract data.

## Tests

- `backend/tests/test_market_data_routing.py`
  - Verifies price/OI provider routing.
- `backend/tests/test_signal_integrity.py`
  - Verifies entry/SL/TP immutability.
- `backend/tests/test_trade_layer.py`
  - Verifies high quality/official trade gating.
- `backend/tests/test_notifications.py`
  - Verifies Telegram message formatting.
- `backend/tests/test_importer_precedence.py`
  - Verifies import precedence and data preservation.

## Editing Rules

- Price not moving: edit `app.html` live price functions first, then `sentiment_scanner/*` only if source data is wrong.
- Signal not appearing: inspect `scripts/update_seed_signals.py`, then `backend/app/jobs.py`, then API routers.
- Wrong position status: inspect `displayState()`/`updateLiveSignalStates()` for frontend display and `backend/app/positions.py` for persisted state.
- Wrong win rate/PnL: inspect `pnlPct()`, `finalWinStats()`, `backend/app/stats.py`, and backtest routers.
- Notification repeat/missing: inspect notification memory in `app.html`, then `backend/app/notifications.py`.
- Do not delete historical data when changing strategy rules. Add filters/labels instead.
