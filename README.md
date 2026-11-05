# Polymarket BTC 5m V7

Paper/live trading bot for Polymarket **BTC UP/DOWN 5-minute** markets.

> Research and engineering code, not financial advice. Prediction markets can lose the
> entire stake. Start in paper mode and validate live execution with a tiny position.

## Status

- Paper mode has been exercised end-to-end: entry, market close, Gamma polling, local
  fallback settlement, balance update, and state cleanup.
- Live order placement and on-chain redemption are implemented but have **not** been
  validated with real funds in this repository.
- No historical backtest is bundled. Do not treat paper results as a profitability claim.

## Strategy

At **T-240** (60 seconds after the 5-minute market opens), the bot evaluates the final
240-second window:

1. Track the UP-token mid move and require `|move| >= 0.20`.
2. Track oriented top-10 book imbalance and require `|imbalance| >= 0.30`.
3. Require `|spot move| >= 2.0 bps`.
4. Buy the selected side's ask, capped at `max_entry_price: 0.95`.
5. Hold to settlement by default (`enable_take_profit: false`).

If the book is not ready, the strategy refuses to open a position. `force_trade_test`
bypasses thresholds for plumbing tests only and must stay `false` in normal operation.

## Layout

```text
polymarket-sniper-v7/
├── main.py                 # event loop, market lifecycle, paper settlement
├── config.yaml             # non-secret configuration
├── .env.example            # secret placeholders
├── requirements.txt
├── run.ps1                 # Windows launcher
├── core/
│   ├── feed.py             # Polymarket + Binance websocket feeds, book state
│   ├── strategy.py         # T-240 entry rules
│   ├── executor.py         # paper/live execution and fills
│   ├── state_manager.py    # crash-safe JSON state
│   └── redeem_engine.py    # live on-chain redemption (optional)
└── utils/
    └── analytics.py        # balance, win rate, drawdown
```

## Requirements

- Python 3.11 or newer.
- Windows PowerShell for `run.ps1`; Linux/macOS can call `python main.py` directly.

Optional live-only packages (`py-clob-client`, `web3`, `eth-account`) are listed in
`requirements.txt`. Paper mode does not need a private key.

## Install

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
pip install -r requirements.txt
Copy-Item .env.example .env
```

For paper mode, leave the `.env` values blank.

## Configure

`config.yaml` contains non-secret settings only. Secret values are read from environment
variables:

```dotenv
POLYMARKET_PRIVATE_KEY=
POLYMARKET_FUNDER=
POLYGON_RPC_URL=https://polygon-rpc.com
```

Never commit `.env`, `data/state.json`, `logs/`, or `live_trading_confirm.txt`.

Paper mode is the default:

```yaml
mode: "paper"
strategy:
  force_trade_test: false
```

## Run

```powershell
.\run.ps1
```

or:

```powershell
python main.py --config config.yaml
```

At startup the bot discovers the current 5-minute market, connects to Polymarket and
Binance websockets, and waits for the final entry window.

## Resolution and settlement

When the market closes, `main.py` freezes the final Binance spot price as `spot_close`.

1. The bot waits `resolution.start_buffer_s` (default 5 seconds).
2. It polls Gamma every `resolution.poll_interval_s` (default 5 seconds).
3. Gamma resolution requires `closed=true` plus a consistent outcome/outcome-price
   mapping. `winningOutcome` alone is not trusted.
4. If Gamma has not resolved by `resolution.fallback_after_s` (default 120 seconds),
   the bot settles locally from `spot_close` versus `spot_open`.
5. A tie remains unresolved; it is not guessed.
6. After `resolution.hard_deadline_s` (default 300 seconds), an unresolved position is
   marked `unresolved` for manual review.

A successful settlement removes the position from state and updates:

- executor balance and analytics balance;
- realized PnL;
- win count and win rate;
- drawdown;
- the `[结算]` log line with market, winner, side, final price, source, PnL, and balance.

## State and restart

`data/state.json` is the source of truth. Open positions are reserved before an order is
submitted, which prevents duplicate orders after a restart. Paper positions that are still
open are reconciled against the executor balance on startup.

## Risk controls

`config.yaml` contains:

```yaml
risk:
  stake_usdc: 10.0
  starting_balance_usdc: 1000.0
  max_concurrent_positions: 2
  daily_max_loss_usdc: 30.0
  max_trades_per_day: 40
```

The runtime enforces position concurrency, daily loss, balance, and one position per
market. `max_trades_per_day` is available for the executor layer; verify it is enforced
before relying on it.

## Logs

- `logs/v7.log`: rotating application log.
- `[MarketSnapshot]`: one detailed snapshot per market, not per websocket message.
- `[结算]`: detailed settlement record.

## Tests and limitations

- No automated test suite is bundled.
- Gamma may lag or remain unresolved; the 120-second local fallback exists for that case.
- The fallback assumes the official 5-minute market resolves on the Binance spot open vs
  close. A tie is left unresolved.
- `redeem_engine.py` supports standard binary CTF markets; `negRisk=true` is rejected.
- On-chain redemption is optional and has not been live-tested here.

## Live checklist

- [ ] Paper mode runs for at least 24 hours.
- [ ] Kill/restart mid-position and verify state recovery.
- [ ] Verify one settlement, including the `[结算]` log.
- [ ] Create `live_trading_confirm.txt` only on the live host.
- [ ] Start with one tiny live position and confirm the fill.
- [ ] Confirm the redeem transaction and USDC receipt.
- [ ] Never commit `.env` or a private key.
