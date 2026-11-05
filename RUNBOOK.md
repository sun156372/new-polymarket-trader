# Runbook — Polymarket BTC 5m V7

This is the short operational checklist. Strategy and configuration details are in
`README.md`.

## 1. Preflight

```powershell
python --version
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
```

Use paper mode first:

```yaml
mode: "paper"
strategy:
  force_trade_test: false
```

No private key is needed for paper mode.

## 2. Start

```powershell
.\run.ps1
```

The launcher creates `data/` and `logs/`, uses `.venv` when present, and starts:

```text
python -u main.py --config config.yaml
```

## 3. What healthy startup looks like

- `[Feed]` messages show Binance and Polymarket websocket connections.
- A `[Market]` line selects the current 5-minute market.
- `[MarketSnapshot]` prints once for that market.
- No repeated `[PM 盘口更新]` spam.

## 4. During the final window

- Indicators update continuously from T-240 to market end.
- The entry decision is evaluated once per second.
- A skipped entry is normal when a threshold or readiness gate fails.
- `force_trade_test` must remain `false`.

## 5. At market close

- `spot_close` is frozen at the close.
- Gamma is polled after the start buffer, then every poll interval.
- If Gamma is unresolved at the fallback deadline, the bot settles from
  `spot_open` versus `spot_close`.
- A tie remains unresolved by design.
- A successful settlement logs `[结算]` and removes the position.

## 6. Stop and restart

Stop with `Ctrl+C`. State is in `data/state.json`.

After restart:

- open paper positions are reconciled;
- existing market positions are not duplicated;
- unresolved positions remain visible for manual review.

Do not delete `data/state.json` while a position is open.

## 7. Troubleshooting

**No market found**
: Check the Polymarket Gamma endpoint and the current UTC time. Five-minute markets rotate
  continuously, but discovery can lag at boundaries.

**Websocket connected but no book**
: Wait for the first `book` snapshot. The strategy refuses to trade until both books are
  ready and fresh.

**Gamma unresolved**
: This is expected sometimes. The local fallback handles it after 120 seconds. Do not
  lower the fallback blindly; verify the official market resolution rule first.

**Import errors**
: Activate `.venv` and reinstall `requirements.txt`. Live-only packages are optional for
  paper mode.

**Balance looks wrong**
: Check `[结算]` and the analytics lines. Paper equity is starting balance minus entry
  cost plus settlement payout; open positions are not counted as cash.

## 8. Live transition

Only after paper validation:

1. Create `live_trading_confirm.txt` on the live host with exactly
   `I CONFIRM LIVE TRADING`.
2. Fill `.env` locally; never commit it.
3. Switch `mode: "live"` and keep `force_trade_test: false`.
4. Use one tiny position.
5. Verify the CLOB fill, Gamma resolution, redeem transaction, and USDC receipt.
