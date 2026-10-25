# polymarket-sniper — V7

Production-oriented Polymarket BTC 5m sniper.

- **T-240 decision** (60 seconds into the 5m market)
- follow the Up-token mid move, `|move| >= 0.20`
- top-10 book imbalance, oriented, `|imbalance| >= 0.30`
- spot move, `|spot move| >= 2.0 bps`
- buy the side's **ask**, cap `max_entry_price: 0.95`
- **hold to settlement** (`enable_take_profit: false`; 0.98 TP was tested and rejected)
- paper and live modes
- crash-safe `data/state.json`
- async auto-redeem every 60s

> This code is for research/engineering. Prediction-market trading can lose the entire stake. Start with paper, then a tiny live position.

## 1. Layout

```text
polymarket-sniper/
├── .gitignore
├── .env.example
├── requirements.txt
├── config.yaml
├── main.py
├── README.md
├── core/
│   ├── __init__.py
│   ├── feed.py
│   ├── strategy.py
│   ├── executor.py
│   ├── state_manager.py
│   └── redeem_engine.py
└── utils/
    ├── __init__.py
    └── analytics.py
```

## 2. Install

```bash
cd polymarket-sniper
python -m venv .venv
# Windows: .venv\Scripts\activate
# Linux/macOS: source .venv/bin/activate
pip install -r requirements.txt
```

`web3`/`eth-account` are only needed for redeem. `py-clob-client` is only needed for live orders.

## 3. Configure

Copy `.env.example` to `.env` and fill it **locally**:

```bash
cp .env.example .env
```

```dotenv
POLYMARKET_PRIVATE_KEY=0x...
POLYMARKET_FUNDER=
POLYGON_RPC_URL=https://polygon-rpc.com
```

Rules:

- **Never put a private key in `config.yaml` or git.**
- `config.yaml` only stores env-var **names**:
  - `polymarket.private_key_env: POLYMARKET_PRIVATE_KEY`
  - `redeem.rpc_url_env: POLYGON_RPC_URL`
- `.env`, `data/state.json`, `logs/`, and `live_trading_confirm.txt` are gitignored.

## 4. Paper mode

```yaml
mode: "paper"
```

Run:

```bash
python main.py --config config.yaml
```

Paper mode:

- reads live Polymarket/Binance websockets;
- fills at `ask + slippage`;
- charges `0.07 * p * (1-p)` entry fee;
- settles at 0/1;
- writes state and prints win rate / balance / drawdown.

No private key is needed.

## 5. Live mode

### 5.1 Confirm file

Create `live_trading_confirm.txt` in the project root with exactly:

```text
I CONFIRM LIVE TRADING
```

If the file is missing or has different content, `LiveExecutor` refuses to initialize.

### 5.2 Environment

```dotenv
POLYMARKET_PRIVATE_KEY=0x...
POLYMARKET_FUNDER=          # optional proxy/funder address
POLYGON_RPC_URL=https://polygon-rpc.com
```

### 5.3 Switch mode

```yaml
mode: "live"
```

Run:

```bash
python main.py --config config.yaml
```

Live orders use `py_clob_client` market orders (`FOK`). If the package or credentials are missing, startup fails closed.

## 6. State recovery

`data/state.json` is the source of truth.

On every open:

1. reserve `pending_fill` (prevents duplicate orders on restart);
2. submit order;
3. on fill mark `open`;
4. on failure remove the reservation.

On restart:

```text
[State] recovered N active position(s)
```

`pending_fill` is deliberately not auto-redeemed: the executor must reconcile the order after restart.

## 7. Auto-redeem

Runs only in live mode (configurable).

Every 60 seconds, for positions past `end_time + 30s`:

1. resolve the market via Gamma `?slug=...`;
2. losing side → clear record;
3. winning side → `ConditionalTokens.redeemPositions` on Polygon;
4. log:

```text
[Redeem] 成功赎回市场 btc-updown-5m-...，金额 10.000000 USDC。tx=0x...
```

Contract config:

```yaml
redeem:
  interval_s: 60
  conditional_tokens: "0x4D97DCd97eC945f40cF65F87097ACe5EA0476045"
  collateral_usdc: "0x2791Bca1f2de4661ED88A30C99A7a9449Aa84174"
```

**Limitations**

- standard binary CTF only; `negRisk=true` is rejected;
- `condition_id` must be present in state (the executor obtains it from Gamma/CLOB);
- on-chain redeem was not executed in the development sandbox (no RPC/key);
- `pending_fill` recovery does not guess whether an order filled; query the CLOB order and update state.

## 8. Risk controls

`config.yaml`:

```yaml
risk:
  stake_usdc: 10.0
  starting_balance_usdc: 1000.0
  max_concurrent_positions: 2
  daily_max_loss_usdc: 30.0
  max_trades_per_day: 40
```

The runtime checks:

- active positions < `max_concurrent_positions`;
- daily PnL > `-daily_max_loss_usdc`;
- balance > 0;
- no existing position for the same market.

`max_trades_per_day` is available in config for the executor layer; the current runtime's primary hard stops are concurrency and daily loss.

## 9. WebSocket/staleness

- Polymarket: `wss://ws-subscriptions-clob.polymarket.com/ws/market`
- Binance: `wss://stream.binance.com:9443/ws/btcusdt@bookTicker`
- Each book stores a **local receive time**.
- The strategy rejects books older than `max_quote_staleness_s` (2s).
- Both feeds reconnect with exponential backoff; a WS failure does not kill the process.

## 10. Operations

Logs:

```text
logs/v7.log
```

Run under systemd:

```ini
[Unit]
Description=Polymarket V7 sniper
After=network-online.target

[Service]
WorkingDirectory=/opt/polymarket-sniper
EnvironmentFile=/opt/polymarket-sniper/.env
ExecStart=/opt/polymarket-sniper/.venv/bin/python main.py --config config.yaml
Restart=always
RestartSec=5

[Install]
WantedBy=multi-user.target
```

## 11. Safety checklist

- [ ] paper mode for at least 24h
- [ ] state recovery verified by killing the process mid-position
- [ ] one tiny live trade end-to-end
- [ ] Redeem transaction confirmed and USDC received
- [ ] `live_trading_confirm.txt` only on the live host
- [ ] no `.env` or key in git
- [ ] `max_entry_price` ≤ 0.95
- [ ] daily loss limit funded

## 12. Disclaimer

No warranty. This is not financial advice. Prediction markets can resolve against you and liquidity/execution can differ from backtests.
