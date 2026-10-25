#!/usr/bin/env python3
"""V7 entrypoint: paper or live, T-240 V6 strategy, crash recovery, auto-redeem.

Key behavior:
  * paper/live executor is chosen from config.mode
  * strategy evaluates at T-240 (plus a small 3s tolerance)
  * force_trade_test=True bypasses the main data-readiness gate so the strategy
    can force a trade even when up_mid_open / spot_open are missing
  * analytics.maybe_print() is called once per loop and is internally throttled
  * all per-market entries are reserved in state.json before order submission

Run:
  python main.py --config config.yaml
"""
from __future__ import annotations

import argparse
import asyncio
import logging
import signal
import sys
import time
from logging.handlers import RotatingFileHandler
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parent

try:
    from dotenv import load_dotenv
    load_dotenv(ROOT / ".env")
except Exception:  # noqa: BLE001
    pass

from core.executor import Fill, build_executor
from core.feed import BinanceFeed, Market, MarketDiscovery, PolymarketFeed, now_ms
from core.redeem_engine import RedeemEngine
from core.state_manager import Position, StateManager, new_trade_id
from core.strategy import V6Params, V6Strategy
from utils.analytics import Analytics

LOG = logging.getLogger("v7.main")


def setup_logging(cfg: dict) -> None:
    log_path = Path(cfg.get("analytics", {}).get("log_path", "logs/v7.log"))
    log_path.parent.mkdir(parents=True, exist_ok=True)
    fmt = logging.Formatter("%(asctime)s %(levelname)s %(name)s %(message)s")
    root = logging.getLogger()
    root.setLevel(logging.INFO)
    fh = RotatingFileHandler(
        log_path, maxBytes=20 * 1024 * 1024, backupCount=5, encoding="utf-8"
    )
    fh.setFormatter(fmt)
    sh = logging.StreamHandler(sys.stdout)
    sh.setFormatter(fmt)
    root.addHandler(fh)
    root.addHandler(sh)


class Runtime:
    def __init__(self, cfg: dict):
        self.cfg = cfg
        self.params = V6Params.from_config(cfg)
        self.strategy = V6Strategy(self.params)
        self.mode = str(cfg.get("mode", "paper")).lower()

        self.state = StateManager(
            cfg.get("state", {}).get("path", "data/state.json"),
            lock_path=cfg.get("state", {}).get("lock_path"),
            backup_corrupt=bool(cfg.get("state", {}).get("backup_corrupt", True)),
        )

        starting = float(cfg.get("risk", {}).get("starting_balance_usdc", 1000.0))
        self.analytics = Analytics.from_config(
            cfg, starting_balance=starting, balance=starting
        )

        self.executor = build_executor(self.mode, cfg, self.params)
        self.discovery = MarketDiscovery(
            cfg.get("polymarket", {}).get(
                "gamma_url", "https://gamma-api.polymarket.com"
            )
        )
        self.pm = PolymarketFeed(
            cfg.get("polymarket", {}).get(
                "clob_ws", "wss://ws-subscriptions-clob.polymarket.com/ws/market"
            )
        )
        self.binance = BinanceFeed(
            cfg.get("binance", {}).get(
                "ws_url", "wss://stream.binance.com:9443/ws/btcusdt@bookTicker"
            )
        )

        self.market: Market | None = None
        self.up_mid_open: float | None = None
        self.spot_open: float | None = None
        self.entry_done = False
        self.paper_fill: Fill | None = None
        self.trade_id: str | None = None
        self.stop = asyncio.Event()

    # ------------------------------------------------------------------ helpers
    async def _roll_market(self, now: int) -> bool:
        slug, _ = MarketDiscovery.slug_for(now)
        if self.market and self.market.slug == slug:
            return True

        m = await asyncio.to_thread(self.discovery.fetch, slug)
        if m is None:
            return False

        self.market = m
        self.up_mid_open = None
        self.spot_open = None
        self.entry_done = False
        self.paper_fill = None
        self.trade_id = None
        await self.pm.subscribe([m.up_token, m.down_token])
        LOG.info(
            "[Market] %s start=%s end=%s condition=%s",
            m.slug,
            time.strftime("%H:%M:%S", time.gmtime(m.start_ms / 1000)),
            time.strftime("%H:%M:%S", time.gmtime(m.end_ms / 1000)),
            m.condition_id,
        )
        return True

    def reconcile_recovered_paper(self) -> None:
        """Restart in paper mode: deduct open positions' cost from paper equity."""
        if self.mode != "paper":
            return
        for p in self.state.active_positions():
            if p.status != "open":
                continue
            fee = p.shares * 0.07 * p.entry_price * (1 - p.entry_price)
            cost = p.shares * p.entry_price + fee
            self.executor.balance -= cost
            self.analytics.balance -= cost
            LOG.info(
                "[Paper] recovered open position cost=%.4f market=%s",
                cost,
                p.market_slug,
            )

    def _capture_open(self, now: int) -> None:
        if not self.market or now < self.market.start_ms:
            return
        if self.up_mid_open is None:
            b = self.pm.book(self.market.up_token)
            if b.ready and b.mid is not None:
                self.up_mid_open = b.mid
        if self.spot_open is None:
            p = self.binance.spot()
            if p:
                self.spot_open = p

    def _can_enter(self) -> tuple[bool, str]:
        active = len(self.state.active_positions())
        return self.analytics.can_trade(
            max_daily_loss=float(
                self.cfg.get("risk", {}).get("daily_max_loss_usdc", 30.0)
            ),
            active_positions=active,
            max_positions=int(
                self.cfg.get("risk", {}).get("max_concurrent_positions", 2)
            ),
            max_trades_per_day=int(
                self.cfg.get("risk", {}).get("max_trades_per_day", 40)
            ),
        )

    async def _enter(self, sig) -> None:
        if self.state.has_open_market(sig.market.slug):
            LOG.info("[Skip] already have position for %s", sig.market.slug)
            return

        ok, why = self._can_enter()
        if not ok:
            LOG.warning("[Risk] entry blocked: %s", why)
            return

        stake = float(self.cfg.get("risk", {}).get("stake_usdc", 10.0))
        trade_id = new_trade_id(sig.market.slug, sig.side)
        est_shares = stake / max(sig.ask, 1e-9)
        pos = Position(
            trade_id=trade_id,
            market_slug=sig.market.slug,
            condition_id=sig.market.condition_id,
            token_id=(
                sig.market.up_token if sig.side == "UP" else sig.market.down_token
            ),
            side=sig.side,
            entry_price=sig.ask,
            shares=est_shares,
            opened_at_ms=now_ms(),
            end_time_ms=sig.market.end_ms,
            status="pending_fill",
            neg_risk=sig.market.neg_risk,
        )
        if not self.state.add_position(pos):
            return

        try:
            fill = await asyncio.to_thread(self.executor.buy, sig, stake)
            self.state.update_position(
                trade_id,
                status="open",
                entry_price=fill.entry_price,
                shares=fill.shares,
                order_id=fill.order_id,
            )
            if self.mode == "paper":
                self.paper_fill = fill
            self.trade_id = trade_id
            self.analytics.note_position_change()
            LOG.info(
                "[Entry] %s %s ask=%.4f fill=%.4f shares=%.4f",
                sig.market.slug,
                sig.side,
                sig.ask,
                fill.entry_price,
                fill.shares,
            )
        except Exception:
            self.state.remove_position(trade_id)
            raise

    async def _settle_paper(self) -> None:
        if self.mode != "paper" or not self.market:
            return
        if now_ms() < self.market.end_ms + 15_000:
            return

        winner = await asyncio.to_thread(self.discovery.resolve, self.market.slug)
        if winner is None:
            return

        if self.paper_fill is not None and self.trade_id:
            pnl = self.executor.settle_paper(self.paper_fill, winner)
            self.analytics.record(
                pnl=pnl,
                stake=self.paper_fill.stake_usdc,
                won=self.paper_fill.side == winner,
                market=self.paper_fill.market_slug,
                side=self.paper_fill.side,
            )
            self.state.remove_position(self.trade_id)
            self.paper_fill = None
            self.trade_id = None

        # recovered paper position after restart
        for p in self.state.active_positions():
            if p.market_slug == self.market.slug and p.status == "open":
                won = p.side.upper() == winner
                payout = p.shares if won else 0.0
                cost = (
                    p.shares * p.entry_price
                    + p.shares * 0.07 * p.entry_price * (1 - p.entry_price)
                )
                pnl = payout - cost
                self.analytics.record(
                    pnl=pnl,
                    stake=cost,
                    won=won,
                    market=p.market_slug,
                    side=p.side,
                )
                self.state.remove_position(p.trade_id)

    async def step(self) -> None:
        now = now_ms()

        if self.market is None or now >= self.market.end_ms:
            if self.market is not None:
                await self._settle_paper()
                if self.mode == "paper" and self.paper_fill is not None:
                    return
            if not await self._roll_market(now):
                await asyncio.sleep(1)
                return

        assert self.market is not None
        self._capture_open(now)
        secs = (self.market.end_ms - now) / 1000.0

        # force_trade_test must bypass the main's data-readiness gate.
        ready = (
            self.params.force_trade_test
            or (self.up_mid_open is not None and self.spot_open is not None)
        )
        if not self.entry_done and ready:
            if self.params.entry_t_minus_s - 2.5 <= secs <= self.params.entry_t_minus_s + 0.5:
                self.entry_done = True
                sig = self.strategy.evaluate(
                    market=self.market,
                    up_book=self.pm.book(self.market.up_token),
                    down_book=self.pm.book(self.market.down_token),
                    up_mid_open=self.up_mid_open,
                    spot_open=self.spot_open,
                    spot_now=self.binance.spot(),
                    now_ms=now,
                )
                if sig:
                    await self._enter(sig)
                else:
                    LOG.info(
                        "[Signal] none secs=%.1f up_mid=%.4f spot=%.2f",
                        secs,
                        self.pm.book(self.market.up_token).mid or -1,
                        self.binance.spot() or -1,
                    )

        # Safe to call every loop: Analytics throttles to 60s / trade events.
        self.analytics.maybe_print()


async def amain(config_path: str) -> int:
    cfg = yaml.safe_load(Path(config_path).read_text(encoding="utf-8"))
    setup_logging(cfg)
    LOG.info("V7 starting mode=%s config=%s", cfg.get("mode"), config_path)

    runtime = Runtime(cfg)
    runtime.state.recover()
    runtime.reconcile_recovered_paper()

    if runtime.mode == "live" and cfg.get("redeem", {}).get("enabled", True):
        engine = RedeemEngine(
            runtime.state,
            interval_s=int(cfg.get("redeem", {}).get("interval_s", 60)),
            finality_buffer_s=int(
                cfg.get("redeem", {}).get("finality_buffer_s", 30)
            ),
        )
        asyncio.create_task(engine.run_forever(runtime.stop), name="redeem-engine")

    await runtime.pm.start()
    await runtime.binance.start()

    def _stop(*_):
        LOG.info("shutdown requested")
        runtime.stop.set()

    signal.signal(signal.SIGINT, _stop)
    signal.signal(signal.SIGTERM, _stop)

    while not runtime.stop.is_set():
        try:
            await runtime.step()
        except Exception:  # noqa: BLE001
            LOG.exception("main loop step failed; continuing in 5s")
            await asyncio.sleep(5)
        await asyncio.sleep(0.25)

    await runtime.pm.stop()
    await runtime.binance.stop()
    LOG.info("V7 stopped")
    return 0


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", default="config.yaml")
    args = ap.parse_args()
    return asyncio.run(amain(args.config))


if __name__ == "__main__":
    raise SystemExit(main())
