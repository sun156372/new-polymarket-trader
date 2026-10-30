#!/usr/bin/env python3
"""V7 entrypoint: paper or live, T-240 V6 strategy, crash recovery, auto-redeem."""
from __future__ import annotations

import argparse
import asyncio
import logging
import math
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
except Exception:
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
        print(f"Loaded force_trade_test={self.params.force_trade_test}", flush=True)
        print(f"Loaded strategy params={cfg.get('strategy', {})}", flush=True)
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
        self.market_open_locked = False
        self.entry_done = False
        self.paper_fill: Fill | None = None
        self.trade_id: str | None = None
        self.paper_settle_started_ms: int | None = None
        self.paper_settle_deadline_ms: int | None = None
        self.stop = asyncio.Event()

    def _clear_paper_fill(self) -> None:
        self.paper_fill = None
        self.trade_id = None
        self.paper_settle_started_ms = None
        self.paper_settle_deadline_ms = None

    def _sanitize_state_on_startup(self) -> None:
        """Startup self-check: remove invalid positions with entry_price=None/<=0."""
        cleaned = 0
        for p in self.state.positions():
            entry = p.entry_price
            valid = False
            try:
                val = float(entry)
                valid = math.isfinite(val) and val > 0
            except Exception:
                valid = False

            if not valid:
                print(
                    f"[State] 警告: 清理坏单 trade_id={p.trade_id} market={p.market_slug} "
                    f"entry_price={entry!r} status={p.status}",
                    flush=True,
                )
                self.state.remove_position(p.trade_id)
                cleaned += 1

        if cleaned:
            LOG.warning("[State] removed %d invalid position(s) at startup", cleaned)

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
        self.market_open_locked = False
        self.entry_done = False
        self.paper_fill = None
        self.trade_id = None
        self.paper_settle_started_ms = None
        self.paper_settle_deadline_ms = None

        up = self.pm.get_tick(self.market.up_token, force_refresh=True, timeout=5.0)
        if up is not None and up.mid is not None:
            self.up_mid_open = float(up.mid)
        spot = self.binance.spot()
        if spot is not None:
            self.spot_open = float(spot)

        if self.up_mid_open is not None and self.spot_open is not None:
            self.market_open_locked = True
            LOG.info(
                "[MarketOpen] immediate lock market=%s up_mid_open=%.6f spot_open=%.6f",
                self.market.slug,
                self.up_mid_open,
                self.spot_open,
            )

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
        if self.mode != "paper":
            return
        for p in self.state.active_positions():
            if p.status != "open":
                continue
            try:
                fee = p.shares * 0.07 * p.entry_price * (1 - p.entry_price)
                cost = p.shares * p.entry_price + fee
                self.executor.balance -= cost
                self.analytics.balance -= cost
                LOG.info(
                    "[Paper] recovered open position cost=%.4f market=%s",
                    cost,
                    p.market_slug,
                )
            except Exception as e:
                print(f"[Settle Error] 清理坏账: {e}", flush=True)
                self.state.remove_position(p.trade_id)

    def _capture_open(self, now: int, force: bool = False) -> None:
        if not self.market or now < self.market.start_ms:
            return

        if self.up_mid_open is not None and self.spot_open is not None:
            self.market_open_locked = True
            return

        if not force:
            secs = (self.market.end_ms - now) / 1000.0
            if secs > 300.0:
                return

        if self.up_mid_open is None:
            up = self.pm.get_tick(self.market.up_token, force_refresh=True)
            if up is not None and up.mid is not None:
                self.up_mid_open = float(up.mid)

        if self.spot_open is None:
            p = self.binance.spot()
            if p is not None:
                self.spot_open = float(p)

        if self.up_mid_open is not None and self.spot_open is not None:
            self.market_open_locked = True
            LOG.info(
                "[MarketOpen] capture market=%s up_mid_open=%.6f spot_open=%.6f",
                self.market.slug,
                self.up_mid_open,
                self.spot_open,
            )

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
        try:
            ask_value = float(sig.ask)
        except Exception:
            ask_value = None
        if sig is None or sig.ask is None or ask_value is None or not math.isfinite(ask_value) or ask_value <= 0:
            print(
                f"[Error] 拦截坏单: market={getattr(sig.market, 'slug', 'unknown')} "
                f"ask={getattr(sig, 'ask', None)!r} reason=invalid signal.ask",
                flush=True,
            )
            return

        if self.state.has_open_market(sig.market.slug):
            LOG.info("[Skip] already have position for %s", sig.market.slug)
            return

        ok, why = self._can_enter()
        if not ok:
            LOG.warning("[Risk] entry blocked: %s", why)
            return

        stake = float(self.cfg.get("risk", {}).get("stake_usdc", 10.0))
        trade_id = new_trade_id(sig.market.slug, sig.side)
        est_shares = stake / max(float(sig.ask), 1e-9)
        pos = Position(
            trade_id=trade_id,
            market_slug=sig.market.slug,
            condition_id=sig.market.condition_id,
            token_id=(
                sig.market.up_token if sig.side == "UP" else sig.market.down_token
            ),
            side=sig.side,
            entry_price=float(sig.ask),
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
            if fill.entry_price is None or not math.isfinite(float(fill.entry_price)) or float(fill.entry_price) <= 0:
                raise ValueError(f"invalid fill entry price: {fill.entry_price!r}")

            self.state.update_position(
                trade_id,
                status="open",
                entry_price=float(fill.entry_price),
                shares=float(fill.shares),
                order_id=fill.order_id,
            )
            if self.mode == "paper":
                self.paper_fill = fill
                self.trade_id = trade_id
                self.paper_settle_started_ms = now_ms()
                self.paper_settle_deadline_ms = self.market.end_ms + 300_000 if self.market else None
            self.analytics.note_position_change()
            print(
                f"[{'Paper' if self.mode == 'paper' else 'Live'}] 买入"
                f" market={sig.market.slug}"
                f" side={sig.side}"
                f" ask={float(sig.ask):.4f}"
                f" fill={float(fill.entry_price):.4f}"
                f" shares={float(fill.shares):.4f}",
                flush=True,
            )
            LOG.info(
                "[Entry] %s %s ask=%.4f fill=%.4f shares=%.4f",
                sig.market.slug,
                sig.side,
                float(sig.ask),
                float(fill.entry_price),
                float(fill.shares),
            )
        except Exception as exc:
            print(f"[Error] 下单失败，清理坏单 trade_id={trade_id} error={exc}", flush=True)
            self.state.remove_position(trade_id)
            raise

    async def _settle_paper(self) -> None:
        if self.mode != "paper" or not self.market:
            return

        now = now_ms()
        if now < self.market.end_ms + 15_000:
            return

        if self.paper_fill is not None and self.paper_settle_deadline_ms is None:
            self.paper_settle_deadline_ms = self.market.end_ms + 300_000

        if self.paper_fill is not None and self.paper_settle_deadline_ms is not None:
            if now > self.paper_settle_deadline_ms:
                LOG.warning(
                    "[Paper] settlement timeout market=%s; clearing stale pending fill after deadline=%s",
                    self.market.slug,
                    self.paper_settle_deadline_ms,
                )
                if self.trade_id:
                    self.state.remove_position(self.trade_id)
                self._clear_paper_fill()
                return

        winner = await asyncio.to_thread(self.discovery.resolve, self.market.slug)
        if winner is None:
            if self.paper_settle_deadline_ms is not None and now > self.paper_settle_deadline_ms:
                LOG.warning(
                    "[Paper] winner still unresolved after deadline market=%s deadline=%s",
                    self.market.slug,
                    self.paper_settle_deadline_ms,
                )
                if self.trade_id:
                    self.state.remove_position(self.trade_id)
                self._clear_paper_fill()
            return

        if self.paper_fill is not None and self.trade_id:
            try:
                pnl = self.executor.settle_paper(self.paper_fill, winner)
                self.analytics.record(
                    pnl=pnl,
                    stake=self.paper_fill.stake_usdc,
                    won=self.paper_fill.side == winner,
                    market=self.paper_fill.market_slug,
                    side=self.paper_fill.side,
                )
                self.state.remove_position(self.trade_id)
                self._clear_paper_fill()
            except Exception as e:
                print(f"[Settle Error] 清理坏账: {e}", flush=True)
                if self.trade_id:
                    self.state.remove_position(self.trade_id)
                self._clear_paper_fill()

        for p in list(self.state.active_positions()):
            if p.market_slug == self.market.slug and p.status == "open":
                try:
                    won = p.side.upper() == winner
                    payout = p.shares if won else 0.0
                    cost = p.shares * p.entry_price + (p.shares * 0.07 * p.entry_price * (1 - p.entry_price))
                    pnl = payout - cost
                    if not math.isfinite(pnl):
                        raise ValueError(f"invalid pnl: {pnl!r}")
                    self.analytics.record(
                        pnl=pnl,
                        stake=cost,
                        won=won,
                        market=p.market_slug,
                        side=p.side,
                    )
                    self.state.remove_position(p.trade_id)
                except Exception as e:
                    print(f"[Settle Error] 清理坏账: {e}", flush=True)
                    self.state.remove_position(p.trade_id)

    async def step(self) -> None:
        now = now_ms()

        if self.market is None or now >= self.market.end_ms:
            if self.market is not None:
                await self._settle_paper()
                if self.mode == "paper" and self.paper_fill is not None:
                    await asyncio.sleep(1.0)
                    return
            if not await self._roll_market(now):
                await asyncio.sleep(1)
                return

        assert self.market is not None

        self._capture_open(now)

        secs = (self.market.end_ms - now) / 1000.0
        in_window = (
            self.params.entry_t_minus_s - 2.5
            <= secs
            <= self.params.entry_t_minus_s + 0.5
        )
        if not self.entry_done and in_window:
            up = self.pm.get_tick(self.market.up_token, force_refresh=True)
            down = self.pm.get_tick(self.market.down_token, force_refresh=True)
            print(
                "[Main] 正在执行策略判断..."
                f" market={self.market.slug}"
                f" secs={secs:.2f}"
                f" up_mid_open={self.up_mid_open}"
                f" spot_open={self.spot_open}"
                f" up_mid={up.mid if up else None}"
                f" spot_now={self.binance.spot()}"
                f" up_ready={up.ready if up else False}"
                f" down_ready={down.ready if down else False}"
                f" up_best_bid={up.best_bid if up else None}"
                f" up_best_ask={up.best_ask if up else None}"
                f" force={self.params.force_trade_test}",
                flush=True,
            )

            sig = self.strategy.evaluate(
                market=self.market,
                up_book=up,
                down_book=down,
                up_mid_open=self.up_mid_open,
                spot_open=self.spot_open,
                spot_now=self.binance.spot(),
                now_ms=now,
            )
            if sig:
                self.entry_done = True
                await self._enter(sig)
            else:
                print(
                    f"[Main] strategy returned None market={self.market.slug}"
                    f" secs={secs:.2f} up_mid={up.mid if up else None}"
                    f" spot={self.binance.spot()}",
                    flush=True,
                )

        self.analytics.maybe_print()


def _build_runtime(config_path: str) -> Runtime:
    cfg = yaml.safe_load(Path(config_path).read_text(encoding="utf-8"))
    if not isinstance(cfg, dict):
        raise ValueError(f"invalid config file: {config_path}")
    setup_logging(cfg)
    runtime = Runtime(cfg)
    return runtime


async def amain(config_path: str) -> int:
    cfg = yaml.safe_load(Path(config_path).read_text(encoding="utf-8"))
    if not isinstance(cfg, dict):
        raise ValueError(f"invalid config file: {config_path}")

    setup_logging(cfg)
    LOG.info("V7 starting mode=%s config=%s", cfg.get("mode"), config_path)

    runtime = Runtime(cfg)
    params = runtime.params
    print(f"Loaded force_trade_test={params.force_trade_test}", flush=True)
    print(f"Loaded full config strategy={cfg.get('strategy', {})}", flush=True)

    runtime.state.recover()
    runtime._sanitize_state_on_startup()
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

    try:
        while not runtime.stop.is_set():
            try:
                await runtime.step()
            except KeyboardInterrupt:
                LOG.warning("KeyboardInterrupt received; stopping main loop")
                runtime.stop.set()
                break
            except Exception as exc:
                LOG.exception("main loop step failed; continuing in 5s: %s", exc)
                await asyncio.sleep(5)
            await asyncio.sleep(0.25)
    finally:
        try:
            await runtime.pm.stop()
        except Exception:
            LOG.exception("failed to stop PM feed")
        try:
            await runtime.binance.stop()
        except Exception:
            LOG.exception("failed to stop Binance feed")
        LOG.info("V7 stopped")

    return 0


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", default="config.yaml")
    args = ap.parse_args()
    try:
        return asyncio.run(amain(args.config))
    except KeyboardInterrupt:
        print("[Main] KeyboardInterrupt; exiting cleanly", flush=True)
        return 0
    except Exception as exc:
        print(f"[Main] fatal error: {exc}", flush=True)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())


















































































































































