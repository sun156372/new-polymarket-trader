#!/usr/bin/env python3
"""V7 entrypoint: paper or live, final-240s V6 strategy, crash recovery, auto-redeem."""
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
from typing import Any

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
from core.twap_feed import ChainlinkTwapFeed
from utils.analytics import Analytics

LOG = logging.getLogger("v7.main")


def _finite_float(value: Any) -> float | None:
    """Convert config/feed values to a finite float, or None."""
    if value is None:
        return None
    try:
        out = float(value)
    except (TypeError, ValueError):
        return None
    return out if math.isfinite(out) else None


def _positive_float(value: Any) -> float | None:
    """Reject None/0/negative/inf feed prices such as a placeholder spot=0."""
    out = _finite_float(value)
    return out if out is not None and out > 0 else None


def setup_logging(cfg: dict) -> None:
    log_path = Path(cfg.get("analytics", {}).get("log_path", "logs/v7.log"))
    log_path.parent.mkdir(parents=True, exist_ok=True)
    fmt = logging.Formatter("%(asctime)s %(levelname)s %(name)s %(message)s")

    root = logging.getLogger()
    root.setLevel(logging.INFO)

    if not root.handlers:
        fh = RotatingFileHandler(
            log_path, maxBytes=20 * 1024 * 1024, backupCount=5, encoding="utf-8"
        )
        fh.setFormatter(fmt)
        root.addHandler(fh)

        sh = logging.StreamHandler(sys.stdout)
        sh.setFormatter(fmt)
        root.addHandler(sh)


class Runtime:
    def __init__(self, cfg: dict):
        self.cfg = cfg
        self.params = V6Params.from_config(cfg)
        print(f"Loaded force_trade_test={self.params.force_trade_test}", flush=True)
        print(f"Loaded strategy params={cfg.get('strategy', {})}", flush=True)
        self.strategy = V6Strategy(self.params)
        self.mode = str(cfg.get("mode", "paper")).lower()

        resolution_cfg = cfg.get("resolution", {})
        self.resolution_start_buffer_ms = int(
            (_finite_float(resolution_cfg.get("start_buffer_s")) or 15.0) * 1000
        )
        self.resolution_poll_interval_ms = int(
            (_finite_float(resolution_cfg.get("poll_interval_s")) or 5.0) * 1000
        )
        self.resolution_fallback_ms = int(
            (_finite_float(resolution_cfg.get("fallback_after_s")) or 300.0) * 1000
        )
        self.resolution_hard_deadline_ms = int(
            (_finite_float(resolution_cfg.get("hard_deadline_s")) or 900.0) * 1000
        )

        twap_cfg = cfg.get("chainlink_twap", {}) or {}
        self.twap_enabled = bool(twap_cfg.get("enabled", True))
        self.twap = ChainlinkTwapFeed(
            twap_cfg.get("ws_url", "wss://ws-live-data.polymarket.com"),
            topic=twap_cfg.get("topic", "prices.crypto.chainlink.twap"),
            symbol=twap_cfg.get("symbol", "btc/usd"),
            window_seconds=int(_finite_float(twap_cfg.get("window_seconds")) or 30),
            stale_after_s=float(_finite_float(twap_cfg.get("stale_after_s")) or 120.0),
            open_tolerance_ms=int(
                (float(_finite_float(twap_cfg.get("open_tolerance_s")) or 60.0)) * 1000
            ),
            close_tolerance_ms=int(
                (float(_finite_float(twap_cfg.get("close_tolerance_s")) or 45.0)) * 1000
            ),
        )

        self.state = StateManager(
            cfg.get("state", {}).get("path", "data/state.json"),
            lock_path=cfg.get("state", {}).get("lock_path"),
            backup_corrupt=bool(cfg.get("state", {}).get("backup_corrupt", True)),
        )

        starting = float(cfg.get("risk", {}).get("starting_balance_usdc", 1000.0))
        self.analytics = Analytics.from_config(cfg, starting_balance=starting, balance=starting)
        replayed = self.analytics.rebuild_from_history(self.state.history)
        if replayed:
            LOG.info(
                "[Analytics] restored %d settled trades balance=%.4f pnl=%.4f",
                replayed,
                self.analytics.balance,
                self.analytics.total_pnl,
            )

        self.executor = build_executor(self.mode, cfg, self.params)
        if self.mode == "paper":
            # PaperExecutor is constructed with starting cash.  Rebase it to
            # the cash implied by the persisted ledger so a restart does not
            # pretend previous settled PnL never happened.
            self.executor.balance = float(self.analytics.balance)
        self.discovery = MarketDiscovery(
            cfg.get("polymarket", {}).get("gamma_url", "https://gamma-api.polymarket.com")
        )

        self.pm = PolymarketFeed(
            cfg.get("polymarket", {}).get("clob_ws", "wss://ws-subscriptions-clob.polymarket.com/ws/market")
        )
        self.binance = BinanceFeed(
            cfg.get("binance", {}).get("ws_url", "wss://stream.binance.com:9443/ws/btcusdt@bookTicker")
        )

        self.market: Market | None = None
        self.up_mid_open: float | None = None
        self.up_mid: float | None = None
        self.spot_open: float | None = None
        self.spot_now: float | None = None
        self.spot_close: float | None = None
        self.twap_open: float | None = None
        self.twap_close: float | None = None
        self.twap_open_ts_ms: int | None = None
        self.twap_close_ts_ms: int | None = None
        self.twap_window_s: int | None = None
        self.move_distance: float | None = None
        self.spot_move: float | None = None
        self.imbalance: float | None = None
        self.indicators_ts_ms: int | None = None
        self.up_best_bid: float | None = None
        self.up_best_ask: float | None = None
        self.up_ready = False
        self.down_ready = False
        self.market_open_locked = False
        self.entry_done = False
        self.paper_fill: Fill | None = None
        self.trade_id: str | None = None
        self.paper_settle_started_ms: int | None = None
        self.paper_settle_deadline_ms: int | None = None
        self.printed_markets: set[str] = set()
        self._last_strategy_log_ms: int = 0
        self._last_strategy_skip_log_ms: int = 0
        self._last_strategy_none_log_ms: int = 0
        self.stop = asyncio.Event()
        self._startup_health_panel_printed = False

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
                self.state.cancel_trade(p.trade_id)
                cleaned += 1

        if cleaned:
            LOG.warning("[State] removed %d invalid position(s) at startup", cleaned)

    def _resolve_winner_from_ws_cache(self) -> str | None:
        if self.market is None:
            return None

        candidates = []
        if self.market.condition_id:
            candidates.append(self.market.condition_id)
        candidates.append(self.market.slug)
        candidates.extend([self.market.up_token, self.market.down_token])

        seen: set[str] = set()
        for key in candidates:
            if not key or key in seen:
                continue
            seen.add(key)
            val = self.pm.market_resolution_cache.get(str(key))
            if val in {"UP", "DOWN"}:
                return val

        for token in (self.market.up_token, self.market.down_token):
            if not token:
                continue
            val = self.pm.asset_resolution_cache.get(str(token))
            if val in {"UP", "DOWN"}:
                return val
        return None

    async def _resolve_market_winner(self) -> tuple[str | None, str]:
        """Resolve the official winner from the Gamma API only.

        The WS cache is useful for display, but it is not sufficient for cash
        settlement: the official market must be ``closed`` and its final
        ``winner`` (or outcomePrices) must be readable.  If that is not
        available, return None and keep the position PENDING instead of
        guessing from spot or from a neighbouring market.
        """
        if self.market is None:
            return None, "no-market"

        # Resolve the exact market only.  Never probe neighbouring 5m slugs:
        # a delayed or failed lookup could settle this position with the
        # previous/next market's winner.
        try:
            winner = await asyncio.to_thread(self.discovery.resolve, self.market.slug)
            if winner in {"UP", "DOWN"}:
                return winner, f"gamma:{self.market.slug}"
        except Exception as exc:  # noqa: BLE001
            LOG.warning(
                "[Resolve] gamma resolve failed market=%s err=%s",
                self.market.slug,
                exc,
            )

        return None, "gamma-unresolved"

    async def _resolve_winner_for_position(self, p: Position) -> tuple[str | None, str]:
        """Resolve a specific position's market, including recovered old markets."""
        if self.market is not None and p.market_slug == self.market.slug:
            return await self._resolve_market_winner()

        try:
            winner = await asyncio.to_thread(self.discovery.resolve, p.market_slug)
            if winner in {"UP", "DOWN"}:
                return winner, f"gamma:{p.market_slug}"
        except Exception as exc:  # noqa: BLE001
            LOG.warning(
                "[Resolve] gamma resolve failed market=%s err=%s",
                p.market_slug,
                exc,
            )
        return None, "unresolved"

    async def _roll_market(self, now: int) -> bool:
        slug, _ = MarketDiscovery.slug_for(now)
        if self.market and self.market.slug == slug:
            return True

        m = await asyncio.to_thread(self.discovery.fetch, slug)
        if m is None:
            return False

        self.market = m
        self.up_mid_open = None
        self.up_mid = None
        self.spot_open = None
        self.spot_now = None
        self.spot_close = None
        self.twap_open = None
        self.twap_close = None
        self.twap_open_ts_ms = None
        self.twap_close_ts_ms = None
        self.twap_window_s = None
        self.move_distance = None
        self.spot_move = None
        self.imbalance = None
        self.indicators_ts_ms = None
        self.up_best_bid = None
        self.up_best_ask = None
        self.up_ready = False
        self.down_ready = False
        self.market_open_locked = False
        self.entry_done = False
        self.paper_fill = None
        self.trade_id = None
        self.paper_settle_started_ms = None
        self.paper_settle_deadline_ms = None
        self._last_strategy_log_ms = 0
        self._last_strategy_skip_log_ms = 0
        self._last_strategy_none_log_ms = 0

        await self.pm.bootstrap_snapshot(m)
        await self.pm.subscribe([m.up_token, m.down_token])

        up = self.pm.get_tick(self.market.up_token, force_refresh=True, timeout=5.0)
        down = self.pm.get_tick(self.market.down_token, force_refresh=True, timeout=5.0)

        if up is not None and up.mid is not None:
            self.up_mid_open = float(up.mid)

        spot = _positive_float(self.binance.spot())
        if spot is not None:
            self.spot_open = spot
            self.spot_now = spot

        # The official settlement reference is the Chainlink TWAP at the
        # market's start, not Binance's tick.  Capture the closest RTDS tick
        # once and freeze it; the local fallback must not read a later price.
        twap_tick = self.twap.value_near(
            self.market.start_ms,
            tolerance_ms=self.twap.open_tolerance_ms,
        )
        if twap_tick is not None:
            self.twap_open = float(twap_tick.value)
            self.twap_open_ts_ms = int(twap_tick.timestamp_ms)
            self.twap_window_s = int(twap_tick.window_seconds)
            LOG.info(
                "[MarketOpen] twap_open=%.4f ts=%s window=%ss source=chainlink",
                self.twap_open,
                self.twap_open_ts_ms,
                self.twap_window_s,
            )
        else:
            LOG.warning(
                "[MarketOpen] no Chainlink TWAP near start market=%s; local fallback will remain unavailable",
                self.market.slug,
            )

        self.up_ready = bool(up and up.ready)
        self.down_ready = bool(down and down.ready)
        self.up_best_bid = up.best_bid if up else None
        self.up_best_ask = up.best_ask if up else None

        if self.up_mid_open is not None and self.spot_open is not None:
            self.market_open_locked = True
            LOG.info(
                "[MarketOpen] immediate lock market=%s up_mid_open=%.6f spot_open=%.6f",
                self.market.slug,
                self.up_mid_open,
                self.spot_open,
            )

        LOG.info(
            "[Market] %s start=%s end=%s condition=%s up_token=%s down_token=%s",
            m.slug,
            time.strftime("%H:%M:%S", time.gmtime(m.start_ms / 1000)),
            time.strftime("%H:%M:%S", time.gmtime(m.end_ms / 1000)),
            m.condition_id,
            m.up_token,
            m.down_token,
        )
        return True

    def reconcile_recovered_paper(self) -> None:
        if self.mode != "paper":
            return
        for p in self.state.active_positions():
            if p.status not in {"open", "PENDING", "PENDING_RESOLUTION"}:
                continue
            try:
                fee_coef = float(getattr(self.executor, "fee_coefficient", 0.07))
                fee = p.shares * fee_coef * p.entry_price * (1 - p.entry_price)
                cost = p.shares * p.entry_price + fee
                self.executor.balance -= cost
                self.analytics.debit_entry(cost)
                if p.status == "open":
                    self.state.ensure_open_record(p, cost_usdc=cost)
                LOG.info(
                    "[Paper] recovered open position cost=%.4f market=%s",
                    cost,
                    p.market_slug,
                )
            except Exception as e:
                print(f"[Settle Error] 清理坏账: {e}", flush=True)
                self.state.remove_position(p.trade_id)

    def _capture_open(self, now: int) -> None:
        if not self.market or now < self.market.start_ms:
            return

        if self.up_mid_open is not None and self.spot_open is not None:
            self.market_open_locked = True
            return

        if self.up_mid_open is None:
            up = self.pm.get_tick(self.market.up_token, force_refresh=True)
            if up is not None and up.mid is not None:
                self.up_mid_open = float(up.mid)

        if self.spot_open is None:
            p = _positive_float(self.binance.spot())
            if p is not None:
                self.spot_open = p
                self.spot_now = p

        if self.up_mid_open is not None and self.spot_open is not None:
            self.market_open_locked = True
            LOG.info(
                "[MarketOpen] capture market=%s up_mid_open=%.6f spot_open=%.6f",
                self.market.slug,
                self.up_mid_open,
                self.spot_open,
            )

    def _update_indicators(self, now: int) -> None:
        """Refresh mid/move/imbalance every step, not only at T-240."""
        if self.market is None:
            return

        up = self.pm.get_tick(self.market.up_token)
        down = self.pm.get_tick(self.market.down_token)
        up_mid = _finite_float(up.mid if up is not None else None)
        self.up_mid = up_mid

        if up_mid is not None and self.up_mid_open is not None:
            self.move_distance = float(up_mid - self.up_mid_open)
        else:
            self.move_distance = None

        spot_now = _positive_float(self.spot_now)
        spot_open = _positive_float(self.spot_open)
        if spot_now is not None and spot_open is not None:
            self.spot_move = float((spot_now - spot_open) / spot_open)
        else:
            self.spot_move = None

        if up is not None and down is not None and up.ready and down.ready:
            self.imbalance = float(self.strategy.book_imbalance(up, down))
        else:
            self.imbalance = None

        self.indicators_ts_ms = now

    def _capture_close(self, now: int) -> None:
        """Freeze the Chainlink TWAP at the exact market close.

        Binance spot_close is still frozen for display, but the local fallback
        must use Polymarket's official-synced Chainlink 30s TWAP.  The close
        tick is selected once, near the boundary, and persisted with the
        position so a restart cannot replace it with a later price.
        """
        if self.market is None:
            return
        # Capture at/after the close, never one second early: a pre-close
        # print is not the settlement reference and can flip UP/DOWN.
        if now < self.market.end_ms:
            return
        if now > self.market.end_ms + 90_000:
            LOG.warning(
                "[MarketClose] too late to capture close market=%s late_ms=%d",
                self.market.slug,
                now - self.market.end_ms,
            )
            return

        if self.twap_close is None:
            tick = self.twap.value_near(
                self.market.end_ms,
                tolerance_ms=self.twap.close_tolerance_ms,
            )
            if tick is not None:
                self.twap_close = float(tick.value)
                self.twap_close_ts_ms = int(tick.timestamp_ms)
                self.twap_window_s = int(tick.window_seconds)
                if self.trade_id:
                    self.state.update_position(
                        self.trade_id,
                        twap_close=self.twap_close,
                        twap_close_ts_ms=self.twap_close_ts_ms,
                        twap_window_s=self.twap_window_s,
                        twap_source="chainlink",
                    )
                LOG.info(
                    "[MarketClose] market=%s twap_close=%.4f ts=%s window=%ss",
                    self.market.slug,
                    self.twap_close,
                    self.twap_close_ts_ms,
                    self.twap_window_s,
                )
            else:
                LOG.warning(
                    "[MarketClose] no Chainlink TWAP near close market=%s; fallback unavailable",
                    self.market.slug,
                )

        # Binance close is retained only for diagnostics; it is never used for
        # settlement after this patch.
        if self.spot_close is None and now <= self.market.end_ms + 15_000:
            spot = _positive_float(self.binance.spot())
            if spot is not None:
                self.spot_close = spot
                if self.trade_id:
                    self.state.update_position(self.trade_id, spot_close=spot)
                LOG.info(
                    "[MarketClose] market=%s spot_close=%.6f (display only)",
                    self.market.slug,
                    spot,
                )

    def _local_winner_for_position(self, p: Position) -> tuple[str | None, str]:
        """Last-resort local decision after the official 5-minute timeout.

        This is deliberately NOT the normal settlement path.  It uses only the
        Chainlink 30s TWAP frozen for this exact position at its market start
        and end; it never reads Binance spot or a later live price.  If either
        TWAP is unavailable, return None and keep the position unresolved.
        """
        twap_open = _positive_float(p.twap_open)
        twap_close = _positive_float(p.twap_close)
        source = "LOCAL_FALLBACK_TIMEOUT"

        if twap_open is None or twap_close is None:
            LOG.warning(
                "[Resolve] local fallback unavailable market=%s twap_open=%s twap_close=%s",
                p.market_slug,
                twap_open,
                twap_close,
            )
            return None, "local-fallback-no-twap"
        if twap_close > twap_open:
            return "UP", source
        if twap_close < twap_open:
            return "DOWN", source
        LOG.warning(
            "[Resolve] local fallback tie market=%s twap_open=%.6f twap_close=%.6f",
            p.market_slug,
            twap_open,
            twap_close,
        )
        return None, "local-fallback-twap-tie"

    def _can_enter(self) -> tuple[bool, str]:
        active = len(self.state.active_positions())
        return self.analytics.can_trade(
            max_daily_loss=float(self.cfg.get("risk", {}).get("daily_max_loss_usdc", 30.0)),
            active_positions=active,
            max_positions=int(self.cfg.get("risk", {}).get("max_concurrent_positions", 2)),
            max_trades_per_day=int(self.cfg.get("risk", {}).get("max_trades_per_day", 40)),
        )

    async def _enter(self, sig) -> None:
        if sig is None:
            print("[Error] 拦截坏单: signal=None", flush=True)
            return
        try:
            ask_value = float(sig.ask)
        except Exception:
            ask_value = None
        if ask_value is None or not math.isfinite(ask_value) or ask_value <= 0 or ask_value > 1.0:
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
            token_id=(sig.market.up_token if sig.side == "UP" else sig.market.down_token),
            side=sig.side,
            entry_price=float(sig.ask),
            shares=est_shares,
            opened_at_ms=now_ms(),
            end_time_ms=sig.market.end_ms,
            status="pending_fill",
            neg_risk=sig.market.neg_risk,
            spot_open=_finite_float(self.spot_open),
            spot_close=None,
            twap_open=_positive_float(self.twap_open),
            twap_close=None,
            twap_open_ts_ms=self.twap_open_ts_ms,
            twap_close_ts_ms=None,
            twap_window_s=self.twap_window_s,
            twap_source="chainlink" if self.twap_open is not None else None,
        )
        if not self.state.add_position(pos):
            return

        try:
            fill = await asyncio.to_thread(self.executor.buy, sig, stake)
            if fill.entry_price is None or not math.isfinite(float(fill.entry_price)) or float(fill.entry_price) <= 0:
                raise ValueError(f"invalid fill entry price: {fill.entry_price!r}")

            shares = float(fill.shares)
            entry_price = float(fill.entry_price)
            fee = float(fill.fee_usdc)
            entry_cost = shares * entry_price + fee
            if not self.state.mark_open(
                trade_id,
                entry_price=entry_price,
                shares=shares,
                order_id=fill.order_id,
                cost_usdc=entry_cost,
            ):
                raise RuntimeError(f"state persistence failed for {trade_id}")

            if self.mode == "paper":
                # PaperExecutor.buy() already paid this from its own cash
                # balance; mirror the debit on the dashboard immediately.
                self.analytics.debit_entry(entry_cost)
                self.paper_fill = fill
                self.trade_id = trade_id
                self.paper_settle_started_ms = now_ms()
                self.paper_settle_deadline_ms = self.market.end_ms + 300_000 if self.market else None
            self.analytics.note_entry()
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
            LOG.warning("[Entry] rejected trade_id=%s error=%s", trade_id, exc)
            self.state.cancel_trade(trade_id)
            return

    async def _settle_paper(self) -> None:
        if self.mode != "paper":
            return

        now = now_ms()
        active = [
            p for p in self.state.active_positions()
            if p.status in {"open", "PENDING", "PENDING_RESOLUTION", "unresolved"}
        ]

        fee_coef = float(getattr(self.executor, "fee_coefficient", 0.07))

        # --- startup / periodic cleanup: remove stale unresolved zombies ---
        for p in list(self.state.active_positions()):
            if p.status == "unresolved":
                age_ms = now - (p.last_attempt_ms or p.opened_at_ms)
                if age_ms > 86_400_000:
                    print(
                        f"[Zombie] 清理僵尸单 trade_id={p.trade_id} market={p.market_slug} "
                        f"status={p.status} opened={p.opened_at_ms} now={now} age_ms={age_ms}",
                        flush=True,
                    )
                    LOG.warning(
                        "[Zombie] removing stale unresolved position trade_id=%s market=%s age_ms=%d",
                        p.trade_id,
                        p.market_slug,
                        age_ms,
                    )
                    self.state.remove_position(p.trade_id)

        for p in active:
            # At the exact market close, stop pretending this is an open
            # position.  PENDING is persisted before the first API poll; the
            # official result is the only thing allowed to book a WIN/LOSS.
            if p.status == "open" and now >= p.end_time_ms:
                self.state.update_position(
                    p.trade_id,
                    status="PENDING",
                    last_error="waiting-official-resolution",
                )
                LOG.info(
                    "[Paper] market closed; PENDING official resolution market=%s",
                    p.market_slug,
                )
                print(
                    f"[结算] 盘口 {p.market_slug} 结束 | "
                    f"等待 Polymarket 官方结果 | status=PENDING",
                    flush=True,
                )
                if p.trade_id == self.trade_id:
                    self._clear_paper_fill()
                p.status = "PENDING"

            # Legacy PENDING_RESOLUTION records from the previous patch are
            # treated identically, but new records use PENDING.
            if p.status == "PENDING_RESOLUTION":
                self.state.update_position(p.trade_id, status="PENDING")
                p.status = "PENDING"

            if now < p.end_time_ms + self.resolution_start_buffer_ms:
                continue

            fallback_at = p.end_time_ms + self.resolution_fallback_ms
            last_attempt = p.last_attempt_ms or 0
            winner: str | None = None
            source = "gamma-unresolved"

            # Official result first.  Never settle from local spot inside this
            # window: the local comparison is only an emergency fallback.
            if now < fallback_at:
                if now - last_attempt < self.resolution_poll_interval_ms:
                    continue
                self.state.update_position(p.trade_id, last_attempt_ms=now)
                winner, source = await self._resolve_winner_for_position(p)
                if winner is None:
                    self.state.update_position(
                        p.trade_id,
                        status="PENDING",
                        last_attempt_ms=now,
                        last_error="waiting-official-resolution",
                    )
                    continue
            else:
                # Five-minute official timeout: one last official attempt, then
                # the frozen local close may be used.  This branch is explicit
                # and loudly logged; it is not the normal settlement path.
                if now - last_attempt >= self.resolution_poll_interval_ms:
                    self.state.update_position(p.trade_id, last_attempt_ms=now)
                    winner, source = await self._resolve_winner_for_position(p)
                if winner is None:
                    local_winner, local_source = self._local_winner_for_position(p)
                    if local_winner in {"UP", "DOWN"}:
                        winner, source = local_winner, local_source
                        already = str(p.last_error or "").startswith("fallback:")
                        if not already:
                            print(
                                f"[警告] 官方结算超时，启用本地 Chainlink TWAP 兜底 "
                                f"market={p.market_slug} source={source} "
                                f"twap_open={_finite_float(p.twap_open)} "
                                f"twap_close={_finite_float(p.twap_close)} "
                                f"window={p.twap_window_s}s",
                                flush=True,
                            )
                            LOG.warning(
                                "[Paper] official resolution timeout; local fallback market=%s source=%s",
                                p.market_slug,
                                source,
                            )
                        self.state.update_position(
                            p.trade_id,
                            status="PENDING",
                            last_error=f"fallback:{source}",
                        )

            # --- forced fallback after 180 seconds past market end when Gamma is still unresolved ---
            if winner is None and now >= p.end_time_ms + 180_000:
                spot_open = _positive_float(p.spot_open)
                spot_close = _positive_float(p.spot_close)

                if spot_open is None:
                    spot_open = _positive_float(self.spot_open)
                if spot_close is None:
                    spot_close = _positive_float(self.spot_close)

                # Prefer the frozen position snapshot over live market state.
                if spot_open is not None and spot_close is not None:
                    if spot_close > spot_open:
                        winner = "UP"
                        source = "force_local_spot_fallback"
                    elif spot_close < spot_open:
                        winner = "DOWN"
                        source = "force_local_spot_fallback"
                    else:
                        winner = None
                        source = "gamma-unresolved"

                if winner is not None:
                    print(
                        f"[强制兜底] market={p.market_slug} side={p.side} "
                        f"spot_open={spot_open} spot_close={spot_close} winner={winner} "
                        f"source={source} end_time_ms={p.end_time_ms} now_ms={now}",
                        flush=True,
                    )
                    LOG.warning(
                        "[Paper] forced local spot fallback winner=%s market=%s source=%s "
                        "spot_open=%.6f spot_close=%.6f",
                        winner,
                        p.market_slug,
                        source,
                        spot_open,
                        spot_close,
                    )

                    shares = float(p.shares)
                    entry = float(p.entry_price)
                    fee = shares * fee_coef * entry * (1.0 - entry)
                    cost = shares * entry + fee
                    side = str(p.side).strip().upper()
                    if side == "YES":
                        side = "UP"
                    elif side == "NO":
                        side = "DOWN"
                    if side not in {"UP", "DOWN"}:
                        raise ValueError(f"invalid position side: {p.side!r}")

                    won = side == winner
                    payout = shares if won else 0.0
                    final_price = 1.0 if won else 0.0

                    fill_for_settle = Fill(
                        market_slug=p.market_slug,
                        side=p.side,
                        token_id=p.token_id,
                        entry_price=entry,
                        shares=shares,
                        stake_usdc=cost,
                        fee_usdc=fee,
                        order_id=p.order_id,
                        raw={
                            "paper": True,
                            "resolution_source": source,
                            "spot_open": spot_open,
                            "spot_close": spot_close,
                            "forced": True,
                        },
                    )

                    pnl = self.executor.settle_paper(fill_for_settle, winner)
                    if not all(math.isfinite(v) for v in (cost, payout, pnl)):
                        raise ValueError(
                            f"invalid settlement math: cost={cost!r} payout={payout!r} pnl={pnl!r}"
                        )

                    self.analytics.record(
                        pnl=pnl,
                        stake=cost,
                        won=won,
                        market=p.market_slug,
                        side=p.side,
                        payout=payout,
                        source=source,
                    )
                    self.state.update_position(
                        p.trade_id,
                        resolution_source=source,
                    )

                    closed = self.state.close_trade(
                        p.trade_id,
                        status="WIN" if won else "LOSS",
                        payout=payout,
                        pnl=pnl,
                        cost_usdc=cost,
                        source=source,
                        settled_at_ms=now,
                    )
                    if not closed:
                        self.state.update_position(
                            p.trade_id,
                            status="unresolved",
                            last_error="settlement accounting done but history/position missing",
                        )

                    LOG.info(
                        "[Paper] forced fallback settled market=%s side=%s winner=%s source=%s "
                        "payout=%.4f pnl=%+.4f cost=%.4f closed=%s",
                        p.market_slug,
                        p.side,
                        winner,
                        source,
                        payout,
                        pnl,
                        cost,
                        closed,
                    )
                    continue

            if winner is None:
                if now >= p.end_time_ms + self.resolution_hard_deadline_ms:
                    LOG.warning(
                        "[Paper] winner still unresolved at hard deadline market=%s source=%s; preserving position for manual review",
                        p.market_slug,
                        source,
                    )
                    print(
                        f"[结算] 盘口 {p.market_slug} 结束 | 无法确定赢家 | "
                        f"来源: {source} | 持仓: {p.side} | 保留持仓等待人工处理",
                        flush=True,
                    )
                    self.state.update_position(
                        p.trade_id,
                        status="unresolved",
                        last_error=source,
                    )
                    if p.trade_id == self.trade_id:
                        self._clear_paper_fill()
                continue

            winner = str(winner).upper()
            try:
                shares = float(p.shares)
                entry = float(p.entry_price)
                fee = shares * fee_coef * entry * (1.0 - entry)
                cost = shares * entry + fee
                side = str(p.side).strip().upper()
                if side == "YES":
                    side = "UP"
                elif side == "NO":
                    side = "DOWN"
                if side not in {"UP", "DOWN"}:
                    raise ValueError(f"invalid position side: {p.side!r}")

                won = side == winner
                payout = shares if won else 0.0
                final_price = 1.0 if won else 0.0

                fill_for_settle = Fill(
                    market_slug=p.market_slug,
                    side=p.side,
                    token_id=p.token_id,
                    entry_price=entry,
                    shares=shares,
                    stake_usdc=cost,
                    fee_usdc=fee,
                    order_id=p.order_id,
                    raw={"paper": True, "resolution_source": source},
                )

                pnl = self.executor.settle_paper(fill_for_settle, winner)
                if not all(math.isfinite(v) for v in (cost, payout, pnl)):
                    raise ValueError(
                        f"invalid settlement math: cost={cost!r} payout={payout!r} pnl={pnl!r}"
                    )

                self.analytics.record(
                    pnl=pnl,
                    stake=cost,
                    won=won,
                    market=p.market_slug,
                    side=p.side,
                    payout=payout,
                    source=source,
                )
                if str(source).lower().startswith("local"):
                    # local TWAP fallback logs can include the frozen reference
                    twap_detail = (
                        f" twap_open={_finite_float(p.twap_open)}"
                        f" twap_close={_finite_float(p.twap_close)}"
                        f" twap_window={p.twap_window_s}s"
                    )
                else:
                    twap_detail = ""

                print(
                    f"[结算] 盘口 {p.market_slug} 结束 | 最终赢家: {winner} | "
                    f"我的持仓: {held} ({p.side}) | 最终价格: {final_price:.4f} | "
                    f"来源: {source} | 份数: {shares:.4f} | 成本: {cost:.4f} | "
                    f"回收: {payout:.4f} | 盈亏: {pnl:+.4f} | "
                    f"最新余额: {self.analytics.balance:.4f} | "
                    f"executor_balance: {self.executor.balance:.4f}{twap_detail}",
                    flush=True,
                )

                held = "YES" if p.side.upper() in {"UP", "YES"} else "NO"
                closed = self.state.close_trade(
                    p.trade_id,
                    status="WIN" if won else "LOSS",
                    payout=payout,
                    pnl=pnl,
                    cost_usdc=cost,
                    source=source,
                    settled_at_ms=now,
                )
                if not closed:
                    self.state.update_position(
                        p.trade_id,
                        status="unresolved",
                        last_error="settlement accounting done but history/position missing",
                    )
                    LOG.warning(
                        "[Paper] settlement accounting done but history/position missing trade_id=%s",
                        p.trade_id,
                    )

                LOG.info(
                    "[Paper] settled market=%s side=%s winner=%s source=%s payout=%.4f pnl=%+.4f cost=%.4f closed=%s",
                    p.market_slug,
                    p.side,
                    winner,
                    source,
                    payout,
                    pnl,
                    cost,
                    closed,
                )
            except Exception as e:
                print(f"[Settle Error] 清理坏账: {e}", flush=True)
                self.state.update_position(
                    p.trade_id,
                    status="unresolved",
                    last_error=f"settle: {type(e).__name__}: {e}",
                )
                if p.trade_id == self.trade_id:
                    self._clear_paper_fill()

        if self.paper_fill is not None and self.trade_id:
            positions = {p.trade_id: p for p in self.state.active_positions()}
            pos = positions.get(self.trade_id)
            if pos is None or pos.status in {"PENDING", "PENDING_RESOLUTION"}:
                self._clear_paper_fill()

    def _compact_status_snapshot(self) -> dict[str, Any]:
        if self.market is None:
            return {
                "market_id": None,
                "binance_spot_now": None,
                "up_best_bid": None,
                "up_best_ask": None,
                "up_mid": None,
                "up_ready": False,
                "down_ready": False,
            }

        up = self.pm.get_tick(self.market.up_token)
        down = self.pm.get_tick(self.market.down_token)

        spot = self.binance.spot()
        self.spot_now = spot
        self.up_best_bid = up.best_bid if up else None
        self.up_best_ask = up.best_ask if up else None
        self.up_ready = bool(up and up.ready)
        self.down_ready = bool(down and down.ready)

        return {
            "market_id": self.market.slug,
            "binance_spot_now": spot,
            "up_best_bid": up.best_bid if up else None,
            "up_best_ask": up.best_ask if up else None,
            "up_mid": up.mid if up else None,
            "up_ready": self.up_ready,
            "down_ready": self.down_ready,
        }

    async def _emit_startup_health_panel(self) -> None:
        if self._startup_health_panel_printed:
            return
        await asyncio.sleep(5.0)
        status = self._compact_status_snapshot()
        self._startup_health_panel_printed = True

        print(
            "\n=== 启动健康检查面板 ===\n"
            f"market_id={status['market_id']}\n"
            f"binance_spot_now={status['binance_spot_now']}\n"
            f"pm_up_best_bid={status['up_best_bid']}\n"
            f"pm_up_best_ask={status['up_best_ask']}\n"
            f"pm_up_mid={status['up_mid']}\n"
            f"up_ready={status['up_ready']}\n"
            f"down_ready={status['down_ready']}\n"
            "========================\n",
            flush=True,
        )

    def _refresh_status_from_ws(self) -> None:
        if self.market is None:
            return

        up = self.pm.get_tick(self.market.up_token)
        down = self.pm.get_tick(self.market.down_token)

        spot = self.binance.spot()
        self.spot_now = spot
        self.up_best_bid = up.best_bid if up else None
        self.up_best_ask = up.best_ask if up else None
        self.up_ready = bool(up and up.ready)
        self.down_ready = bool(down and down.ready)

    def _log_market_snapshot_once(self, at_decision: bool = False) -> None:
        """One full book/indicator snapshot per market, never per update.

        Printed as soon as both books are ready (and spot_open exists).  If
        that has not happened by the decision window, force one diagnostic
        snapshot with whatever is available, so a skipped entry is still
        explainable.
        """
        if self.market is None:
            return

        market_id = self.market.slug
        if market_id in self.printed_markets:
            return

        up = self.pm.get_tick(self.market.up_token)
        down = self.pm.get_tick(self.market.down_token)
        both_ready = bool(up and down and up.ready and down.ready)
        if not at_decision:
            if not both_ready or self.spot_open is None:
                return

        def fmt(value: float | None) -> str:
            return "NA" if value is None else f"{value:.4f}"

        print(
            "[PM 盘口更新] "
            f"market_id={market_id} "
            f"up_best_bid={fmt(up.best_bid if up else None)} "
            f"up_best_ask={fmt(up.best_ask if up else None)} "
            f"down_best_bid={fmt(down.best_bid if down else None)} "
            f"down_best_ask={fmt(down.best_ask if down else None)} "
            f"up_mid={fmt(self.up_mid)} "
            f"up_mid_open={fmt(self.up_mid_open)} "
            f"spot_open={fmt(self.spot_open)} "
            f"spot_now={fmt(self.spot_now)} "
            f"move_distance={fmt(self.move_distance)} "
            f"spot_move={fmt(self.spot_move)} "
            f"imbalance={fmt(self.imbalance)} "
            f"up_ready={self.up_ready} down_ready={self.down_ready}",
            flush=True,
        )
        self.printed_markets.add(market_id)

    async def step(self) -> None:
        now = now_ms()
        self._capture_close(now)

        # Settle any paper position whose market has closed, even if a newer
        # market is already active (recovered positions included).  This must
        # run at the close, not only after the 15s buffer, so the position is
        # persisted as PENDING immediately.
        if self.mode == "paper" and any(
            p.status in {"open", "PENDING", "PENDING_RESOLUTION"}
            and now >= p.end_time_ms
            for p in self.state.active_positions()
        ):
            await self._settle_paper()

        if self.market is None or now >= self.market.end_ms:
            if self.market is not None:
                self._capture_close(now)
                await self._settle_paper()
                if self.mode == "paper" and self.paper_fill is not None:
                    await asyncio.sleep(1.0)
                    return
            if not await self._roll_market(now):
                await asyncio.sleep(1)
                return

        assert self.market is not None

        self._capture_open(now)
        self._refresh_status_from_ws()
        self._update_indicators(now)

        secs = (self.market.end_ms - now) / 1000.0
        in_entry_window = secs <= self.params.entry_t_minus_s

        self._log_market_snapshot_once(at_decision=in_entry_window)

        if not self.entry_done and in_entry_window:
            up = self.pm.get_tick(self.market.up_token, force_refresh=True)
            down = self.pm.get_tick(self.market.down_token, force_refresh=True)
            self.spot_now = self.binance.spot()
            self.up_ready = bool(up and up.ready)
            self.down_ready = bool(down and down.ready)
            self.up_best_bid = up.best_bid if up else None
            self.up_best_ask = up.best_ask if up else None
            self._update_indicators(now)

            if now - self._last_strategy_log_ms >= 5_000:
                LOG.info(
                    "[Main] strategy-check market=%s secs=%.2f up_mid=%s spot_now=%s up_ready=%s down_ready=%s up_best_bid=%s up_best_ask=%s",
                    self.market.slug,
                    secs,
                    up.mid if up else None,
                    self.spot_now,
                    self.up_ready,
                    self.down_ready,
                    self.up_best_bid,
                    self.up_best_ask,
                )
                self._last_strategy_log_ms = now

            if not self.up_ready or not self.down_ready:
                if now - self._last_strategy_skip_log_ms >= 5_000:
                    LOG.info(
                        "[Main] strategy skipped: up_ready=%s down_ready=%s",
                        self.up_ready,
                        self.down_ready,
                    )
                    self._last_strategy_skip_log_ms = now
            else:
                sig = self.strategy.evaluate(
                    market=self.market,
                    up_book=up,
                    down_book=down,
                    up_mid_open=self.up_mid_open,
                    spot_open=self.spot_open,
                    spot_now=self.spot_now,
                    now_ms=now,
                    precomputed_move=self.move_distance,
                    precomputed_imbalance=self.imbalance,
                )
                if sig:
                    self.entry_done = True
                    await self._enter(sig)
                elif now - self._last_strategy_none_log_ms >= 5_000:
                    LOG.info(
                        "[Main] strategy returned None market=%s secs=%.2f up_mid=%s spot_now=%s",
                        self.market.slug,
                        secs,
                        up.mid if up else None,
                        self.spot_now,
                    )
                    self._last_strategy_none_log_ms = now

        self.analytics.maybe_print()

    async def amain(self, config_path: str) -> int:
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
                finality_buffer_s=int(cfg.get("redeem", {}).get("finality_buffer_s", 30)),
            )
            asyncio.create_task(engine.run_forever(runtime.stop), name="redeem-engine")

        await runtime.pm.start()
        await runtime.binance.start()
        if runtime.twap_enabled:
            await runtime.twap.start()

        asyncio.create_task(runtime._emit_startup_health_panel(), name="startup-health-panel")

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
            try:
                await runtime.twap.stop()
            except Exception:
                LOG.exception("failed to stop Chainlink TWAP feed")
            LOG.info("V7 stopped")

        return 0

    @staticmethod
    async def main() -> int:
        ap = argparse.ArgumentParser()
        ap.add_argument("--config", default="config.yaml")
        args = ap.parse_args()
        try:
            cfg = yaml.safe_load(Path(args.config).read_text(encoding="utf-8"))
            if not isinstance(cfg, dict):
                raise ValueError(f"invalid config file: {args.config}")
            runtime = Runtime(cfg)
            return await runtime.amain(args.config)
        except KeyboardInterrupt:
            print("[Main] KeyboardInterrupt; exiting cleanly", flush=True)
            return 0
        except Exception as exc:
            print(f"[Main] fatal error: {exc}", flush=True)
            return 1


if __name__ == "__main__":
    raise SystemExit(asyncio.run(Runtime.main()))
