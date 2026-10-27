#!/usr/bin/env python3
"""Pure-rule V6 strategy with T-240 diagnostics and a force-trade test mode.

Rules:
  entry at T-240 (60 seconds into the 5m market)
  follow the Up-token mid move since the open, |move| >= 0.20
  oriented top-10 book imbalance >= 0.30
  |spot move since the open| >= 2.0 bps
  max entry ask <= 0.95

No machine learning.

force_trade_test=True:
  at T-240, bypass move/imbalance/spot/max-entry/staleness gates and return a
  signal for the best available side. It still requires *some* valid ask; no
  quote means no order. This is intentionally loud and should only be used to
  exercise open -> settle -> redeem.
"""
from __future__ import annotations

import json
import logging
import math
import urllib.request
from dataclasses import dataclass

from core.feed import Book, Market, try_refresh_book_snapshot

LOG = logging.getLogger("v7.strategy")

EPS = 1e-9


def _valid_ask(ask: float | None) -> float | None:
    try:
        value = float(ask)
    except Exception:
        return None
    if not math.isfinite(value) or value <= 0:
        return None
    return value


@dataclass(frozen=True)
class V6Params:
    entry_t_minus_s: float = 240.0
    move_threshold: float = 0.20
    mode: str = "follow"
    imbalance_depth: int = 10
    min_book_imbalance: float = 0.30
    min_spot_move: float = 0.00020
    max_entry_price: float = 0.95
    max_quote_staleness_s: float = 5.0
    max_slippage_cents: float = 2.0
    debug: bool = True
    force_trade_test: bool = False

    @classmethod
    def from_config(cls, cfg: dict) -> "V6Params":
        strategy_cfg = (
            cfg.get("strategy", {}) if isinstance(cfg.get("strategy", {}), dict) else {}
        )
        value = strategy_cfg.get("force_trade_test", cfg.get("force_trade_test", False))
        return cls(
            entry_t_minus_s=float(strategy_cfg.get("entry_t_minus_s", 240.0)),
            move_threshold=float(strategy_cfg.get("move_threshold", 0.20)),
            mode=str(strategy_cfg.get("mode", "follow")),
            imbalance_depth=int(strategy_cfg.get("imbalance_depth", 10)),
            min_book_imbalance=float(strategy_cfg.get("min_book_imbalance", 0.30)),
            min_spot_move=float(strategy_cfg.get("min_spot_move", 0.00020)),
            max_entry_price=float(strategy_cfg.get("max_entry_price", 0.95)),
            max_quote_staleness_s=float(
                strategy_cfg.get("max_quote_staleness_s", 5.0)
            ),
            max_slippage_cents=float(strategy_cfg.get("max_slippage_cents", 2.0)),
            debug=bool(strategy_cfg.get("debug", True)),
            force_trade_test=bool(value),
        )


@dataclass
class Signal:
    market: Market
    side: str
    move: float
    imbalance: float
    spot_move: float
    ask: float
    bid: float
    spread: float
    secs_to_close: float
    reason: str = "ready"


def _top_levels(book: Book, side: str, n: int) -> list[float]:
    """Return prices for top-n bid/ask levels (safe for empty books)."""
    if side == "bid":
        return sorted(book.bids, reverse=True)[:n]
    return sorted(book.asks)[:n]


def _volume(book: Book, side: str, n: int) -> float:
    prices = _top_levels(book, side, n)
    src = book.bids if side == "bid" else book.asks
    return float(sum(src[p] for p in prices))


def _notional(book: Book, side: str, n: int) -> float:
    prices = _top_levels(book, side, n)
    src = book.bids if side == "bid" else book.asks
    return float(sum(p * src[p] for p in prices))


def _imbalance(book: Book, depth: int) -> float:
    """(bid_notional - ask_notional) / (bid_notional + ask_notional + EPS)."""
    bn = _notional(book, "bid", depth)
    an = _notional(book, "ask", depth)
    return (bn - an) / (bn + an + EPS)


def _bps(spot_move: float) -> float:
    return spot_move * 1e4


def _gate(reason: str) -> None:
    """Terminal-visible gate result; independent of logging config."""
    print(f"[Gate拦截] 原因={reason}", flush=True)


class V6Strategy:
    def __init__(self, params: V6Params):
        self.p = params

    def _debug(
        self,
        *,
        market: Market,
        secs: float,
        up_book: Book,
        down_book: Book,
        up_mid_open: float | None,
        up_mid: float | None,
        spot_open: float | None,
        spot_now: float | None,
        spot_move: float | None,
        imbalance: float | None,
        move: float | None,
        side: str,
        ask: float | None,
        reason: str,
    ) -> None:
        """Always emit terminal diagnostics; never suppress them behind a debug flag."""
        bid_vol5 = _volume(up_book, "bid", 5)
        ask_vol5 = _volume(up_book, "ask", 5)
        dbid_vol5 = _volume(down_book, "bid", 5)
        dask_vol5 = _volume(down_book, "ask", 5)
        bn10 = _notional(up_book, "bid", self.p.imbalance_depth)
        an10 = _notional(up_book, "ask", self.p.imbalance_depth)
        sm_bps = _bps(spot_move) if spot_move is not None else float("nan")
        im = imbalance if imbalance is not None else float("nan")
        mv = move if move is not None else float("nan")
        print(
            f"[Strategy][诊断] market={market.slug} secs={secs:.2f} "
            f"side={side} reason={reason} up_mid_open={up_mid_open} up_mid={up_mid} "
            f"move={mv:.6f} imbalance={im:.4f} spot_move_bps={sm_bps:.2f} "
            f"up_bid_vol5={bid_vol5:.2f} up_ask_vol5={ask_vol5:.2f} "
            f"down_bid_vol5={dbid_vol5:.2f} down_ask_vol5={dask_vol5:.2f} "
            f"up_bid_notional={bn10:.2f} up_ask_notional={an10:.2f} "
            f"ask={ask} spot_open={spot_open} spot_now={spot_now}",
            flush=True,
        )

    def evaluate(
        self,
        *,
        market: Market,
        up_book: Book,
        down_book: Book,
        up_mid_open: float | None,
        spot_open: float | None,
        spot_now: float | None,
        now_ms: int,
    ) -> Signal | None:
        secs = (market.end_ms - now_ms) / 1000.0
        if secs <= 0:
            return None

        # 强制测试模式：优先，且直接绕过 quote stale
        if self.p.force_trade_test:
            up_ask = up_book.best_ask
            down_ask = down_book.best_ask
            if up_ask is None and down_ask is None:
                print(f"[Error] 价格无效 market={market.slug} ask=None both sides", flush=True)
                return None
            if up_ask is not None and down_ask is not None:
                side = "UP" if up_ask <= down_ask else "DOWN"
                ask = up_ask if side == "UP" else down_ask
            elif up_ask is not None:
                side = "UP"
                ask = up_ask
            else:
                side = "DOWN"
                ask = down_ask

            ask = _valid_ask(ask)
            if ask is None:
                print(f"[Error] 价格无效 market={market.slug} ask={ask}", flush=True)
                return None

            bid = up_book.best_bid if side == "UP" else down_book.best_bid
            spread = (ask - bid) if bid is not None else 0.0
            print(
                f"[Strategy][ForceTrade] market={market.slug} side={side} "
                f"ask={ask} bid={bid} spread={spread} secs={secs:.2f}",
                flush=True,
            )
            return Signal(
                market=market,
                side=side,
                move=0.0,
                imbalance=0.0,
                spot_move=0.0,
                ask=float(ask),
                bid=float(bid) if bid is not None else 0.0,
                spread=float(spread),
                secs_to_close=secs,
                reason="force_trade_test",
            )

        if up_mid_open is None:
            return None
        up_mid = up_book.mid
        down_mid = down_book.mid
        if up_mid is None or down_mid is None:
            return None

        move = up_mid - up_mid_open
        spot_move = (
            None
            if spot_open is None or spot_now is None
            else float(spot_now - spot_open)
        )
        imbalance = _imbalance(up_book, self.p.imbalance_depth) - _imbalance(
            down_book, self.p.imbalance_depth
        )

        stale_ms = max(up_book.last_update_ms, down_book.last_update_ms)
        if stale_ms and now_ms - stale_ms > self.p.max_quote_staleness_s * 1000:
            _gate("quote stale")
            return None

        ask_up = up_book.best_ask
        ask_down = down_book.best_ask
        if ask_up is None and ask_down is None:
            print(f"[Error] 价格无效 market={market.slug} ask=None both sides", flush=True)
            return None

        if move >= self.p.move_threshold and imbalance >= self.p.min_book_imbalance:
            side = "UP"
            ask = ask_up
            bid = up_book.best_bid
        elif move <= -self.p.move_threshold and imbalance <= -self.p.min_book_imbalance:
            side = "DOWN"
            ask = ask_down
            bid = down_book.best_bid
        else:
            reason = (
                f"gate move={move:.4f} imb={imbalance:.4f} "
                f"spot={spot_move if spot_move is not None else 'NA'}"
            )
            _gate(reason)
            return None

        ask = _valid_ask(ask)
        if ask is None:
            print(f"[Error] 价格无效 market={market.slug} ask={ask}", flush=True)
            _gate("no valid ask")
            return None

        if ask > self.p.max_entry_price:
            _gate(f"max entry ask {ask:.4f} > {self.p.max_entry_price:.4f}")
            return None
        if spot_move is None:
            _gate("spot_open/spot_now unavailable")
            return None
        if abs(spot_move) < self.p.min_spot_move:
            _gate(f"spot move too small {abs(spot_move):.6f} < {self.p.min_spot_move:.6f}")
            return None

        spread = (ask - bid) if bid is not None else 0.0
        reason = "ready"
        if self.p.debug:
            self._debug(
                market=market,
                secs=secs,
                up_book=up_book,
                down_book=down_book,
                up_mid_open=up_mid_open,
                up_mid=up_mid,
                spot_open=spot_open,
                spot_now=spot_now,
                spot_move=spot_move,
                imbalance=imbalance,
                move=move,
                side=side,
                ask=ask,
                reason=reason,
            )

        return Signal(
            market=market,
            side=side,
            move=float(move),
            imbalance=float(imbalance),
            spot_move=float(spot_move),
            ask=float(ask),
            bid=float(bid) if bid is not None else 0.0,
            spread=float(spread),
            secs_to_close=float(secs),
            reason=reason,
        )


__all__ = ["V6Params", "Signal", "V6Strategy"]
