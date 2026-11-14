
#!/usr/bin/env python3
"""Pure-rule V6 strategy with a hard data-readiness gate.

Rules:
  entry anywhere in the final entry_t_minus_s seconds (default 240)
  follow the Up-token mid move since the open, |move| >= 0.20
  oriented top-10 book imbalance >= 0.30
  |spot move since the open| >= 2.0 bps
  max entry ask <= 0.95

No machine learning.  There is no bypass: if any required
spot/book field is missing, non-finite, or stale, evaluate() returns None.
"""
from __future__ import annotations

import json
import logging
import math
import urllib.request
from dataclasses import dataclass
from typing import Any

from core.feed import Market

LOG = logging.getLogger("v7.strategy")

EPS = 1e-9
BookLike = Any


def _valid_ask(ask: float | None) -> float | None:
    """Strict validation: reject None, NaN, inf, <=0, or >1 (binary payoff)."""
    if ask is None:
        return None
    try:
        value = float(ask)
    except Exception:
        return None
    if not math.isfinite(value) or value <= 0 or value > 1.0:
        return None
    return value


def _valid_bid(bid: float | None) -> float | None:
    """Strict bid validation; zero is a legal bid, None/NA is not."""
    if bid is None:
        return None
    try:
        value = float(bid)
    except Exception:
        return None
    if not math.isfinite(value) or value < 0 or value > 1.0:
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
    hard_max_quote_staleness_s: float = 60.0
    max_slippage_cents: float = 2.0
    debug: bool = True

    @classmethod
    def from_config(cls, cfg: dict) -> "V6Params":
        strategy_cfg = (
            cfg.get("strategy", {}) if isinstance(cfg.get("strategy", {}), dict) else {}
        )
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
            hard_max_quote_staleness_s=float(
                strategy_cfg.get("hard_max_quote_staleness_s", 60.0)
            ),
            max_slippage_cents=float(strategy_cfg.get("max_slippage_cents", 2.0)),
            debug=bool(strategy_cfg.get("debug", True)),
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
    # Ask-side liquidity at decision time.  The paper executor must consume
    # this ladder instead of assuming unlimited size at best_ask.
    ask_size: float = 0.0
    ask_levels: tuple[tuple[float, float], ...] = ()


def _number(value: Any) -> float | None:
    """Convert Polymarket strings/numbers to a finite float."""
    if value is None:
        return None
    try:
        out = float(value)
    except (TypeError, ValueError):
        return None
    return out if math.isfinite(out) else None


def _levels(book: BookLike, side: str) -> dict[float, float]:
    """Normalize one side of a book, tolerating string keys/values."""
    raw = getattr(book, "bids" if side == "bid" else "asks", None)
    if not isinstance(raw, dict):
        return {}

    out: dict[float, float] = {}
    for price, size in raw.items():
        price_f = _number(price)
        size_f = _number(size)
        if price_f is None or size_f is None:
            continue
        if not (0.0 < price_f <= 1.0) or size_f < 0.0:
            continue
        out[price_f] = size_f
    return out


def _top_levels(book: BookLike, side: str, n: int) -> list[float]:
    """Return prices for top-n bid/ask levels (safe for empty books)."""
    levels = _levels(book, side)
    return sorted(levels, reverse=(side == "bid"))[:n]


def _volume(book: BookLike, side: str, n: int) -> float:
    levels = _levels(book, side)
    prices = _top_levels(book, side, n)
    return float(sum(levels[p] for p in prices))


def _notional(book: BookLike, side: str, n: int) -> float:
    levels = _levels(book, side)
    prices = _top_levels(book, side, n)
    return float(sum(p * levels[p] for p in prices))


def _imbalance(book: BookLike, depth: int) -> float:
    """(bid_notional - ask_notional) / (bid_notional + ask_notional + EPS)."""
    bn = _notional(book, "bid", depth)
    an = _notional(book, "ask", depth)
    return (bn - an) / (bn + an + EPS)


def _bps(spot_move: float) -> float:
    return spot_move * 1e4


def _ask_ladder(book: BookLike) -> tuple[tuple[float, float], ...]:
    """Sorted (price, size) ask levels from a Tick/book object."""
    levels = _levels(book, "ask")
    return tuple(
        sorted(
            (price, size)
            for price, size in levels.items()
            if price > 0.0 and size > 0.0
        )
    )


def _reject_bad_signal(market: Market, ask: float | None, reason: str) -> None:
    """Common guard: no bad ask can leave this function."""
    if ask is None or not math.isfinite(float(ask)) or float(ask) <= 0:
        print(
            f"[Error] 拦截坏单: market={market.slug} reason={reason} ask={ask!r}",
            flush=True,
        )
        return None


class V6Strategy:
    def __init__(self, params: V6Params):
        self.p = params
        self._last_gate_log_ms: int | None = None

    def _gate(self, reason: str, now_ms: int) -> None:
        """Throttle gate diagnostics to at most one line per five seconds.

        A changed gate reason is deliberately not printed immediately: the
        reason contains live numbers, so it changes on almost every tick and
        would defeat the throttle.
        """
        if (
            self._last_gate_log_ms is not None
            and now_ms - self._last_gate_log_ms < 5_000
        ):
            return
        self._last_gate_log_ms = now_ms
        print(f"[Gate拦截] 原因={reason}", flush=True)

    def book_imbalance(
        self,
        up_book: BookLike,
        down_book: BookLike,
        depth: int | None = None,
    ) -> float:
        """Oriented top-N imbalance, exactly the value used by evaluate()."""
        use_depth = self.p.imbalance_depth if depth is None else int(depth)
        return _imbalance(up_book, use_depth) - _imbalance(down_book, use_depth)

    def _debug(
        self,
        *,
        market: Market,
        secs: float,
        up_book: BookLike,
        down_book: BookLike,
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
        up_book: BookLike,
        down_book: BookLike,
        up_mid_open: float | None,
        spot_open: float | None,
        spot_now: float | None,
        now_ms: int,
        precomputed_move: float | None = None,
        precomputed_imbalance: float | None = None,
    ) -> Signal | None:
        secs = (market.end_ms - now_ms) / 1000.0
        if secs <= 0:
            return None
        # Entry is only allowed inside the final entry window. The main loop
        # also gates this; keep the invariant here for every caller.
        if secs > self.p.entry_t_minus_s:
            return None

        # Hard readiness gate: never compute indicators or emit a Signal
        # from a missing/not-ready order book.
        if up_book is None or down_book is None:
            self._gate("book missing", now_ms)
            return None
        if not bool(getattr(up_book, "ready", False)) or not bool(
            getattr(down_book, "ready", False)
        ):
            self._gate("up_ready/down_ready false", now_ms)
            return None

        # HARD DATA GATE: this is deliberately before every indicator
        # calculation.  A missing/NA price must never reach the move, OFI, or
        # order path; there is no escape hatch.
        spot_open_f = _number(spot_open)
        spot_now_f = _number(spot_now)
        if (
            spot_open_f is None
            or spot_now_f is None
            or spot_open_f <= 0
            or spot_now_f <= 0
        ):
            self._gate(
                f"DATA spot_open={spot_open!r} spot_now={spot_now!r}", now_ms
            )
            return None

        up_bid = _valid_bid(up_book.best_bid)
        up_ask = _valid_ask(up_book.best_ask)
        down_bid = _valid_bid(down_book.best_bid)
        down_ask = _valid_ask(down_book.best_ask)
        missing = [
            name
            for name, value in (
                ("up_bid", up_bid),
                ("up_ask", up_ask),
                ("down_bid", down_bid),
                ("down_ask", down_ask),
            )
            if value is None
        ]
        if missing:
            self._gate(
                f"DATA missing={','.join(missing)} "
                f"up={up_book.best_bid!r}/{up_book.best_ask!r} "
                f"down={down_book.best_bid!r}/{down_book.best_ask!r}",
                now_ms,
            )
            return None
        if up_bid > up_ask or down_bid > down_ask:
            self._gate(
                f"DATA crossed book up={up_bid:.4f}/{up_ask:.4f} "
                f"down={down_bid:.4f}/{down_ask:.4f}",
                now_ms,
            )
            return None

        up_mid_open_f = _number(up_mid_open)
        up_mid = _number(up_book.mid)
        down_mid = _number(down_book.mid)
        if up_mid_open_f is None or up_mid is None or down_mid is None:
            self._gate(
                f"DATA mids up_open={up_mid_open!r} up={up_book.mid!r} "
                f"down={down_book.mid!r}",
                now_ms,
            )
            return None

        # Hard 60s dead-book gate.  Any older book is not a tradeable market,
        # even if its top-of-book fields happen to parse.
        for label, book in (("up", up_book), ("down", down_book)):
            ts = int(getattr(book, "last_update_ms", 0) or 0)
            age_ms = now_ms - ts
            if ts <= 0 or age_ms > self.p.hard_max_quote_staleness_s * 1000:
                self._gate(f"HARD {label} quote stale {age_ms}ms", now_ms)
                return None

        # `move` is the Up-token mid probability change, in the same units as
        # strategy.move_threshold (0.20 means 20 cents on a 0..1 token).
        move = _number(up_mid - up_mid_open_f)
        if move is None:
            return None
        precomputed_move_f = _number(precomputed_move)
        if precomputed_move_f is not None:
            move = precomputed_move_f
        move = float(move)

        # `spot_move` is a separate fractional BTC return used by the spot
        # gate. Never substitute raw BTC price for the token-probability move:
        # 79662.005 - 79662.005 is not comparable to move_threshold=0.20.
        spot_move = float((spot_now_f - spot_open_f) / spot_open_f)
        imbalance = float(
            _imbalance(up_book, self.p.imbalance_depth)
            - _imbalance(down_book, self.p.imbalance_depth)
        )
        precomputed_imbalance_f = _number(precomputed_imbalance)
        if precomputed_imbalance_f is not None:
            imbalance = precomputed_imbalance_f
        imbalance = float(imbalance)

        # Both books must be fresh. Using max(up, down) would let a stale
        # side hide behind a fresh side.
        for label, book in (("up", up_book), ("down", down_book)):
            ts = int(getattr(book, "last_update_ms", 0) or 0)
            if ts <= 0 or now_ms - ts > self.p.max_quote_staleness_s * 1000:
                self._gate(f"{label} quote stale {now_ms - ts}ms", now_ms)
                return None

        if move >= self.p.move_threshold and imbalance >= self.p.min_book_imbalance:
            side = "UP"
            ask = up_ask
            bid = up_bid
        elif move <= -self.p.move_threshold and imbalance <= -self.p.min_book_imbalance:
            side = "DOWN"
            ask = down_ask
            bid = down_bid
        else:
            spot_now_txt = f"{spot_now_f:.3f}" if spot_now_f is not None else "NA"
            spot_open_txt = f"{spot_open_f:.3f}" if spot_open_f is not None else "NA"
            spot_bps_txt = f"{_bps(spot_move):.2f}" if spot_move is not None else "NA"
            reason = (
                f"gate move={move:.4f} (threshold {self.p.move_threshold:.4f}) "
                f"imb={imbalance:.4f} (min {self.p.min_book_imbalance:.4f}) "
                f"spot_now={spot_now_txt} spot_open={spot_open_txt} "
                f"spot_move_bps={spot_bps_txt}"
            )
            self._gate(reason, now_ms)
            return None

        ask = _valid_ask(ask)
        if ask is None or ask <= 0:
            print(
                f"[Error] 拦截坏单: market={market.slug} invalid ask after validation ask={ask!r}",
                flush=True,
            )
            self._gate("no valid ask", now_ms)
            return None

        if ask > self.p.max_entry_price:
            self._gate(f"max entry ask {ask:.4f} > {self.p.max_entry_price:.4f}", now_ms)
            return None
        if spot_move is None:
            self._gate("spot_open/spot_now unavailable", now_ms)
            return None
        if abs(spot_move) < self.p.min_spot_move:
            self._gate(
                f"spot move too small {abs(spot_move):.6f} < {self.p.min_spot_move:.6f}",
                now_ms,
            )
            return None

        spread = (ask - bid) if bid is not None else 0.0
        reason = "ready"
        selected_book = up_book if side == "UP" else down_book
        ask_levels = _ask_ladder(selected_book)
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
            ask_size=ask_levels[0][1] if ask_levels else 0.0,
            ask_levels=ask_levels,
        )


__all__ = ["V6Params", "Signal", "V6Strategy"]
