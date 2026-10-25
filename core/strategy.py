"""Pure-rule V6 strategy. No machine learning, no fitted model at runtime.

Rules:
  entry at T-240 (60s into the 5m market)
  follow the Up-token mid move since the open, |move| >= 0.20
  oriented top-10 book imbalance >= 0.30
  |spot move since the open| >= 2.0 bps
  buy the side's ask, hold to settlement
"""
from __future__ import annotations

import math
from dataclasses import dataclass

from core.feed import Book, Market


@dataclass(frozen=True)
class V6Params:
    entry_t_minus_s: float = 240.0
    move_threshold: float = 0.20
    mode: str = "follow"
    imbalance_depth: int = 10
    min_book_imbalance: float = 0.30
    min_spot_move: float = 0.00020
    max_entry_price: float = 0.95
    max_quote_staleness_s: float = 2.0
    max_slippage_cents: float = 2.0

    @classmethod
    def from_config(cls, cfg: dict) -> "V6Params":
        s = cfg.get("strategy", {})
        return cls(
            entry_t_minus_s=float(s.get("entry_t_minus_s", 240.0)),
            move_threshold=float(s.get("move_threshold", 0.20)),
            mode=str(s.get("mode", "follow")),
            imbalance_depth=int(s.get("imbalance_depth", 10)),
            min_book_imbalance=float(s.get("min_book_imbalance", 0.30)),
            min_spot_move=float(s.get("min_spot_move", 0.00020)),
            max_entry_price=float(s.get("max_entry_price", 0.95)),
            max_quote_staleness_s=float(s.get("max_quote_staleness_s", 2.0)),
            max_slippage_cents=float(s.get("max_slippage_cents", 2.0)),
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


class V6Strategy:
    def __init__(self, params: V6Params):
        self.p = params

    def evaluate(self, *, market: Market, up_book: Book, down_book: Book,
                 up_mid_open: float | None, spot_open: float | None,
                 spot_now: float | None, now_ms: int) -> Signal | None:
        p = self.p
        if up_mid_open is None or not math.isfinite(up_mid_open):
            return None
        if spot_open is None or spot_now is None or spot_open <= 0:
            return None
        secs = (market.end_ms - now_ms) / 1000.0
        if not (p.entry_t_minus_s - 2.5 <= secs <= p.entry_t_minus_s + 0.5):
            return None
        if not up_book.ready or not down_book.ready:
            return None
        if up_book.mid is None:
            return None

        move = up_book.mid - up_mid_open
        if abs(move) < p.move_threshold:
            return None
        side = "UP" if move > 0 else "DOWN"
        if p.mode == "fade":
            side = "DOWN" if side == "UP" else "UP"

        raw_imb = up_book.imbalance(levels=p.imbalance_depth)
        if raw_imb is None:
            return None
        imb = raw_imb if side == "UP" else -raw_imb
        if abs(imb) < p.min_book_imbalance:
            return None

        spot_move = spot_now / spot_open - 1.0
        if abs(spot_move) < p.min_spot_move:
            return None

        book = up_book if side == "UP" else down_book
        ask, bid = book.best_ask, book.best_bid
        if ask is None or bid is None or not (0 < ask < 1):
            return None
        if now_ms - book.last_update_ms > p.max_quote_staleness_s * 1000:
            return None
        if ask > p.max_entry_price:
            return None
        if ask - bid > p.max_slippage_cents / 100.0 + 0.05:
            return None

        return Signal(
            market=market, side=side, move=move, imbalance=imb,
            spot_move=spot_move, ask=ask, bid=bid, spread=ask - bid,
            secs_to_close=secs, reason="ready",
        )
