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
import urllib.request
from dataclasses import dataclass

from core.feed import Book, Market, try_refresh_book_snapshot

LOG = logging.getLogger("v7.strategy")

EPS = 1e-9


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
    debug: bool = True
    force_trade_test: bool = False

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
            debug=bool(s.get("debug", True)),
            force_trade_test=bool(s.get("force_trade_test", False)),
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

    def _debug(self, *, market: Market, secs: float, up_book: Book, down_book: Book,
               up_mid_open: float | None, up_mid: float | None,
               spot_open: float | None, spot_now: float | None,
               spot_move: float | None, imbalance: float | None,
               move: float | None, side: str, ask: float | None,
               reason: str) -> None:
        """Always emit terminal diagnostics; never suppress them behind a debug flag."""
        bid_vol5 = _volume(up_book, "bid", 5)
        ask_vol5 = _volume(up_book, "ask", 5)
        dbid_vol5 = _volume(down_book, "bid", 5)
        dask_vol5 = _volume(down_book, "ask", 5)
        bn10 = _notional(up_book, "bid", self.p.imbalance_depth)
        an10 = _notional(up_book, "ask", self.p.imbalance_depth)
        sm_bps = _bps(spot_move) if spot_move is not None else float("nan")
        im = imbalance if imbalance is not None else float("nan")

        print(
            f"[Strategy][拦截] 原因={reason}"
            f" market={market.slug}"
            f" secs={secs:.2f}"
            f" side={side}"
            f" move={move if move is not None else 'None'}"
            f" imbalance={im:.6f}"
            f" spot_open={spot_open}"
            f" spot_now={spot_now}"
            f" spot_bps={sm_bps:.3f}"
            f" bid_vol5={bid_vol5:.6f}"
            f" ask_vol5={ask_vol5:.6f}"
            f" bid_notional10={bn10:.6f}"
            f" ask_notional10={an10:.6f}"
            f" up_best_bid={up_book.best_bid}"
            f" up_best_ask={up_book.best_ask}"
            f" down_bid_vol5={dbid_vol5:.6f}"
            f" down_ask_vol5={dask_vol5:.6f}"
            f" ask={ask}",
            flush=True,
        )

        LOG.info(
            "[Debug] market=%s secs_to_close=%.2f reason=%s side=%s",
            market.slug, secs, reason, side,
        )
        LOG.info(
            "[Debug] UP book: levels=%d/%d best_bid=%s best_ask=%s "
            "bid_vol5=%.6f ask_vol5=%.6f bid_notional10=%.6f ask_notional10=%.6f "
            "imbalance10=%.6f",
            len(up_book.bids), len(up_book.asks),
            f"{up_book.best_bid:.6f}" if up_book.best_bid is not None else "None",
            f"{up_book.best_ask:.6f}" if up_book.best_ask is not None else "None",
            bid_vol5, ask_vol5, bn10, an10, im,
        )
        LOG.info(
            "[Debug] DOWN book: levels=%d/%d best_bid=%s best_ask=%s "
            "bid_vol5=%.6f ask_vol5=%.6f",
            len(down_book.bids), len(down_book.asks),
            f"{down_book.best_bid:.6f}" if down_book.best_bid is not None else "None",
            f"{down_book.best_ask:.6f}" if down_book.best_ask is not None else "None",
            dbid_vol5, dask_vol5,
        )
        LOG.info(
            "[Debug] up_mid_open=%s up_mid=%s move=%s "
            "spot_open=%s spot_now=%s spot_move=%.6f bps=%.3f ask=%s",
            f"{up_mid_open:.6f}" if up_mid_open is not None else "None",
            f"{up_mid:.6f}" if up_mid is not None else "None",
            f"{move:+.6f}" if move is not None else "None",
            f"{spot_open:.6f}" if spot_open is not None else "None",
            f"{spot_now:.6f}" if spot_now is not None else "None",
            spot_move if spot_move is not None else float("nan"),
            sm_bps, f"{ask:.6f}" if ask is not None else "None",
        )

    def evaluate(self, *, market: Market, up_book: Book, down_book: Book,
                 up_mid_open: float | None, spot_open: float | None,
                 spot_now: float | None, now_ms: int) -> Signal | None:
        p = self.p
        secs = (market.end_ms - now_ms) / 1000.0

        if not (p.entry_t_minus_s - 2.5 <= secs <= p.entry_t_minus_s + 0.5):
            _gate("NOT_T240")
            return None

        if not up_book.ready and not p.force_trade_test:
            print(f"[Strategy] trying REST fallback for up token={market.up_token} book_ready={up_book.ready}", flush=True)
            refreshed = try_refresh_book_snapshot(up_book, market=market, timeout=10.0)
            if refreshed:
                print(
                    f"[Strategy] REST fallback succeeded for {market.up_token}: "
                    f"best_bid={up_book.best_bid} best_ask={up_book.best_ask}",
                    flush=True,
                )
            else:
                self._debug(
                    market=market, secs=secs, up_book=up_book, down_book=down_book,
                    up_mid_open=up_mid_open, up_mid=None, spot_open=spot_open,
                    spot_now=spot_now, spot_move=None, imbalance=None, move=None,
                    side="", ask=None, reason="UP_BOOK_NOT_READY",
                )
                _gate("UP_BOOK_NOT_READY")
                return None

        up_mid = up_book.mid
        move = (up_mid - up_mid_open) if (up_mid is not None and up_mid_open is not None) else None
        side = "UP" if (move is not None and move >= 0) else "DOWN"
        if p.mode == "fade" and move is not None:
            side = "DOWN" if side == "UP" else "UP"

        raw_imb = _imbalance(up_book, p.imbalance_depth) if up_book.ready else None
        imb = raw_imb if (raw_imb is not None and side == "UP") else (
            -raw_imb if raw_imb is not None else None
        )
        spot_move = None
        if spot_open is not None and spot_open > 0 and spot_now is not None:
            spot_move = spot_now / (spot_open + EPS) - 1.0

        book = up_book if side == "UP" else down_book
        ask = book.best_ask
        bid = book.best_bid

        if p.force_trade_test:
            if ask is None or not (0 < ask < 1):
                alt_side = "DOWN" if side == "UP" else "UP"
                alt = down_book if alt_side == "DOWN" else up_book
                if alt.best_ask is not None and 0 < alt.best_ask < 1:
                    side, book, ask, bid = alt_side, alt, alt.best_ask, alt.best_bid
                else:
                    self._debug(
                        market=market, secs=secs, up_book=up_book, down_book=down_book,
                        up_mid_open=up_mid_open, up_mid=up_mid, spot_open=spot_open,
                        spot_now=spot_now, spot_move=spot_move, imbalance=imb,
                        move=move, side=side, ask=None, reason="FORCE_NO_ASK",
                    )
                    _gate("FORCE_NO_ASK")
                    return None
            ask = float(ask)
            bid = float(bid) if bid is not None else max(0.0, ask - 0.01)
            self._debug(
                market=market, secs=secs, up_book=up_book, down_book=down_book,
                up_mid_open=up_mid_open, up_mid=up_mid, spot_open=spot_open,
                spot_now=spot_now, spot_move=spot_move, imbalance=imb,
                move=move, side=side, ask=ask, reason="FORCE_TRADE_TEST",
            )
            LOG.warning(
                "[ForceTest] BYPASSING ALL THRESHOLDS market=%s side=%s ask=%.6f",
                market.slug, side, ask,
            )
            return Signal(
                market=market, side=side, move=move or 0.0, imbalance=imb or 0.0,
                spot_move=spot_move or 0.0, ask=ask, bid=bid,
                spread=max(0.0, ask - bid), secs_to_close=secs, reason="force_test",
            )

        if up_mid_open is None or up_mid is None:
            self._debug(
                market=market, secs=secs, up_book=up_book, down_book=down_book,
                up_mid_open=up_mid_open, up_mid=up_mid, spot_open=spot_open,
                spot_now=spot_now, spot_move=spot_move, imbalance=imb,
                move=move, side=side, ask=ask, reason="MISSING_UP_MID",
            )
            _gate("MISSING_UP_MID")
            return None
        if spot_open is None or spot_open <= 0 or spot_now is None:
            self._debug(
                market=market, secs=secs, up_book=up_book, down_book=down_book,
                up_mid_open=up_mid_open, up_mid=up_mid, spot_open=spot_open,
                spot_now=spot_now, spot_move=spot_move, imbalance=imb,
                move=move, side=side, ask=ask, reason="MISSING_SPOT",
            )
            _gate("MISSING_SPOT")
            return None
        if move is None or abs(move) < p.move_threshold:
            self._debug(
                market=market, secs=secs, up_book=up_book, down_book=down_book,
                up_mid_open=up_mid_open, up_mid=up_mid, spot_open=spot_open,
                spot_now=spot_now, spot_move=spot_move, imbalance=imb,
                move=move, side=side, ask=ask, reason="MOVE_TOO_SMALL",
            )
            _gate("MOVE_TOO_SMALL")
            return None
        if imb is None or abs(imb) < p.min_book_imbalance:
            self._debug(
                market=market, secs=secs, up_book=up_book, down_book=down_book,
                up_mid_open=up_mid_open, up_mid=up_mid, spot_open=spot_open,
                spot_now=spot_now, spot_move=spot_move, imbalance=imb,
                move=move, side=side, ask=ask, reason="IMBALANCE_TOO_SMALL",
            )
            _gate("IMBALANCE_TOO_SMALL")
            return None
        if spot_move is None or abs(spot_move) < p.min_spot_move:
            self._debug(
                market=market, secs=secs, up_book=up_book, down_book=down_book,
                up_mid_open=up_mid_open, up_mid=up_mid, spot_open=spot_open,
                spot_now=spot_now, spot_move=spot_move, imbalance=imb,
                move=move, side=side, ask=ask, reason="SPOT_TOO_SMALL",
            )
            _gate("SPOT_TOO_SMALL")
            return None

        if not down_book.ready:
            self._debug(
                market=market, secs=secs, up_book=up_book, down_book=down_book,
                up_mid_open=up_mid_open, up_mid=up_mid, spot_open=spot_open,
                spot_now=spot_now, spot_move=spot_move, imbalance=imb,
                move=move, side=side, ask=ask, reason="DOWN_BOOK_NOT_READY",
            )
            _gate("DOWN_BOOK_NOT_READY")
            return None
        if ask is None or bid is None or not (0 < ask < 1):
            self._debug(
                market=market, secs=secs, up_book=up_book, down_book=down_book,
                up_mid_open=up_mid_open, up_mid=up_mid, spot_open=spot_open,
                spot_now=spot_now, spot_move=spot_move, imbalance=imb,
                move=move, side=side, ask=ask, reason="NO_QUOTE",
            )
            _gate("NO_QUOTE")
            return None
        if now_ms - book.last_update_ms > p.max_quote_staleness_s * 1000:
            self._debug(
                market=market, secs=secs, up_book=up_book, down_book=down_book,
                up_mid_open=up_mid_open, up_mid=up_mid, spot_open=spot_open,
                spot_now=spot_now, spot_move=spot_move, imbalance=imb,
                move=move, side=side, ask=ask, reason="STALE_QUOTE",
            )
            _gate("STALE_QUOTE")
            return None
        if ask > p.max_entry_price:
            self._debug(
                market=market, secs=secs, up_book=up_book, down_book=down_book,
                up_mid_open=up_mid_open, up_mid=up_mid, spot_open=spot_open,
                spot_now=spot_now, spot_move=spot_move, imbalance=imb,
                move=move, side=side, ask=ask, reason="PRICE_TOO_HIGH",
            )
            _gate("PRICE_TOO_HIGH")
            return None

        self._debug(
            market=market, secs=secs, up_book=up_book, down_book=down_book,
            up_mid_open=up_mid_open, up_mid=up_mid, spot_open=spot_open,
            spot_now=spot_now, spot_move=spot_move, imbalance=imb,
            move=move, side=side, ask=ask, reason="READY",
        )
        return Signal(
            market=market, side=side, move=move, imbalance=imb,
            spot_move=spot_move, ask=ask, bid=bid,
            spread=max(0.0, ask - bid), secs_to_close=secs, reason="ready",
        )
