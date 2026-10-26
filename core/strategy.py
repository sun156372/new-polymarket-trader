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
    def from_config(cls, cfg: dict) -> \"V6Params\":\n        s = cfg.get(\"strategy\", {})\n        return cls(\n            entry_t_minus_s=float(s.get(\"entry_t_minus_s\", 240.0)),\n            move_threshold=float(s.get(\"move_threshold\", 0.20)),\n            mode=str(s.get(\"mode\", \"follow\")),\n            imbalance_depth=int(s.get(\"imbalance_depth\", 10)),\n            min_book_imbalance=float(s.get(\"min_book_imbalance\", 0.30)),\n            min_spot_move=float(s.get(\"min_spot_move\", 0.00020)),\n            max_entry_price=float(s.get(\"max_entry_price\", 0.95)),\n            max_quote_staleness_s=float(s.get(\"max_quote_staleness_s\", 2.0)),\n            max_slippage_cents=float(s.get(\"max_slippage_cents\", 2.0)),\n            debug=bool(s.get(\"debug\", True)),\n            force_trade_test=bool(s.get(\"force_trade_test\", False)),\n        )


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
    reason: str = \"ready"


def _top_levels(book: Book, side: str, n: int) -> list[float]:
    """Return prices for top-n bid/ask levels (safe for empty books)."""
    if side == \"bid\":\n        return sorted(book.bids, reverse=True)[:n]
    return sorted(book.asks)[:n]


def _volume(book: Book, side: str, n: int) -> float:
    prices = _top_levels(book, side, n)
    src = book.bids if side == \"bid\" else book.asks
    return float(sum(src[p] for p in prices))


def _notional(book: Book, side: str, n: int) -> float:
    prices = _top_levels(book, side, n)
    src = book.bids if side == \"bid\" else book.asks
    return float(sum(p * src[p] for p in prices))


def _imbalance(book: Book, depth: int) -> float:
    """(bid_notional - ask_notional) / (bid_notional + ask_notional + EPS)."""
    bn = _notional(book, \"bid\", depth)
    an = _notional(book, \"ask\", depth)
    return (bn - an) / (bn + an + EPS)


def _bps(spot_move: float) -> float:
    return spot_move * 1e4


def _gate(reason: str) -> None:
    """Terminal-visible gate result; independent of logging config."""
    print(f\"[Gate拦截] 原因={reason}\", flush=True)


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
        bid_vol5 = _volume(up_book, \"bid\", 5)
        ask_vol5 = _volume(up_book, \"ask\", 5)
        dbid_vol5 = _volume(down_book, \"bid\", 5)
        dask_vol5 = _volume(down_book, \"ask\", 5)
        bn10 = _notional(up_book, \"bid\", self.p.imbalance_depth)
        an10 = _notional(up_book, \"ask\", self.p.imbalance_depth)
        sm_bps = _bps(spot_move) if spot_move is not None else float(\"nan\")\n        im = imbalance if imbalance is not None else float(\"nan\")\n\n        print(\n            f\"[Strategy][拦截] 原因={reason}\"\n            f\" market={market.slug}\"\n            f\" secs={secs:.2f}\"\n            f\" side={side}\"\n            f\" move={move if move is not None else 'None'}\"\n            f\" imbalance={im:.6f}\"\n            f\" spot_open={spot_open}\"\n            f\" spot_now={spot_now}\"\n            f\" spot_bps={sm_bps:.3f}\"\n            f\" bid_vol5={bid_vol5:.6f}\"\n            f\" ask_vol5={ask_vol5:.6f}\"\n            f\" bid_notional10={bn10:.6f}\"\n            f\" ask_notional10={an10:.6f}\"\n            f\" up_best_bid={up_book.best_bid}\"\n            f\" up_best_ask={up_book.best_ask}\"\n            f\" down_bid_vol5={dbid_vol5:.6f}\"\n            f\" down_ask_vol5={dask_vol5:.6f}\"\n            f\" ask={ask}\",\n            flush=True,\n        )\n\n        LOG.info(\n            \"[Debug] market=%s secs_to_close=%.2f reason=%s side=%s\",\n            market.slug, secs, reason, side,\n        )\n        LOG.info(\n            \"[Debug] UP book: levels=%d/%d best_bid=%s best_ask=%s \"\n            \"bid_vol5=%.6f ask_vol5=%.6f bid_notional10=%.6f ask_notional10=%.6f \"\n            \"imbalance10=%.6f\",\n            len(up_book.bids), len(up_book.asks),\n            f\"{up_book.best_bid:.6f}\" if up_book.best_bid is not None else \"None\",\n            f\"{up_book.best_ask:.6f}\" if up_book.best_ask is not None else \"None\",\n            bid_vol5, ask_vol5, bn10, an10, im,\n        )\n        LOG.info(\n            \"[Debug] DOWN book: levels=%d/%d best_bid=%s best_ask=%s \"\n            \"bid_vol5=%.6f ask_vol5=%.6f\",\n            len(down_book.bids), len(down_book.asks),\n            f\"{down_book.best_bid:.6f}\" if down_book.best_bid is not None else \"None\",\n            f\"{down_book.best_ask:.6f}\" if down_book.best_ask is not None else \"None\",\n            dbid_vol5, dask_vol5,\n        )\n        LOG.info(\n            \"[Debug] up_mid_open=%s up_mid=%s move=%s \"\n            \"spot_open=%s spot_now=%s spot_move=%.6f bps=%.3f ask=%s\",\n            f\"{up_mid_open:.6f}\" if up_mid_open is not None else \"None\",\n            f\"{up_mid:.6f}\" if up_mid is not None else \"None\",\n            f\"{move:+.6f}\" if move is not None else \"None\",\n            f\"{spot_open:.6f}\" if spot_open is not None else \"None\",\n            f\"{spot_now:.6f}\" if spot_now is not None else \"None\",\n            spot_move if spot_move is not None else float(\"nan\"),\n            sm_bps, f\"{ask:.6f}\" if ask is not None else \"None\",\n        )\n\n    def evaluate(self, *, market: Market, up_book: Book, down_book: Book,\n                 up_mid_open: float | None, spot_open: float | None,\n                 spot_now: float | None, now_ms: int) -> Signal | None:\n        p = self.p\n        secs = (market.end_ms - now_ms) / 1000.0\n\n        if not (p.entry_t_minus_s - 2.5 <= secs <= p.entry_t_minus_s + 0.5):\n            _gate(\"NOT_T240\")\n            return None\n\n        if not up_book.ready and not p.force_trade_test:\n            print(f\"[Strategy] trying REST fallback for up token={market.up_token} book_ready={up_book.ready}\", flush=True)\n            refreshed = try_refresh_book_snapshot(up_book, market=market, timeout=10.0)\n            if refreshed:\n                print(\n                    f\"[Strategy] REST fallback succeeded for {market.up_token}: \"\n                    f\"best_bid={up_book.best_bid} best_ask={up_book.best_ask}\",\n                    flush=True,\n                )\n            else:\n                self._debug(\n                    market=market, secs=secs, up_book=up_book, down_book=down_book,\n                    up_mid_open=up_mid_open, up_mid=None, spot_open=spot_open,\n                    spot_now=spot_now, spot_move=None, imbalance=None, move=None,\n                    side=\"\", ask=None, reason=\"UP_BOOK_NOT_READY\",\n                )\n                _gate(\"UP_BOOK_NOT_READY\")\n                return None\n\n        up_mid = up_book.mid\n        move = (up_mid - up_mid_open) if (up_mid is not None and up_mid_open is not None) else None\n        side = \"UP\" if (move is not None and move >= 0) else \"DOWN\"\n        if p.mode == \"fade\" and move is not None:\n            side = \"DOWN\" if side == \"UP\" else \"UP\"\n\n        raw_imb = _imbalance(up_book, p.imbalance_depth) if up_book.ready else None\n        imb = raw_imb if (raw_imb is not None and side == \"UP\") else (\n            -raw_imb if raw_imb is not None else None\n        )\n        spot_move = None\n        if spot_open is not None and spot_open > 0 and spot_now is not None:\n            spot_move = spot_now / (spot_open + EPS) - 1.0\n\n        book = up_book if side == \"UP\" else down_book\n        ask = book.best_ask\n        bid = book.best_bid\n\n        if p.force_trade_test:\n            if ask is None or not (0 < ask < 1):\n                alt_side = \"DOWN\" if side == \"UP\" else \"UP\"\n                alt = down_book if alt_side == \"DOWN\" else up_book\n                if alt.best_ask is not None and 0 < alt.best_ask < 1:\n                    side, book, ask, bid = alt_side, alt, alt.best_ask, alt.best_bid\n                else:\n                    self._debug(\n                        market=market, secs=secs, up_book=up_book, down_book=down_book,\n                        up_mid_open=up_mid_open, up_mid=up_mid, spot_open=spot_open,\n                        spot_now=spot_now, spot_move=spot_move, imbalance=imb,\n                        move=move, side=side, ask=None, reason=\"FORCE_NO_ASK\",\n                    )\n                    _gate(\"FORCE_NO_ASK\")\n                    return None\n            ask = float(ask)\n            bid = float(bid) if bid is not None else max(0.0, ask - 0.01)\n            self._debug(\n                market=market, secs=secs, up_book=up_book, down_book=down_book,\n                up_mid_open=up_mid_open, up_mid=up_mid, spot_open=spot_open,\n                spot_now=spot_now, spot_move=spot_move, imbalance=imb,\n                move=move, side=side, ask=ask, reason=\"FORCE_TRADE_TEST\",\n            )\n            LOG.warning(\n                \"[ForceTest] BYPASSING ALL THRESHOLDS market=%s side=%s ask=%.6f\",\n                market.slug, side, ask,\n            )\n            return Signal(\n                market=market, side=side, move=move or 0.0, imbalance=imb or 0.0,\n                spot_move=spot_move or 0.0, ask=ask, bid=bid,\n                spread=max(0.0, ask - bid), secs_to_close=secs, reason=\"force_test\",\n            )\n\n        if up_mid_open is None or up_mid is None:\n            self._debug(\n                market=market, secs=secs, up_book=up_book, down_book=down_book,\n                up_mid_open=up_mid_open, up_mid=up_mid, spot_open=spot_open,\n                spot_now=spot_now, spot_move=spot_move, imbalance=imb,\n                move=move, side=side, ask=ask, reason=\"MISSING_UP_MID\",\n            )\n            _gate(\"MISSING_UP_MID\")\n            return None\n        if spot_open is None or spot_open <= 0 or spot_now is None:\n            self._debug(\n                market=market, secs=secs, up_book=up_book, down_book=down_book,\n                up_mid_open=up_mid_open, up_mid=up_mid, spot_open=spot_open,\n                spot_now=spot_now, spot_move=spot_move, imbalance=imb,\n                move=move, side=side, ask=ask, reason=\"MISSING_SPOT\",\n            )\n            _gate(\"MISSING_SPOT\")\n            return None\n        if move is None or abs(move) < p.move_threshold:\n            self._debug(\n                market=market, secs=secs, up_book=up_book, down_book=down_book,\n                up_mid_open=up_mid_open, up_mid=up_mid, spot_open=spot_open,\n                spot_now=spot_now, spot_move=spot_move, imbalance=imb,\n                move=move, side=side, ask=ask, reason=\"MOVE_TOO_SMALL\",\n            )\n            _gate(\"MOVE_TOO_SMALL\")\n            return None\n        if imb is None or abs(imb) < p.min_book_imbalance:\n            self._debug(\n                market=market, secs=secs, up_book=up_book, down_book=down_book,\n                up_mid_open=up_mid_open, up_mid=up_mid, spot_open=spot_open,\n                spot_now=spot_now, spot_move=spot_move, imbalance=imb,\n                move=move, side=side, ask=ask, reason=\"IMBALANCE_TOO_SMALL\",\n            )\n            _gate(\"IMBALANCE_TOO_SMALL\")\n            return None\n        if spot_move is None or abs(spot_move) < p.min_spot_move:\n            self._debug(\n                market=market, secs=secs, up_book=up_book, down_book=down_book,\n                up_mid_open=up_mid_open, up_mid=up_mid, spot_open=spot_open,\n                spot_now=spot_now, spot_move=spot_move, imbalance=imb,\n                move=move, side=side, ask=ask, reason=\"SPOT_TOO_SMALL\",\n            )\n            _gate(\"SPOT_TOO_SMALL\")\n            return None\n\n        if not down_book.ready:\n            self._debug(\n                market=market, secs=secs, up_book=up_book, down_book=down_book,\n                up_mid_open=up_mid_open, up_mid=up_mid, spot_open=spot_open,\n                spot_now=spot_now, spot_move=spot_move, imbalance=imb,\n                move=move, side=side, ask=ask, reason=\"DOWN_BOOK_NOT_READY\",\n            )\n            _gate(\"DOWN_BOOK_NOT_READY\")\n            return None\n        if ask is None or bid is None or not (0 < ask < 1):\n            self._debug(\n                market=market, secs=secs, up_book=up_book, down_book=down_book,\n                up_mid_open=up_mid_open, up_mid=up_mid, spot_open=spot_open,\n                spot_now=spot_now, spot_move=spot_move, imbalance=imb,\n                move=move, side=side, ask=ask, reason=\"NO_QUOTE\",\n            )\n            _gate(\"NO_QUOTE\")\n            return None\n        if now_ms - book.last_update_ms > p.max_quote_staleness_s * 1000:\n            self._debug(\n                market=market, secs=secs, up_book=up_book, down_book=down_book,\n                up_mid_open=up_mid_open, up_mid=up_mid, spot_open=spot_open,\n                spot_now=spot_now, spot_move=spot_move, imbalance=imb,\n                move=move, side=side, ask=ask, reason=\"STALE_QUOTE\",\n            )\n            _gate(\"STALE_QUOTE\")\n            return None\n        if ask > p.max_entry_price:\n            self._debug(\n                market=market, secs=secs, up_book=up_book, down_book=down_book,\n                up_mid_open=up_mid_open, up_mid=up_mid, spot_open=spot_open,\n                spot_now=spot_now, spot_move=spot_move, imbalance=imb,\n                move=move, side=side, ask=ask, reason=\"PRICE_TOO_HIGH\",\n            )\n            _gate(\"PRICE_TOO_HIGH\")\n            return None\n\n        self._debug(\n            market=market, secs=secs, up_book=up_book, down_book=down_book,\n            up_mid_open=up_mid_open, up_mid=up_mid, spot_open=spot_open,\n            spot_now=spot_now, spot_move=spot_move, imbalance=imb,\n            move=move, side=side, ask=ask, reason=\"READY\",\n        )\n        return Signal(\n            market=market, side=side, move=move, imbalance=imb,\n            spot_move=spot_move, ask=ask, bid=bid,\n            spread=max(0.0, ask - bid), secs_to_close=secs, reason=\"ready\",\n        )\n```

如果你愿意，我下一步可以直接继续帮你做：
- 一版更稳的 `api_book_snapshot()` 诊断函数，专门检测 WebSocket 字段名是否真的和 Polymarket 真实 payload 不一致
- 直接给你一个“最小可运行的调试脚本”，用来抓取原始 WS JSON 以及 HTTP 结果，并打印真实结构
- 进一步把 `feed.py` 改成“先 REST 拉快照再 WS 增量更新”的更稳定风格
