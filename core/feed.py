"""Market data layer: Polymarket CLOB WebSocket + Binance bookTicker.

Design rules:
  * every book carries a local receive timestamp; strategy refuses stale books
  * reconnect loops never kill the process
  * Gamma discovery resolves the current btc-updown-5m market and token ids
"""
from __future__ import annotations

import asyncio
import json
import logging
import math
import time
import urllib.parse
import urllib.request
from dataclasses import dataclass, field
from typing import Any, Iterable

try:
    import websockets
except Exception:  # noqa: BLE001
    websockets = None

LOG = logging.getLogger("v7.feed")


def now_ms() -> int:
    return int(time.time() * 1000)


# --------------------------------------------------------------------------- book
def _levels(raw: Any) -> list[tuple[float, float]]:
    """Normalize Polymarket bids/asks into [(price, size)]."""
    out: list[tuple[float, float]] = []
    if not raw:
        return out
    for item in raw:
        try:
            if isinstance(item, dict):
                p = float(item.get("price"))
                s = float(item.get("size", 0))
            else:
                p, s = float(item[0]), float(item[1])
            if 0 < p < 1 and s > 0:
                out.append((p, s))
        except (TypeError, ValueError, IndexError):
            continue
    return out


@dataclass
class Book:
    token_id: str
    bids: dict[float, float] = field(default_factory=dict)
    asks: dict[float, float] = field(default_factory=dict)
    last_update_ms: int = 0

    @property
    def ready(self) -> bool:
        return bool(self.bids) and bool(self.asks) and self.last_update_ms > 0

    @property
    def best_bid(self) -> float | None:
        return max(self.bids) if self.bids else None

    @property
    def best_ask(self) -> float | None:
        return min(self.asks) if self.asks else None

    @property
    def mid(self) -> float | None:
        b, a = self.best_bid, self.best_ask
        return (a + b) / 2.0 if a is not None and b is not None else None

    def set_book(self, bids: Any, asks: Any, ts_ms: int | None = None) -> None:
        self.bids = {p: s for p, s in _levels(bids)}
        self.asks = {p: s for p, s in _levels(asks)}
        self.last_update_ms = ts_ms or now_ms()

    def apply_change(self, price: float, size: float, side: str, ts_ms: int | None = None) -> None:
        book = self.bids if side.upper() in ("BUY", "BID", "BIDS") else self.asks
        if size <= 0:
            book.pop(price, None)
        elif 0 < price < 1:
            book[price] = size
        self.last_update_ms = ts_ms or now_ms()

    def set_top(self, bid: float | None, ask: float | None, ts_ms: int | None = None) -> None:
        if bid is not None and 0 < bid < 1:
            self.bids[bid] = self.bids.get(bid, 0.0) or 1e-9
        if ask is not None and 0 < ask < 1:
            self.asks[ask] = self.asks.get(ask, 0.0) or 1e-9
        self.last_update_ms = ts_ms or now_ms()

    def imbalance(self, levels: int = 10) -> float | None:
        if not self.bids or not self.asks:
            return None
        bp = sorted(self.bids, reverse=True)[:levels]
        ap = sorted(self.asks)[:levels]
        bn = sum(p * self.bids[p] for p in bp)
        an = sum(p * self.asks[p] for p in ap)
        if bn + an <= 0:
            return None
        return (bn - an) / (bn + an)


@dataclass
class Market:
    slug: str
    condition_id: str
    up_token: str
    down_token: str
    start_ms: int
    end_ms: int
    question: str = ""
    neg_risk: bool = False


# ---------------------------------------------------------------------- discovery
class MarketDiscovery:
    """Gamma market lookup for btc-updown-5m-<start_epoch>."""

    def __init__(self, gamma_url: str = "https://gamma-api.polymarket.com", timeout: float = 15.0):
        self.gamma_url = gamma_url.rstrip("/")
        self.timeout = timeout

    def _get(self, url: str) -> Any:
        req = urllib.request.Request(url, headers={
            "User-Agent": "v7-sniper/1.0", "Accept": "application/json",
        })
        with urllib.request.urlopen(req, timeout=self.timeout) as r:
            return json.loads(r.read().decode("utf-8"))

    @staticmethod
    def slug_for(now: int) -> tuple[str, int]:
        start_s = (now // 300_000) * 300
        return f"btc-updown-5m-{start_s}", start_s * 1000

    @staticmethod
    def _json_list(v: Any) -> list:
        if isinstance(v, list):
            return v
        if isinstance(v, str):
            try:
                x = json.loads(v)
                return x if isinstance(x, list) else []
            except Exception:  # noqa: BLE001
                return []
        return []

    def fetch(self, slug: str, retries: int = 5) -> Market | None:
        url = f"{self.gamma_url}/markets?slug={urllib.parse.quote(slug)}"
        for attempt in range(retries):
            try:
                data = self._get(url)
                if isinstance(data, dict):
                    data = [data]
                if not data:
                    return None
                m = data[0]
                tokens = self._json_list(m.get("clobTokenIds"))
                if len(tokens) < 2:
                    return None
                start_ms = int(slug.rsplit("-", 1)[-1]) * 1000
                return Market(
                    slug=slug,
                    condition_id=str(m.get("conditionId") or ""),
                    up_token=str(tokens[0]),
                    down_token=str(tokens[1]),
                    start_ms=start_ms,
                    end_ms=start_ms + 300_000,
                    question=str(m.get("question") or ""),
                    neg_risk=bool(m.get("negRisk") or False),
                )
            except Exception as exc:  # noqa: BLE001
                LOG.warning("discover %s attempt=%d: %s", slug, attempt + 1, exc)
                time.sleep(min(2 ** attempt, 8))
        return None

    def resolve(self, slug: str) -> str | None:
        """Return 'UP'/'DOWN' when closed, else None."""
        try:
            data = self._get(f"{self.gamma_url}/markets?slug={urllib.parse.quote(slug)}")
            if isinstance(data, dict):
                data = [data]
            if not data:
                return None
            m = data[0]
            if not (m.get("closed") or m.get("resolved")):
                return None
            w = str(m.get("winningOutcome") or m.get("winner") or "").lower()
            if w in ("up", "yes"):
                return "UP"
            if w in ("down", "no"):
                return "DOWN"
            outcomes = [str(x) for x in self._json_list(m.get("outcomes"))]
            prices = self._json_list(m.get("outcomePrices"))
            if outcomes and prices:
                vals = [float(x) for x in prices]
                i = max(range(len(vals)), key=lambda j: vals[j])
                if vals[i] >= 0.99:
                    return "UP" if outcomes[i].lower() in ("up", "yes") else "DOWN"
        except Exception as exc:  # noqa: BLE001
            LOG.warning("resolve %s: %s", slug, exc)
        return None


# -------------------------------------------------------------------------- feeds
class PolymarketFeed:
    def __init__(self, ws_url: str, max_backoff: float = 30.0):
        if websockets is None:
            raise RuntimeError("websockets is required: pip install websockets")
        self.ws_url = ws_url
        self.books: dict[str, Book] = {}
        self.tokens: set[str] = set()
        self._ws = None
        self._task: asyncio.Task | None = None
        self._stop = asyncio.Event()
        self._max_backoff = max_backoff
        self._raw_debug_count = 0

    def book(self, token_id: str) -> Book:
        return self.books.setdefault(token_id, Book(token_id))

    async def subscribe(self, tokens: Iterable[str]) -> None:
        new = {str(t) for t in tokens if t}
        if new == self.tokens:
            return
        self.tokens = new
        for t in new:
            self.book(t)
        if self._ws is not None:
            try:
                await self._ws.send(json.dumps({"assets_ids": sorted(self.tokens), "type": "market"}))
            except Exception as exc:  # noqa: BLE001
                LOG.warning("subscribe send failed: %s", exc)

    async def start(self) -> None:
        if self._task is None:
            self._task = asyncio.create_task(self._run(), name="polymarket-feed")

    async def stop(self) -> None:
        self._stop.set()
        if self._ws is not None:
            await self._ws.close()
        if self._task:
            self._task.cancel()

    def _show_raw_ws(self, raw: str | bytes) -> None:
        if self._raw_debug_count >= 5:
            return
        preview = raw.decode("utf-8", errors="replace") if isinstance(raw, bytes) else str(raw)
        print(f"[WS raw #{self._raw_debug_count + 1}] {preview[:2000]}", flush=True)
        self._raw_debug_count += 1

    def fetch_rest_book(self, token_id: str, timeout: float = 10.0) -> Book | None:
        token_id = str(token_id).strip()
        if not token_id:
            return None
        url = f"https://clob.polymarket.com/book?token_id={urllib.parse.quote(token_id)}"
        print(f"[REST fallback] GET {url}", flush=True)
        req = urllib.request.Request(
            url,
            headers={
                "User-Agent": "v7-sniper/1.0",
                "Accept": "application/json",
                "Referer": "https://polymarket.com/",
            },
        )
        try:
            with urllib.request.urlopen(req, timeout=timeout) as r:
                payload = r.read().decode("utf-8", errors="replace")
                print(f"[REST fallback] URL={url} response={payload[:2000]}", flush=True)
                data = json.loads(payload)
        except Exception as exc:  # noqa: BLE001
            print(f"[REST fallback] FAIL URL={url} error={exc}", flush=True)
            return None

        if isinstance(data, dict):
            if "book" in data and isinstance(data["book"], dict):
                data = data["book"]
            elif "data" in data and isinstance(data["data"], dict):
                data = data["data"]

        if not isinstance(data, dict):
            print(f"[REST fallback] unexpected format for token_id={token_id}: {type(data).__name__}", flush=True)
            return None

        book = self.book(token_id)
        bids = data.get("bids") or data.get("buys") or data.get("bid")
        asks = data.get("asks") or data.get("sells") or data.get("ask")
        if bids is None and "book" in data and isinstance(data["book"], dict):
            bids = data["book"].get("bids") or data["book"].get("buys")
        if asks is None and "book" in data and isinstance(data["book"], dict):
            asks = data["book"].get("asks") or data["book"].get("sells")
        if bids is None or asks is None:
            print(f"[REST fallback] no bids/asks in payload for token_id={token_id}: {data}", flush=True)
            return None

        book.set_book(bids, asks, now_ms())
        print(
            f"[REST fallback] refreshed token={token_id} bids={len(book.bids)} asks={len(book.asks)} "
            f"best_bid={book.best_bid} best_ask={book.best_ask}",
            flush=True,
        )
        return book

    async def _run(self) -> None:
        backoff = 1.0
        while not self._stop.is_set():
            try:
                async with websockets.connect(self.ws_url, ping_interval=20, ping_timeout=20,
                                              close_timeout=5, max_size=16 * 1024 * 1024) as ws:
                    self._ws = ws
                    backoff = 1.0
                    if self.tokens:
                        await ws.send(json.dumps({"assets_ids": sorted(self.tokens), "type": "market"}))
                    LOG.info("Polymarket WS connected, tokens=%d", len(self.tokens))
                    async for raw in ws:
                        self._show_raw_ws(raw)
                        self._handle(raw)
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001
                LOG.warning("Polymarket WS error: %s; reconnect in %.1fs", exc, backoff)
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, self._max_backoff)
            finally:
                self._ws = None

    def _handle(self, raw: str | bytes) -> None:
        try:
            msg = json.loads(raw)
        except Exception:  # noqa: BLE001
            return
        for m in (msg if isinstance(msg, list) else [msg]):
            if not isinstance(m, dict):
                continue
            et = str(m.get("event_type") or m.get("type") or m.get("eventType") or "").lower()
            token = str(
                m.get("asset_id")
                or m.get("token_id")
                or m.get("tokenId")
                or m.get("assetId")
                or ""
            )
            if not token:
                if "payload" in m and isinstance(m["payload"], dict):
                    token = str(
                        m["payload"].get("asset_id")
                        or m["payload"].get("token_id")
                        or m["payload"].get("tokenId")
                        or ""
                    )
            if not token:
                continue
            b = self.book(token)
            ts = now_ms()
            if et in ("book", "book_update", "booksnapshot"):
                bids = m.get("bids") or m.get("buys") or m.get("bid")
                asks = m.get("asks") or m.get("sells") or m.get("ask")
                if bids is None and "payload" in m and isinstance(m["payload"], dict):
                    bids = m["payload"].get("bids") or m["payload"].get("buys")
                if asks is None and "payload" in m and isinstance(m["payload"], dict):
                    asks = m["payload"].get("asks") or m["payload"].get("sells")
                if bids is not None or asks is not None:
                    b.set_book(bids or [], asks or [], ts)
            elif et == "price_change":
                changes = m.get("changes") or m.get("price_changes") or []
                if not changes and m.get("price") is not None:
                    changes = [{"price": m.get("price"), "size": m.get("size"),
                                "side": m.get("side")}]
                for c in changes:
                    try:
                        b.apply_change(float(c["price"]), float(c.get("size", 0)),
                                       str(c.get("side", "BUY")), ts)
                    except (KeyError, TypeError, ValueError):
                        continue
            elif et in ("best_bid_ask", "tick_size_change"):
                b.last_update_ms = ts


class BinanceFeed:
    def __init__(self, ws_url: str):
        if websockets is None:
            raise RuntimeError("websockets is required: pip install websockets")
        self.ws_url = ws_url
        self.price: float | None = None
        self.last_update_ms: int = 0
        self._task: asyncio.Task | None = None
        self._stop = asyncio.Event()

    async def start(self) -> None:
        if self._task is None:
            self._task = asyncio.create_task(self._run(), name="binance-feed")

    async def stop(self) -> None:
        self._stop.set()
        if self._task:
            self._task.cancel()

    async def _run(self) -> None:
        backoff = 1.0
        while not self._stop.is_set():
            try:
                async with websockets.connect(self.ws_url, ping_interval=20, ping_timeout=20) as ws:
                    backoff = 1.0
                    LOG.info("Binance WS connected")
                    async for raw in ws:
                        data = json.loads(raw)
                        bid = data.get("b")
                        ask = data.get("a")
                        if bid is not None and ask is not None:
                            self.price = (float(bid) + float(ask)) / 2.0
                            self.last_update_ms = now_ms()
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001
                LOG.warning("Binance WS error: %s; reconnect in %.1fs", exc, backoff)
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2, 30.0)

    def spot(self) -> float | None:
        if self.price is None:
            return None
        if now_ms() - self.last_update_ms > 5_000:
            return None
        return self.price


# ---------------------------------------------------------------------------
# compatibility helpers for direct strategy-layer fallback
# ---------------------------------------------------------------------------

def try_refresh_book_snapshot(book: Book, market: Market | None = None, timeout: float = 10.0) -> bool:
    """Direct synchronous REST refresh for a single token book."""
    if book.ready:
        return True
    token_id = str(book.token_id).strip()
    if not token_id:
        return False
    url = f"https://clob.polymarket.com/book?token_id={urllib.parse.quote(token_id)}"
    print(f"[REST fallback] GET {url}", flush=True)
    req = urllib.request.Request(
        url,
        headers={
            "User-Agent": "v7-sniper/1.0",
            "Accept": "application/json",
            "Referer": "https://polymarket.com/",
        },
    )
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            payload = r.read().decode("utf-8", errors="replace")
            print(f"[REST fallback] URL={url} response={payload[:2000]}", flush=True)
            data = json.loads(payload)
    except Exception as exc:  # noqa: BLE001
        print(f"[REST fallback] FAIL URL={url} error={exc}", flush=True)
        return False

    if isinstance(data, dict):
        if "book" in data and isinstance(data["book"], dict):
            data = data["book"]
        elif "data" in data and isinstance(data["data"], dict):
            data = data["data"]

    if not isinstance(data, dict):
        print(f"[REST fallback] unexpected format for token_id={token_id}: {type(data).__name__}", flush=True)
        return False

    bids = data.get("bids") or data.get("buys") or data.get("bid")
    asks = data.get("asks") or data.get("sells") or data.get("ask")
    if bids is None or asks is None:
        print(f"[REST fallback] no bids/asks in payload for token_id={token_id}: {data}", flush=True)
        return False

    book.set_book(bids, asks, now_ms())
    print(
        f"[REST fallback] refreshed token={token_id} bids={len(book.bids)} asks={len(book.asks)} "
        f"best_bid={book.best_bid} best_ask={book.best_ask}",
        flush=True,
    )
    return book.ready


__all__ = [
    "Book",
    "Market",
    "MarketDiscovery",
    "PolymarketFeed",
    "BinanceFeed",
    "now_ms",
    "try_refresh_book_snapshot",
]
