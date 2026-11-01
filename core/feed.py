#!/usr/bin/env python3
"""Market data layer: Polymarket CLOB WebSocket + Binance bookTicker.

Design rules:
  * every book carries a local receive timestamp; strategy refuses stale books
  * reconnect loops never kill the process
  * Gamma discovery resolves the current btc-updown-5m market and token ids
  * REST fallback is forced whenever a book is not ready, so the strategy never
    silently starves on missing WebSocket book updates
  * All token IDs are preserved as full strings; no silent truncation
  * winner resolution is resilient: retry + multi-field extraction + no destructive clear
 """
from __future__ import annotations

import asyncio
import json
import logging
import time
import urllib.parse
import urllib.request
from dataclasses import dataclass, field
from typing import Any, Iterable

import requests

try:
    import websockets
except Exception:  # noqa: BLE001
    websockets = None

LOG = logging.getLogger("v7.feed")


def now_ms() -> int:
    return int(time.time() * 1000)


def _as_bool(value: Any) -> bool:
    """Normalize API booleans while handling stringified values like 'false'."""
    if value is None:
        return False
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return value != 0
    if isinstance(value, str):
        s = value.strip().lower()
        if s in {"", "null", "none", "nan"}:
            return False
        if s in {"1", "true", "t", "yes", "y", "up", "winner_up", "winner-up"}:
            return True
        if s in {"0", "false", "f", "no", "n", "down", "winner_down", "winner-down"}:
            return False
    return bool(value)


def _valid_token_id(value: Any) -> str | None:
    """Normalize token ids from Gamma/WS/REST without truncating the full string."""
    if value is None:
        return None
    token = str(value).strip()
    if not token:
        return None
    token = token.replace("\x00", "")
    if token.lower() in {"none", "null", "nan"}:
        return None
    if len(token) < 8:
        return None
    return token


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


def _normalize_winner(value: Any) -> str | None:
    """Normalize Polymarket winner labels to UP/DOWN across API variants."""
    if value is None:
        return None
    if isinstance(value, dict):
        for key in ("value", "name", "label", "outcome", "winner", "winningOutcome", "result"):
            if key in value:
                w = _normalize_winner(value.get(key))
                if w:
                    return w
        for key in ("index", "idx"):
            if key in value:
                try:
                    idx = int(value.get(key))
                except Exception:  # noqa: BLE001
                    idx = None
                if idx == 0:
                    return "DOWN"
                if idx == 1:
                    return "UP"
    if isinstance(value, (list, tuple)):
        for item in value:
            w = _normalize_winner(item)
            if w:
                return w
    if isinstance(value, bool):
        return "UP" if value else "DOWN"
    if isinstance(value, (int, float)):
        if value == 1:
            return "UP"
        if value == 0:
            return "DOWN"
        return None
    s = str(value).strip().lower()
    if s in {"up", "yes", "y", "1", "true", "winner_up", "winner-up", "up_token", "winner-token-up"}:
        return "UP"
    if s in {"down", "no", "n", "0", "false", "winner_down", "winner-down", "down_token", "winner-token-down"}:
        return "DOWN"
    return None


def _coerce_float_list(v: Any) -> list[float]:
    out: list[float] = []
    if v is None:
        return out
    if isinstance(v, list):
        items = v
    elif isinstance(v, str):
        try:
            items = json.loads(v)
        except Exception:
            items = [v]
        if not isinstance(items, list):
            items = [items]
    else:
        items = [v]

    for x in items:
        try:
            out.append(float(x))
        except Exception:  # noqa: BLE001
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
        self.last_update_ms = int(time.time() * 1000)
        if ts_ms is not None:
            self.last_update_ms = max(self.last_update_ms, int(ts_ms))

    def apply_change(self, price: float, size: float, side: str, ts_ms: int | None = None) -> None:
        book = self.bids if side.upper() in ("BUY", "BID", "BIDS") else self.asks
        if size <= 0:
            book.pop(price, None)
        elif 0 < price < 1:
            book[price] = size
        self.last_update_ms = int(time.time() * 1000)
        if ts_ms is not None:
            self.last_update_ms = max(self.last_update_ms, int(ts_ms))

    def set_top(self, bid: float | None, ask: float | None, ts_ms: int | None = None) -> None:
        if bid is not None and 0 < bid < 1:
            self.bids[bid] = self.bids.get(bid, 0.0) or 1e-9
        if ask is not None and 0 < ask < 1:
            self.asks[ask] = self.asks.get(ask, 0.0) or 1e-9
        self.last_update_ms = int(time.time() * 1000)
        if ts_ms is not None:
            self.last_update_ms = max(self.last_update_ms, int(ts_ms))

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


class MarketDiscovery:
    """Gamma market lookup for btc-updown-5m-<start_epoch>."""

    def __init__(self, gamma_url: str = "https://gamma-api.polymarket.com", timeout: float = 15.0):
        self.gamma_url = gamma_url.rstrip("/")
        self.timeout = timeout

    def _get_json(self, url: str) -> Any:
        req = urllib.request.Request(
            url,
            headers={
                "User-Agent": "v7-sniper/1.0",
                "Accept": "application/json",
            },
        )
        with urllib.request.urlopen(req, timeout=self.timeout) as r:
            return json.loads(r.read().decode("utf-8"))

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

    @staticmethod
    def _normalize_token(raw: Any) -> str | None:
        token = _valid_token_id(raw)
        if token is None:
            return None
        return token

    @staticmethod
    def _normalize_winner(value: Any) -> str | None:
        return _normalize_winner(value)

    @staticmethod
    def slug_for(now: int) -> tuple[str, int]:
        start_s = (now // 300_000) * 300
        return f"btc-updown-5m-{start_s}", start_s * 1000

    @staticmethod
    def _pick_outcome_name(value: Any) -> str | None:
        if value is None:
            return None
        if isinstance(value, str):
            s = value.strip()
            if s:
                return s
            return None
        if isinstance(value, (int, float)):
            return str(value)
        return None

    @staticmethod
    def _winner_from_index(index: Any, outcome_names: list[str]) -> str | None:
        try:
            idx = int(index)
        except Exception:  # noqa: BLE001
            return None
        if idx < 0 or idx >= len(outcome_names):
            return None
        return _normalize_winner(outcome_names[idx])

    @classmethod
    def _extract_winner_from_market(cls, m: dict) -> str | None:
        """Extract the winner from different Gamma payload shapes without recursive blow-ups."""
        if not isinstance(m, dict):
            return None

        winner_keys = (
            "winningOutcome",
            "winner",
            "winning_outcome",
            "winnerOutcome",
            "winningOutcomeIndex",
            "winnerOutcomeIndex",
            "winningOutcomeName",
            "winnerName",
            "outcomeWinner",
            "outcomeWinnerName",
            "result",
            "result_name",
            "winning_outcome_name",
            "finalAnswer",
            "final_answer",
            "finalOutcome",
            "final_outcome",
            "resolution",
            "winningAssetId",
            "winning_asset_id",
            "winnerAssetId",
            "winner_asset_id",
            "winnerTokenId",
            "winner_token_id",
            "winnerAsset",
            "winningAsset",
        )

        for key in winner_keys:
            if key in m:
                winner = cls._normalize_winner(m.get(key))
                if winner:
                    return winner

        status = str(
            m.get("status")
            or m.get("marketStatus")
            or m.get("market_status")
            or m.get("resolutionStatus")
            or ""
        ).lower()
        resolved = status in {
            "closed",
            "resolved",
            "finalized",
            "settled",
            "finalized_by_oracle",
            "resolved_by_oracle",
        }
        if not resolved:
            resolved = any(
                bool(m.get(k))
                for k in (
                    "closed",
                    "resolved",
                    "finalized",
                    "isResolved",
                    "is_resolved",
                    "isFinalized",
                    "is_finalized",
                    "hasResolved",
                    "has_resolved",
                )
                if k in m
            )

        outcome_names: list[str] = []
        outcomes_raw = cls._json_list(m.get("outcomes"))
        if not outcomes_raw:
            outcomes_raw = cls._json_list(m.get("outcomeNames"))
        if outcomes_raw:
            for raw in outcomes_raw:
                if isinstance(raw, dict):
                    for key in ("name", "label", "outcome", "title", "value"):
                        name_s = cls._pick_outcome_name(raw.get(key))
                        if name_s:
                            outcome_names.append(str(name_s))
                            break
                else:
                    name_s = cls._pick_outcome_name(raw)
                    if name_s:
                        outcome_names.append(str(name_s))

        prices = _coerce_float_list(
            m.get("outcomePrices")
            or m.get("prices")
            or m.get("outcome_prices")
            or m.get("resolvedPrices")
            or m.get("resolved_prices")
        )

        if outcome_names and prices and len(outcome_names) == len(prices):
            try:
                idx = max(range(len(prices)), key=lambda i: float(prices[i]))
                winner = cls._normalize_winner(outcome_names[idx])
                if winner:
                    return winner
            except Exception:  # noqa: BLE001
                pass

        for key in ("winningIndex", "winnerIndex", "winning_index", "winner_index"):
            if key in m:
                winner = cls._winner_from_index(m.get(key), outcome_names)
                if winner:
                    return winner

        if not outcome_names:
            winner_name = (
                m.get("winnerName")
                or m.get("winningOutcomeName")
                or m.get("result_name")
                or m.get("final_answer")
                or m.get("finalAnswer")
            )
            winner = cls._normalize_winner(winner_name)
            if winner:
                return winner

        if not resolved:
            return None

        if outcome_names and prices and len(outcome_names) == len(prices):
            try:
                idx = max(range(len(prices)), key=lambda i: float(prices[i]))
                winner = cls._normalize_winner(outcome_names[idx])
                if winner:
                    return winner
            except Exception:  # noqa: BLE001
                pass

        stack: list[tuple[Any, int]] = [(m, 0)]
        seen: set[int] = set()
        while stack:
            node, depth = stack.pop()
            if depth > 12:
                continue
            obj_id = id(node)
            if obj_id in seen:
                continue
            seen.add(obj_id)

            if isinstance(node, dict):
                for key in winner_keys:
                    if key in node:
                        winner = cls._normalize_winner(node.get(key))
                        if winner:
                            return winner
                for value in node.values():
                    if isinstance(value, (dict, list, tuple)):
                        stack.append((value, depth + 1))
            elif isinstance(node, list):
                for item in node:
                    if isinstance(item, (dict, list, tuple)):
                        stack.append((item, depth + 1))

        return None

    @staticmethod
    def _coerce_float_list(v: Any) -> list[float]:
        return _coerce_float_list(v)

    @staticmethod
    def _json_list(v: Any) -> list:
        return MarketDiscovery._json_list(v)

    def fetch(self, slug: str, retries: int = 5) -> Market | None:
        url = f"{self.gamma_url}/markets?slug={urllib.parse.quote(slug)}"
        for attempt in range(retries):
            try:
                data = self._get_json(url)
                if isinstance(data, dict):
                    data = [data]
                if not data:
                    return None
                m = data[0]
                raw_tokens = self._json_list(m.get("clobTokenIds"))
                tokens: list[str] = []
                for raw in raw_tokens:
                    token = self._normalize_token(raw)
                    if token:
                        tokens.append(token)
                if len(tokens) < 2:
                    return None
                start_ms = int(slug.rsplit("-", 1)[-1]) * 1000
                return Market(
                    slug=slug,
                    condition_id=str(m.get("conditionId") or m.get("condition_id") or ""),
                    up_token=str(tokens[0]),
                    down_token=str(tokens[1]),
                    start_ms=start_ms,
                    end_ms=start_ms + 300_000,
                    question=str(m.get("question") or ""),
                    neg_risk=_as_bool(m.get("negRisk") or m.get("neg_risk") or False),
                )
            except Exception as exc:  # noqa: BLE001
                LOG.warning("discover %s attempt=%d: %s", slug, attempt + 1, exc)
                time.sleep(min(2 ** attempt, 8))
        return None

    def resolve(self, slug: str, retries: int = 6, backoff_s: float = 1.5) -> str | None:
        """
        Robust winner resolution:
        - retry Gamma queries
        - support multiple payload shapes
        - do not assume None means final loser
        """
        last_err = None
        for attempt in range(retries):
            try:
                url = f"{self.gamma_url}/markets?slug={urllib.parse.quote(slug)}"
                req = urllib.request.Request(
                    url,
                    headers={
                        "User-Agent": "v7-resolve/1.0",
                        "Accept": "application/json",
                    },
                )
                with urllib.request.urlopen(req, timeout=self.timeout) as r:
                    data = json.loads(r.read().decode("utf-8"))

                if isinstance(data, dict):
                    data = [data]
                if not isinstance(data, list) or not data:
                    last_err = ValueError("empty or invalid gamma payload")
                    if attempt < retries - 1:
                        time.sleep(backoff_s * (attempt + 1))
                        continue
                    return None

                queue = []
                for item in data:
                    if isinstance(item, dict):
                        queue.append(item)
                        nested = item.get("markets")
                        if isinstance(nested, list):
                            queue.extend(nested)
                        for key in ("payload", "market", "data"):
                            child = item.get(key)
                            if isinstance(child, dict):
                                queue.append(child)
                            elif isinstance(child, list):
                                queue.extend([x for x in child if isinstance(x, dict)])
                    elif isinstance(item, list):
                        queue.extend([x for x in item if isinstance(x, dict)])
                data = queue or data

                for m in data:
                    if not isinstance(m, dict):
                        continue

                    winner = self._extract_winner_from_market(m)
                    if winner:
                        return winner

                    for key in ("payload", "market", "data", "result_payload", "resolvedMarket"):
                        node = m.get(key)
                        if isinstance(node, dict):
                            winner = self._extract_winner_from_market(node)
                            if winner:
                                return winner
                        elif isinstance(node, list):
                            for item in node:
                                if isinstance(item, dict):
                                    winner = self._extract_winner_from_market(item)
                                    if winner:
                                        return winner

                last_err = ValueError("no final winner in gamma payload")
            except Exception as exc:  # noqa: BLE001
                last_err = exc
                LOG.warning("resolve %s attempt=%d failed: %s", slug, attempt + 1, exc)

            if attempt < retries - 1:
                time.sleep(backoff_s * (attempt + 1))

        LOG.warning("resolve %s unresolved after retries: %s", slug, last_err)
        return None


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
        self.market_resolution_cache: dict[str, str] = {}
        self.asset_resolution_cache: dict[str, str] = {}

    def book(self, token_id: str) -> Book:
        token = _valid_token_id(token_id)
        if token is None:
            raise ValueError(f"invalid token_id: {token_id!r}")
        return self.books.setdefault(token, Book(token_id=token))

    def ensure_book_ready(self, token_id: str, *, timeout: float = 10.0) -> Book | None:
        token = _valid_token_id(token_id)
        if token is None:
            return None
        book = self.book(token)
        if book.ready:
            return book
        return self.refresh_book_snapshot(token, timeout=timeout)

    def get_tick(self, token_id: str, *, force_refresh: bool = True, timeout: float = 10.0) -> Book | None:
        token = _valid_token_id(token_id)
        if token is None:
            return None

        book = self.book(token)

        if force_refresh:
            now = now_ms()
            stale = False
            if not book.ready:
                stale = True
            else:
                if book.last_update_ms <= 0:
                    stale = True
                elif now - book.last_update_ms > 1000:
                    stale = True

            if stale:
                print(
                    f"[REST Fallback] stale/empty book for token={token} age_ms={now - book.last_update_ms if book.last_update_ms else 'n/a'}",
                    flush=True,
                )
                refreshed = self.refresh_book_snapshot(token, timeout=timeout)
                if refreshed is not None:
                    return refreshed

        return book

    def refresh_book_snapshot(self, token_id: str, timeout: float = 10.0) -> Book | None:
        token = _valid_token_id(token_id)
        if token is None:
            return None
        print(f"[REST Fallback] Fetching book for token: {token}", flush=True)

        url = f"https://clob.polymarket.com/book?token_id={urllib.parse.quote(token)}"
        headers = {
            "User-Agent": "v7-sniper/1.0",
            "Accept": "application/json",
            "Referer": "https://polymarket.com/",
        }

        try:
            resp = requests.get(url, timeout=timeout, headers=headers)
            resp.raise_for_status()
            payload = resp.json()
        except Exception as exc:  # noqa: BLE001
            print(f"[REST Fallback] Failed for token={token} url={url} error={exc}", flush=True)
            return None

        data = payload
        if isinstance(data, dict):
            if "book" in data and isinstance(data["book"], dict):
                data = data["book"]
            elif "data" in data and isinstance(data["data"], dict):
                data = data["data"]

        if not isinstance(data, dict):
            print(f"[REST Fallback] Unexpected response shape for token={token}: {type(data).__name__}", flush=True)
            return None

        bids = data.get("bids")
        if bids is None:
            bids = data.get("buys")
        if bids is None:
            bids = data.get("bid")

        if bids is None and "book" in data and isinstance(data["book"], dict):
            book_obj = data["book"]
            bids = book_obj.get("bids")
            if bids is None:
                bids = book_obj.get("buys")
            if bids is None:
                bids = book_obj.get("bid")

        if bids is None:
            bids = []

        asks = data.get("asks")
        if asks is None:
            asks = data.get("sells")
        if asks is None:
            asks = data.get("ask")

        if asks is None and "book" in data and isinstance(data["book"], dict):
            book_obj = data["book"]
            asks = book_obj.get("asks")
            if asks is None:
                asks = book_obj.get("sells")
            if asks is None:
                asks = book_obj.get("ask")

        if asks is None:
            asks = []

        book = self.book(token)
        book.set_book(bids, asks, now_ms())
        book.last_update_ms = int(time.time() * 1000)

        print(
            f"[REST] Refreshed token={token} bids={len(bids)} asks={len(asks)}",
            flush=True,
        )
        return book

    @staticmethod
    def _normalize_event_winner(value: Any) -> str | None:
        return _normalize_winner(value)

    def _remember_resolution(self, msg: dict) -> None:
        if not isinstance(msg, dict):
            return

        winner = None
        for key in (
            "winner",
            "winningOutcome",
            "winning_outcome",
            "result",
            "winningOutcomeName",
            "winnerName",
            "finalOutcome",
            "final_outcome",
            "outcomeWinner",
            "winningAssetId",
            "winning_asset_id",
            "winnerAssetId",
            "winner_asset_id",
            "winning_asset",
            "winner_asset",
        ):
            if key in msg:
                winner = self._normalize_event_winner(msg.get(key))
                if winner:
                    break

        if winner is None:
            for key in ("payload", "market", "data"):
                node = msg.get(key)
                if isinstance(node, dict):
                    if node.get("winner") is not None:
                        winner = self._normalize_event_winner(node.get("winner"))
                    elif node.get("winningOutcome") is not None:
                        winner = self._normalize_event_winner(node.get("winningOutcome"))
                    if winner:
                        break

        if winner is None:
            market_id = str(
                msg.get("condition_id")
                or msg.get("conditionId")
                or msg.get("market_id")
                or msg.get("marketId")
                or msg.get("id")
                or ""
            )
            if not market_id:
                return
            if msg.get("marketStatus") is not None or msg.get("status") is not None:
                if str(msg.get("marketStatus") or msg.get("status") or "").lower() in {"resolved", "closed", "finalized", "settled"}:
                    winner = self._normalize_event_winner(msg.get("winner")) or self._normalize_event_winner(msg.get("result"))

        if winner is None:
            return

        condition_id = str(
            msg.get("condition_id")
            or msg.get("conditionId")
            or msg.get("market_id")
            or msg.get("marketId")
            or msg.get("id")
            or ""
        )
        if condition_id:
            self.market_resolution_cache[condition_id] = winner

        winning_asset_id = msg.get("winning_asset_id") or msg.get("winningAssetId") or msg.get("winner_asset_id") or msg.get("winnerAssetId")
        if winning_asset_id:
            self.asset_resolution_cache[str(winning_asset_id)] = winner

        for key in ("assets_ids", "asset_ids", "tokens"):
            ids = msg.get(key)
            if isinstance(ids, list):
                for asset in ids:
                    if asset:
                        self.asset_resolution_cache[str(asset)] = winner

    def _extract_event_winner(self, msg: dict) -> str | None:
        winner = None
        for key in (
            "winner",
            "winningOutcome",
            "winning_outcome",
            "result",
            "winningOutcomeName",
            "winnerName",
            "finalOutcome",
            "final_outcome",
            "outcomeWinner",
            "winningAssetId",
            "winning_asset_id",
            "winnerAssetId",
            "winner_asset_id",
            "winning_asset",
            "winner_asset",
        ):
            if key in msg:
                winner = self._normalize_event_winner(msg.get(key))
                if winner:
                    return winner
        for key in ("payload", "market", "data"):
            node = msg.get(key)
            if isinstance(node, dict):
                w = self._extract_event_winner(node)
                if w:
                    return w
        return None

    async def subscribe(self, tokens: Iterable[str]) -> None:
        new = {str(t).strip() for t in tokens if t and _valid_token_id(t)}
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

    async def _run(self) -> None:
        backoff = 1.0
        while not self._stop.is_set():
            try:
                async with websockets.connect(
                    self.ws_url,
                    ping_interval=20,
                    ping_timeout=20,
                    close_timeout=5,
                    max_size=16 * 1024 * 1024,
                ) as ws:
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

            event_type = m.get("event_type")
            if event_type is None:
                event_type = m.get("type")
            if event_type is None:
                event_type = m.get("eventType")
            if event_type is None:
                event_type = ""
            et = str(event_type).lower()

            if et in {"market_resolved", "marketresolved", "resolution", "resolved", "finalized", "finalize", "market_finalized"}:
                winner = self._extract_event_winner(m)
                if winner:
                    self._remember_resolution(m)
                    LOG.info("[WS resolved] event=%s winner=%s", et, winner)
                continue

            token = m.get("asset_id")
            if token is None:
                token = m.get("token_id")
            if token is None:
                token = m.get("tokenId")
            if token is None:
                token = m.get("assetId")
            if token is None:
                token = ""

            if not token:
                payload = m.get("payload")
                if isinstance(payload, dict):
                    token = payload.get("asset_id")
                    if token is None:
                        token = payload.get("token_id")
                    if token is None:
                        token = payload.get("tokenId")
                    if token is None:
                        token = ""
            token = _valid_token_id(token)
            if not token:
                continue

            b = self.book(token)
            ts = now_ms()

            if et in ("book", "book_update", "booksnapshot"):
                bids = m.get("bids")
                if bids is None:
                    bids = m.get("buys")
                if bids is None:
                    bids = m.get("bid")

                asks = m.get("asks")
                if asks is None:
                    asks = m.get("sells")
                if asks is None:
                    asks = m.get("ask")

                if bids is not None or asks is not None:
                    b.set_book(bids if bids is not None else [], asks if asks is not None else [], ts)
                    b.last_update_ms = int(time.time() * 1000)

            elif et == "price_change":
                changes = m.get("changes")
                if changes is None:
                    changes = m.get("price_changes")
                if changes is None:
                    changes = []

                if not changes and m.get("price") is not None:
                    changes = [{
                        "price": m.get("price"),
                        "size": m.get("size"),
                        "side": m.get("side"),
                    }]

                for c in changes:
                    try:
                        b.apply_change(
                            float(c["price"]),
                            float(c.get("size", 0)),
                            str(c.get("side", "BUY")),
                            ts,
                        )
                        b.last_update_ms = int(time.time() * 1000)
                    except (KeyError, TypeError, ValueError):
                        continue

            elif et in ("best_bid_ask", "tick_size_change"):
                b.last_update_ms = int(time.time() * 1000)

    def _handle_price_change(self, raw: str | bytes) -> None:
        pass


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


def try_refresh_book_snapshot(book: Book, market: Market | None = None, timeout: float = 10.0) -> bool:
    """Direct synchronous REST refresh for a single token book."""
    if book.ready:
        return True

    token_id = _valid_token_id(book.token_id)
    if not token_id:
        return False

    print(f"[REST Fallback] Fetching book for token: {token_id}", flush=True)

    url = f"https://clob.polymarket.com/book?token_id={urllib.parse.quote(token_id)}"
    headers = {
        "User-Agent": "v7-sniper/1.0",
        "Accept": "application/json",
        "Referer": "https://polymarket.com/",
    }

    try:
        resp = requests.get(url, timeout=timeout, headers=headers)
        resp.raise_for_status()
        payload = resp.json()
    except Exception as exc:  # noqa: BLE001
        print(f"[REST Fallback] Failed for token={token_id} url={url} error={exc}", flush=True)
        return False

    data = payload
    if isinstance(data, dict):
        if "book" in data and isinstance(data["book"], dict):
            data = data["book"]
        elif "data" in data and isinstance(data["data"], dict):
            data = data["data"]

    if not isinstance(data, dict):
        print(f"[REST Fallback] Unexpected response shape for token={token_id}: {type(data).__name__}", flush=True)
        return False

    bids = data.get("bids")
    if bids is None:
        bids = data.get("buys")
    if bids is None:
        bids = data.get("bid")

    if bids is None and "book" in data and isinstance(data["book"], dict):
        book_obj = data["book"]
        bids = book_obj.get("bids")
        if bids is None:
            bids = book_obj.get("buys")
        if bids is None:
            bids = book_obj.get("bid")

    if bids is None:
        bids = []

    asks = data.get("asks")
    if asks is None:
        asks = data.get("sells")
    if asks is None:
        asks = data.get("ask")

    if asks is None and "book" in data and isinstance(data["book"], dict):
        book_obj = data["book"]
        asks = book_obj.get("asks")
        if asks is None:
            asks = book_obj.get("sells")
        if asks is None:
            asks = book_obj.get("ask")

    if asks is None:
        asks = []

    book.set_book(bids, asks, now_ms())
    book.last_update_ms = int(time.time() * 1000)

    print(
        f"[REST] Refreshed token={token_id} bids={len(bids)} asks={len(asks)}",
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

