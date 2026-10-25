#!/usr/bin/env python3
"""V7 crash-safe position state.

state.json is the single source of truth for open positions.
- atomic write (tmp + os.replace)
- cross-process file lock
- idempotent add by trade_id / market_slug
- startup recovery log

No private keys or signatures are stored here.
"""
from __future__ import annotations

import json
import logging
import os
import threading
import time
from contextlib import contextmanager
from dataclasses import asdict, dataclass, field, fields
from pathlib import Path
from typing import Any, Iterable

try:
    from filelock import FileLock
except Exception:  # noqa: BLE001
    FileLock = None

LOG = logging.getLogger("v7.state")

ACTIVE_STATUSES = {"pending_fill", "open", "redeem_pending"}
FINAL_STATUSES = {"redeemed", "lost", "cancelled"}


def now_ms() -> int:
    return int(time.time() * 1000)


def iso_utc(ms: int | None) -> str | None:
    if not ms:
        return None
    return time.strftime("%Y-%m-%dT%H:%M:%S", time.gmtime(ms / 1000)) + "Z"


@dataclass
class Position:
    trade_id: str
    market_slug: str
    condition_id: str
    token_id: str
    side: str                      # UP / DOWN (or YES / NO)
    entry_price: float
    shares: float
    opened_at_ms: int
    end_time_ms: int
    status: str = "open"
    order_id: str | None = None
    tx_hash: str | None = None
    redeemed_at_ms: int | None = None
    amount_usdc: float | None = None
    attempts: int = 0
    last_error: str | None = None
    last_attempt_ms: int | None = None
    neg_risk: bool = False
    extra: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        d = asdict(self)
        d["opened_at"] = iso_utc(self.opened_at_ms)
        d["end_time"] = iso_utc(self.end_time_ms)
        d["redeemed_at"] = iso_utc(self.redeemed_at_ms)
        return d

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> "Position":
        allowed = {f.name for f in fields(cls)}
        clean = {k: v for k, v in d.items() if k in allowed}
        clean.setdefault("trade_id", f"{d.get('market_slug','?')}:{d.get('side','?')}:{d.get('opened_at_ms', now_ms())}")
        clean.setdefault("condition_id", "")
        clean.setdefault("token_id", "")
        clean.setdefault("opened_at_ms", now_ms())
        clean.setdefault("end_time_ms", now_ms())
        clean.setdefault("entry_price", 0.0)
        clean.setdefault("shares", 0.0)
        clean.setdefault("side", "UP")
        clean.setdefault("market_slug", "?")
        return cls(**clean)


class StateManager:
    VERSION = 1

    def __init__(self, path: str | Path = "data/state.json", *,
                 lock_path: str | Path | None = None,
                 backup_corrupt: bool = True):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.lock_path = Path(lock_path) if lock_path else Path(str(self.path) + ".lock")
        self.backup_corrupt = backup_corrupt
        self._thread_lock = threading.RLock()
        self._file_lock = FileLock(str(self.lock_path), timeout=15) if FileLock else None

    @contextmanager
    def _guard(self):
        """In-process lock, plus advisory file lock when the platform permits it.

        Some Windows sandboxes deny the reparse-point open used by filelock;
        falling back keeps the single-process daemon working.
        """
        with self._thread_lock:
            if self._file_lock is None:
                yield
                return
            try:
                with self._file_lock:
                    yield
            except PermissionError:
                LOG.warning("file lock unavailable (%s); using in-process lock only", self.lock_path)
                yield

    # ------------------------------------------------------------ internals
    def _empty(self) -> dict[str, Any]:
        return {"version": self.VERSION, "updated_at_ms": now_ms(), "positions": []}

    def _read_unlocked(self) -> dict[str, Any]:
        if not self.path.exists():
            return self._empty()
        try:
            raw = self.path.read_text(encoding="utf-8")
            data = json.loads(raw)
            if not isinstance(data, dict):
                raise ValueError("root is not an object")
            data.setdefault("version", self.VERSION)
            data.setdefault("positions", [])
            return data
        except Exception as exc:  # noqa: BLE001
            LOG.error("state.json corrupt: %s: %s", type(exc).__name__, exc)
            if self.backup_corrupt:
                backup = self.path.with_suffix(self.path.suffix + f".corrupt.{now_ms()}")
                try:
                    self.path.replace(backup)
                    LOG.error("corrupt state backed up to %s", backup)
                except OSError:
                    pass
            return self._empty()

    def _write_unlocked(self, data: dict[str, Any]) -> None:
        data["version"] = self.VERSION
        data["updated_at_ms"] = now_ms()
        tmp = self.path.with_suffix(self.path.suffix + ".tmp")
        with tmp.open("w", encoding="utf-8") as fh:
            json.dump(data, fh, ensure_ascii=False, indent=2)
            fh.flush()
            os.fsync(fh.fileno())
        os.replace(tmp, self.path)

    def _read(self) -> dict[str, Any]:
        with self._guard():
            return self._read_unlocked()

    def _mutate(self, fn):
        with self._guard():
            data = self._read_unlocked()
            result = fn(data)
            self._write_unlocked(data)
            return result

    # ------------------------------------------------------------- public
    def ensure_file(self) -> None:
        with self._guard():
            if not self.path.exists():
                self._write_unlocked(self._empty())

    def snapshot(self) -> dict[str, Any]:
        return self._read()

    def positions(self, statuses: Iterable[str] | None = None) -> list[Position]:
        data = self._read()
        out = [Position.from_dict(p) for p in data.get("positions", [])]
        if statuses is not None:
            allowed = set(statuses)
            out = [p for p in out if p.status in allowed]
        return sorted(out, key=lambda p: p.opened_at_ms)

    def active_positions(self) -> list[Position]:
        return self.positions(ACTIVE_STATUSES)

    def get(self, trade_id: str) -> Position | None:
        for p in self.positions():
            if p.trade_id == trade_id:
                return p
        return None

    def has_open_market(self, market_slug: str) -> bool:
        return any(p.market_slug == market_slug for p in self.active_positions())

    def add_position(self, pos: Position) -> bool:
        """Atomic, idempotent. False if trade/market already active."""
        def fn(data):
            positions = data.setdefault("positions", [])
            for raw in positions:
                if raw.get("trade_id") == pos.trade_id:
                    return False
                if raw.get("market_slug") == pos.market_slug and raw.get("status") in ACTIVE_STATUSES:
                    return False
            positions.append(pos.to_dict())
            return True
        ok = self._mutate(fn)
        if ok:
            LOG.info("[State] opened trade_id=%s market=%s side=%s price=%.4f shares=%.4f",
                     pos.trade_id, pos.market_slug, pos.side, pos.entry_price, pos.shares)
        else:
            LOG.warning("[State] duplicate ignored trade_id=%s market=%s", pos.trade_id, pos.market_slug)
        return bool(ok)

    def update_position(self, trade_id: str, **changes) -> bool:
        def fn(data):
            for raw in data.setdefault("positions", []):
                if raw.get("trade_id") == trade_id:
                    raw.update(changes)
                    return True
            return False
        ok = self._mutate(fn)
        if not ok:
            LOG.warning("[State] update missing trade_id=%s", trade_id)
        return bool(ok)

    def remove_position(self, trade_id: str) -> bool:
        def fn(data):
            positions = data.setdefault("positions", [])
            before = len(positions)
            data["positions"] = [p for p in positions if p.get("trade_id") != trade_id]
            return len(data["positions"]) != before
        return bool(self._mutate(fn))

    def mark_redeem_pending(self, trade_id: str) -> bool:
        return self.update_position(trade_id, status="redeem_pending", last_attempt_ms=now_ms())

    def mark_redeemed(self, trade_id: str, tx_hash: str, amount_usdc: float) -> bool:
        return self.update_position(trade_id, status="redeemed", tx_hash=tx_hash,
                                    redeemed_at_ms=now_ms(), amount_usdc=amount_usdc,
                                    last_error=None)

    def mark_lost(self, trade_id: str) -> bool:
        return self.update_position(trade_id, status="lost", last_error=None)

    def record_error(self, trade_id: str, error: str) -> bool:
        cur = self.get(trade_id)
        attempts = (cur.attempts if cur else 0) + 1
        return self.update_position(trade_id, attempts=attempts, last_error=error, last_attempt_ms=now_ms())

    def recover(self) -> list[Position]:
        self.ensure_file()
        active = self.active_positions()
        LOG.info("[State] recovered %d active position(s) from %s", len(active), self.path)
        for p in active:
            LOG.info("[State]   %s %s %s shares=%.4f status=%s end=%s",
                     p.trade_id, p.market_slug, p.side, p.shares, p.status, iso_utc(p.end_time_ms))
        return active


def new_trade_id(market_slug: str, side: str, ts_ms: int | None = None) -> str:
    return f"{market_slug}:{side.upper()}:{ts_ms or now_ms()}"
