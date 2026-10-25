#!/usr/bin/env python3
"""Real-time paper/live analytics with strict log throttling.

Key behavior:
  * The log tag is [Paper] or [Live], based on the configured mode.
  * print_status() / maybe_print() never log more than once per 60 seconds
    unless a trade/PnL change explicitly forces one line.
  * record() emits at most one line per closed trade.
  * Safe to call from a tight async loop: calls without changes are no-ops.

Config integration:
    analytics = Analytics(..., mode=cfg.get("mode", "paper"))
    analytics.maybe_print()          # call every loop; internal throttle
    analytics.note_position_change() # call after entry/exit if desired
"""
from __future__ import annotations

import logging
import threading
import time
from dataclasses import dataclass, field

LOG = logging.getLogger("v7.analytics")

HEARTBEAT_SECONDS = 60.0


@dataclass
class Analytics:
    starting_balance: float
    balance: float
    mode: str = "paper"
    heartbeat_s: float = HEARTBEAT_SECONDS

    trades: int = 0
    wins: int = 0
    losses: int = 0
    total_staked: float = 0.0
    total_pnl: float = 0.0
    peak_equity: float = 0.0
    max_drawdown: float = 0.0
    daily_pnl: float = 0.0
    daily_trades: int = 0
    daily_date: str = ""

    last_print_monotonic: float = 0.0
    last_equity: float = 0.0
    _dirty: bool = True
    _lock: threading.Lock = field(default_factory=threading.Lock, repr=False)

    def __post_init__(self) -> None:
        self.mode = str(self.mode or "paper").strip().lower()
        self.heartbeat_s = max(1.0, float(self.heartbeat_s))
        self.peak_equity = max(self.peak_equity, self.balance)
        self.last_equity = self.balance
        self.daily_date = time.strftime("%Y-%m-%d", time.gmtime())

    @classmethod
    def from_config(cls, cfg: dict, *, starting_balance: float, balance: float) -> "Analytics":
        a = cfg.get("analytics", {})
        return cls(
            starting_balance=starting_balance,
            balance=balance,
            mode=str(cfg.get("mode", "paper")),
            heartbeat_s=float(a.get("heartbeat_s", HEARTBEAT_SECONDS)),
        )

    # ------------------------------------------------------------------ mode
    @property
    def tag(self) -> str:
        return "[Paper]" if self.mode == "paper" else "[Live]"

    def set_mode(self, mode: str) -> None:
        with self._lock:
            self.mode = str(mode or "paper").strip().lower()

    # -------------------------------------------------------------- helpers
    def _roll_day(self) -> None:
        today = time.strftime("%Y-%m-%d", time.gmtime())
        if today != self.daily_date:
            self.daily_date = today
            self.daily_pnl = 0.0
            self.daily_trades = 0

    @property
    def win_rate(self) -> float:
        return self.wins / self.trades if self.trades else 0.0

    def note_position_change(self) -> None:
        """Mark state dirty; next maybe_print() emits one line."""
        with self._lock:
            self._dirty = True

    # ---------------------------------------------------------------- event
    def record(self, *, pnl: float, stake: float, won: bool, market: str, side: str) -> None:
        with self._lock:
            self._roll_day()
            self.trades += 1
            self.daily_trades += 1
            self.wins += int(won)
            self.losses += int(not won)
            self.total_staked += stake
            self.total_pnl += pnl
            self.balance += pnl
            self.daily_pnl += pnl
            self.peak_equity = max(self.peak_equity, self.balance)
            self.max_drawdown = min(self.max_drawdown, self.balance - self.peak_equity)
            self.last_equity = self.balance
            self._dirty = True
            self.history.append({
                "ts": time.time(), "market": market, "side": side,
                "stake": stake, "pnl": pnl, "won": won, "balance": self.balance,
            })
            self._emit_locked(force=True)

    # --------------------------------------------------------------- output
    def _format(self) -> str:
        return (
            f"{self.tag} balance={self.balance:.4f} pnl={self.total_pnl:+.4f} "
            f"daily={self.daily_pnl:+.4f} trades={self.trades}/{self.daily_trades} "
            f"win={self.win_rate * 100:.2f}% ({self.wins}/{self.trades}) "
            f"mdd={self.max_drawdown:.4f}"
        )

    def _emit_locked(self, *, force: bool) -> bool:
        now = time.monotonic()
        due = (now - self.last_print_monotonic) >= self.heartbeat_s
        changed = self._dirty
        if not force and not due and not changed:
            return False
        LOG.info(self._format())
        self.last_print_monotonic = now
        self._dirty = False
        return True

    def print_status(self, force: bool = False) -> bool:
        """Compatibility method. Throttled unless force=True."""
        with self._lock:
            return self._emit_locked(force=force)

    def maybe_print(self) -> bool:
        """Call every loop. Emits only on 60s heartbeat or dirty state."""
        with self._lock:
            return self._emit_locked(force=False)

    # ------------------------------------------------------------------ risk
    def can_trade(self, *, max_daily_loss: float, active_positions: int,
                  max_positions: int, max_trades_per_day: int = 40) -> tuple[bool, str]:
        self._roll_day()
        if active_positions >= max_positions:
            return False, "max_concurrent_positions"
        if self.daily_trades >= max_trades_per_day:
            return False, "max_trades_per_day"
        if self.daily_pnl <= -abs(max_daily_loss):
            return False, "daily_max_loss"
        if self.balance <= 0:
            return False, "zero_balance"
        return True, "ok"

    def snapshot(self) -> dict:
        return {
            "mode": self.mode,
            "tag": self.tag,
            "balance": round(self.balance, 6),
            "starting_balance": self.starting_balance,
            "trades": self.trades,
            "wins": self.wins,
            "losses": self.losses,
            "win_rate": round(self.win_rate, 6),
            "total_staked": round(self.total_staked, 6),
            "total_pnl": round(self.total_pnl, 6),
            "daily_pnl": round(self.daily_pnl, 6),
            "daily_trades": self.daily_trades,
            "peak_equity": round(self.peak_equity, 6),
            "max_drawdown": round(self.max_drawdown, 6),
        }

    # history is intentionally last so dataclass field ordering is simple
    history: list[dict] = field(default_factory=list)

