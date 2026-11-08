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
import math
import threading
import time
from dataclasses import dataclass, field
from typing import Iterable

LOG = logging.getLogger("v7.analytics")

HEARTBEAT_SECONDS = 60.0


def _is_official_source(source: object) -> bool:
    """True only for a winner obtained from Polymarket.

    Local spot close is a deliberate emergency fallback and must never be
    counted in the headline win rate.
    """
    value = str(source or "").strip().lower()
    if not value:
        return False
    return not value.startswith("local")


@dataclass
class Analytics:
    starting_balance: float
    balance: float
    mode: str = "paper"
    heartbeat_s: float = HEARTBEAT_SECONDS

    trades: int = 0          # settled trades (WIN/LOSS)
    opened: int = 0          # total entries, including still-open positions
    wins: int = 0            # official settled wins (local fallback excluded)
    losses: int = 0          # official settled losses
    official_trades: int = 0 # settled only from Polymarket official result
    official_wins: int = 0
    official_losses: int = 0
    local_settled: int = 0   # emergency fallback, shown separately
    local_pnl: float = 0.0
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

    def rebuild_from_history(
        self,
        rows: Iterable[dict],
        *,
        starting_balance: float | None = None,
    ) -> int:
        """Replay settled state.json history into the dashboard.

        Only WIN/LOSS rows are counted.  OPEN rows are skipped because the
        startup recovery path (`reconcile_recovered_paper`) debits their cash
        cost exactly once.  Returns the number of settled rows replayed.

        Balance is reconstructed as:
            starting_balance + sum(realized pnl)
        which is the cash left after all settled entries and payouts.  Open
        positions are deliberately not included here.
        """
        base = float(self.starting_balance if starting_balance is None else starting_balance)
        if not math.isfinite(base):
            raise ValueError(f"invalid starting balance: {starting_balance!r}")

        closed: list[dict] = []
        opened = 0
        for row in rows:
            if not isinstance(row, dict):
                continue
            status = str(row.get("status", "")).upper()
            if status not in {"OPEN", "WIN", "LOSS"}:
                continue
            opened += 1
            if status not in {"WIN", "LOSS"}:
                continue
            try:
                pnl = float(row.get("pnl"))
                payout = float(row.get("payout") or 0.0)
                cost = float(row.get("cost_usdc") or row.get("stake") or 0.0)
                settled_ms = int(
                    row.get("settled_at_ms")
                    or row.get("timestamp")
                    or time.time() * 1000
                )
            except (TypeError, ValueError):
                continue
            if not (math.isfinite(pnl) and math.isfinite(payout) and math.isfinite(cost)):
                continue
            source = str(row.get("resolution_source") or row.get("source") or "")
            closed.append({
                "status": status,
                "market": str(row.get("market_id") or row.get("market") or "?"),
                "side": str(row.get("side") or "?"),
                "pnl": pnl,
                "payout": payout,
                "cost": max(0.0, cost),
                "settled_ms": settled_ms,
                "source": source,
                "official": _is_official_source(source),
            })
        closed.sort(key=lambda r: r["settled_ms"])

        today = time.strftime("%Y-%m-%d", time.gmtime())
        with self._lock:
            self.starting_balance = base
            self.balance = base
            self.trades = 0
            self.opened = opened
            self.wins = 0
            self.losses = 0
            self.official_trades = 0
            self.official_wins = 0
            self.official_losses = 0
            self.local_settled = 0
            self.local_pnl = 0.0
            self.total_staked = 0.0
            self.total_pnl = 0.0
            self.peak_equity = base
            self.max_drawdown = 0.0
            self.daily_pnl = 0.0
            self.daily_trades = 0
            self.daily_date = today
            self.history = []
            self.last_equity = base
            self._dirty = True

            for rec in closed:
                won = rec["status"] == "WIN"
                if rec["official"]:
                    self.trades += 1
                    self.wins += int(won)
                    self.losses += int(not won)
                    self.official_trades += 1
                    self.official_wins += int(won)
                    self.official_losses += int(not won)
                    self.total_staked += rec["cost"]
                    self.total_pnl += rec["pnl"]
                else:
                    self.local_settled += 1
                    self.local_pnl += rec["pnl"]
                self.balance += rec["pnl"]
                self.peak_equity = max(self.peak_equity, self.balance)
                self.max_drawdown = min(
                    self.max_drawdown, self.balance - self.peak_equity
                )
                day = time.strftime("%Y-%m-%d", time.gmtime(rec["settled_ms"] / 1000.0))
                if day == today:
                    if rec["official"]:
                        self.daily_pnl += rec["pnl"]
                    self.daily_trades += 1
                self.history.append({
                    "ts": rec["settled_ms"] / 1000.0,
                    "market": rec["market"],
                    "side": rec["side"],
                    "stake": rec["cost"],
                    "pnl": rec["pnl"],
                    "payout": rec["payout"],
                    "won": won,
                    "balance": self.balance,
                })
            self.last_equity = self.balance
        return len(closed)

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
        return self.official_wins / self.official_trades if self.official_trades else 0.0

    def note_entry(self) -> None:
        """Count one new entry for the total-open side of the panel."""
        with self._lock:
            self.opened += 1
            self._dirty = True

    def note_position_change(self) -> None:
        """Mark state dirty; next maybe_print() emits one line."""
        with self._lock:
            self._dirty = True

    def debit_entry(self, cost: float) -> None:
        """Deduct actual paper cash paid at entry (principal + fee).

        The executor already deducted its own cash balance.  This keeps the
        dashboard's balance in sync instead of showing stale starting cash
        until the position settles.
        """
        amount = float(cost)
        if not math.isfinite(amount) or amount < 0:
            raise ValueError(f"invalid entry cost: {cost!r}")
        with self._lock:
            self.balance -= amount
            self.last_equity = self.balance
            self.max_drawdown = min(self.max_drawdown, self.balance - self.peak_equity)
            self._dirty = True

    # ---------------------------------------------------------------- event
    def record(
        self,
        *,
        pnl: float,
        stake: float,
        won: bool,
        market: str,
        side: str,
        payout: float | None = None,
        source: str | None = None,
    ) -> None:
        """Record one settled trade.

        ``source`` is the resolution source stored in state history.  Only
        Polymarket official results contribute to ``win_rate``; local spot
        fallback is still booked in cash/PnL but excluded from the headline
        official win rate.
        """
        pnl_f = float(pnl)
        payout_f = pnl_f if payout is None else float(payout)
        official = _is_official_source(source)
        if not math.isfinite(pnl_f) or not math.isfinite(payout_f):
            raise ValueError(f"invalid settlement: pnl={pnl!r} payout={payout!r}")
        with self._lock:
            self._roll_day()
            self.daily_trades += 1
            if official:
                self.trades += 1
                self.wins += int(won)
                self.losses += int(not won)
                self.official_trades += 1
                self.official_wins += int(won)
                self.official_losses += int(not won)
                self.total_staked += float(stake)
                self.total_pnl += pnl_f
                self.daily_pnl += pnl_f
            else:
                self.local_settled += 1
                self.local_pnl += pnl_f
            # Cash balance changes by the returned payout, not by PnL: the
            # entry cost was already removed by debit_entry().
            self.balance += payout_f
            self.peak_equity = max(self.peak_equity, self.balance)
            self.max_drawdown = min(self.max_drawdown, self.balance - self.peak_equity)
            self.last_equity = self.balance
            self._dirty = True
            self.history.append({
                "ts": time.time(), "market": market, "side": side,
                "stake": stake, "pnl": pnl_f, "payout": payout_f,
                "won": won, "balance": self.balance,
                "source": str(source or ""),
            })
            self._emit_locked(force=True)

    # --------------------------------------------------------------- output
    def _format(self) -> str:
        local = (
            f" local={self.local_pnl:+.4f} ({self.local_settled} fallback)"
            if self.local_settled
            else ""
        )
        return (
            f"{self.tag} balance={self.balance:.4f} pnl={self.total_pnl:+.4f} "
            f"daily={self.daily_pnl:+.4f} "
            f"trades={self.opened}/{self.trades} "
            f"win={self.win_rate * 100:.2f}% "
            f"({self.official_wins}/{self.official_trades} official) "
            f"mdd={self.max_drawdown:.4f}{local}"
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
            "opened": self.opened,
            "settled": self.trades,
            "wins": self.wins,
            "losses": self.losses,
            "official_trades": self.official_trades,
            "official_wins": self.official_wins,
            "official_losses": self.official_losses,
            "local_trades": self.local_settled,
            "local_pnl": round(self.local_pnl, 6),
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

