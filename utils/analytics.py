"""Real-time paper/live performance analytics.

Tracks trades, wins, equity, peak equity and max drawdown.
Thread-safe enough for the single asyncio event loop; no external deps.
"""
from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field

LOG = logging.getLogger("v7.analytics")


@dataclass
class Analytics:
    starting_balance: float
    balance: float
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
    history: list[dict] = field(default_factory=list)

    def __post_init__(self) -> None:
        self.peak_equity = max(self.peak_equity, self.balance)
        self.daily_date = time.strftime("%Y-%m-%d", time.gmtime())

    def _roll_day(self) -> None:
        today = time.strftime("%Y-%m-%d", time.gmtime())
        if today != self.daily_date:
            self.daily_date = today
            self.daily_pnl = 0.0
            self.daily_trades = 0

    @property
    def win_rate(self) -> float:
        return self.wins / self.trades if self.trades else 0.0

    def record(self, *, pnl: float, stake: float, won: bool, market: str, side: str) -> None:
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
        dd = self.balance - self.peak_equity
        self.max_drawdown = min(self.max_drawdown, dd)
        self.history.append({
            "ts": time.time(), "market": market, "side": side,
            "stake": stake, "pnl": pnl, "won": won, "balance": self.balance,
        })
        LOG.info("[Analytics] trade=%s side=%s pnl=%+.4f balance=%.4f win_rate=%.2f%% mdd=%.4f",
                 market, side, pnl, self.balance, self.win_rate * 100, self.max_drawdown)

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

    def print_status(self) -> None:
        s = self.snapshot()
        LOG.info(
            "[Live] balance=%.4f pnl=%+.4f daily=%+.4f win=%.2f%% (%d/%d) mdd=%.4f",
            s["balance"], s["total_pnl"], s["daily_pnl"], s["win_rate"] * 100,
            s["wins"], s["trades"], s["max_drawdown"],
        )
