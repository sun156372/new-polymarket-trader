def _as_bool(value: Any) -> bool:
    """Parse API booleans without letting string 'false' become truthy."""
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
        if s in {"1", "true", "t", "yes", "y", "winner_up", "winner-up", "up"}:
            return True
        if s in {"0", "false", "f", "no", "n", "winner_down", "winner-down", "down"}:
            return False
    return bool(value)


def _normalize_winner(value: Any) -> str | None:
    """Normalize Polymarket winner labels to UP/DOWN across API variants."""
    if value is None:
        return None
    if isinstance(value, bool):
        return "UP" if value else "DOWN"
    s = str(value).strip().lower()
    if s in {"up", "yes", "y", "1", "true", "winner_up", "winner-up"}:
        return "UP"
    if s in {"down", "no", "n", "0", "false", "winner_down", "winner-down"}:
        return "DOWN"
    return None


def _winner_from_outcomes_prices(outcomes: list[str], prices: list[float]) -> str | None:
    """Choose winner from outcome labels when the API only exposes price list."""
    if not outcomes or not prices:
        return None
    try:
        if len(outcomes) != len(prices):
            return None
        idx = max(range(len(prices)), key=lambda i: float(prices[i]))
        if float(prices[idx]) < 0.99:
            return None
        label = str(outcomes[idx]).strip().lower()
        if label in {"up", "yes", "yes_no", "true", "winner_up", "winner-up"}:
            return "UP"
        if label in {"down", "no", "false", "winner_down", "winner-down"}:
            return "DOWN"
    except Exception:  # noqa: BLE001
        return None
    return None
