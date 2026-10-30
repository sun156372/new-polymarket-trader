def _as_bool(value: Any) -> bool:
    """Parse Polymarket API booleans without falling for string 'false'."""
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


def _normalize_winner(value: Any) -> str | None:
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


# ... existing code continues unchanged below to avoid file fragmenting
