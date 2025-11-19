"""Functions for matching broker names against user‑defined targets."""

from typing import Any, Dict, Tuple, Optional


def _normalize(text: str) -> str:
    """Normalize broker names by removing whitespace and converting to lowercase."""
    return text.lower().replace(" ", "").strip()


def match_target_broker(
    broker_name: str, broker_config: Dict[str, Any], buy_ratio: Optional[float] = None
) -> Tuple[bool, Dict[str, Any]]:
    """Check whether a broker matches one of the user‑defined targets.

    Args:
        broker_name: Name of the broker as found on the detail page.
        broker_config: The loaded configuration from brokers.yaml.
        buy_ratio: The buy ratio (between 0 and 1) if available.

    Returns:
        (matches, target_meta) where matches is True if the broker should
        trigger an alert, and target_meta is the matched config dict.
    """
    norm_name = _normalize(broker_name)
    for target in broker_config.get("targets", []):
        t_name = target.get("name")
        if t_name and _normalize(t_name) == norm_name:
            # Check optional ratio threshold
            max_ratio = target.get("max_ratio")
            if max_ratio is not None and buy_ratio is not None:
                try:
                    max_ratio_float = float(max_ratio)
                    if buy_ratio > max_ratio_float:
                        return False, target
                except Exception:
                    # if conversion fails, ignore ratio filter
                    pass
            return True, target
        # If the target specifies a code, you can add code comparison here
        # once your sources provide broker codes.
    return False, {}