
"""Geometric linking of periodic investment returns."""

from collections.abc import Iterable
from decimal import Decimal, InvalidOperation


def geometric_link(
    period_returns: Iterable[Decimal | float | int],
) -> Decimal:
    """
    Geometrically link periodic investment returns.

    Formula:
        R = product(1 + r_t) - 1

    Parameters
    ----------
    period_returns:
        Decimal returns, not percentages.
        Example: 0.05 represents +5%.

    Returns
    -------
    Decimal
        Geometrically linked return.

    Notes
    -----
    - Missing or non-finite returns raise ValueError.
    - Returns below -100% are rejected.
    - Empty periods raise ValueError rather than returning 0%.
    - Valuation-status checks belong in the performance layer
      that selects the observations.
    """
    growth_factor = Decimal("1")
    count = 0

    for value in period_returns:
        try:
            r = Decimal(str(value))
        except (InvalidOperation, ValueError):
            raise ValueError(f"Invalid return: {value!r}") from None

        if not r.is_finite():
            raise ValueError(f"Non-finite return: {value!r}")

        if r < -1:
            raise ValueError(f"Return below -100%: {value!r}")

        growth_factor *= 1 + r
        count += 1

    if count == 0:
        raise ValueError("No returns provided for linking.")

    return growth_factor - 1
