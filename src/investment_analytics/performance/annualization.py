
"""Annualization of cumulative investment returns."""

from decimal import Decimal, InvalidOperation


def annualize_return(
    cumulative_return: Decimal | float | int,
    years: Decimal | float | int,
) -> Decimal:
    """
    Convert a cumulative return into an annualized return.

    Formula:
        R_annualized = (1 + R_cumulative) ** (1 / years) - 1

    Parameters
    ----------
    cumulative_return:
        Cumulative return in decimal form.
        Example: 0.21 represents +21%.

    years:
        Measurement period expressed in years.

    Returns
    -------
    Decimal
        Annualized return in decimal form.
    """
    try:
        r = Decimal(str(cumulative_return))
        n = Decimal(str(years))
    except (InvalidOperation, ValueError, TypeError):
        raise ValueError("Invalid return or measurement period.") from None

    if not r.is_finite() or not n.is_finite():
        raise ValueError("Inputs must be finite.")

    if n <= 0:
        raise ValueError("Measurement period must be positive.")

    if r < -1:
        raise ValueError("Cumulative return cannot be below -100%.")

    return (1 + r) ** (1 / n) - 1
