"""
Reusable CVXPY constraint builders for portfolio construction.

Design goals:
- Composable constraints (each function returns a List[cp.Constraint])
- Clear validation helpers for post-solve diagnostics
- Practical constraints: long-only, max weight, turnover, etc.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List, Optional, Tuple

import cvxpy as cp
import numpy as np


@dataclass(frozen=True)
class ConstraintSpec:
    """Human-readable settings captured alongside a solve."""
    fully_invested: bool = True
    long_only: bool = True
    max_weight: Optional[float] = None
    min_weight: Optional[float] = None  # rarely used; keep for completeness
    turnover_limit: Optional[float] = None  # L1 turnover bound vs w_prev
    weight_sum: float = 1.0               # usually 1.0


def fully_invested(w: cp.Expression, weight_sum: float = 1.0) -> List[cp.Constraint]:
    """Enforce sum(w) == weight_sum."""
    return [cp.sum(w) == float(weight_sum)]


def long_only(w: cp.Expression) -> List[cp.Constraint]:
    """Enforce w >= 0."""
    return [w >= 0]


def max_position_size(w: cp.Expression, max_weight: float) -> List[cp.Constraint]:
    """Enforce w_i <= max_weight for all i."""
    if max_weight <= 0:
        raise ValueError("max_weight must be > 0")
    return [w <= float(max_weight)]


def min_position_size(w: cp.Expression, min_weight: float) -> List[cp.Constraint]:
    """Enforce w_i >= min_weight for all i (uncommon; use carefully)."""
    if min_weight < 0:
        raise ValueError("min_weight must be >= 0")
    return [w >= float(min_weight)]


def turnover_l1(w: cp.Expression, w_prev: np.ndarray, turnover_limit: float) -> List[cp.Constraint]:
    """
    Enforce L1 turnover: ||w - w_prev||_1 <= turnover_limit

    turnover_limit examples:
    - 0.10 means total absolute turnover <= 10% (buy+sell)
    """
    if turnover_limit < 0:
        raise ValueError("turnover_limit must be >= 0")
    w_prev = np.asarray(w_prev, dtype=float).reshape(-1)
    return [cp.norm1(w - w_prev) <= float(turnover_limit)]


def build_constraints(
    w: cp.Expression,
    spec: ConstraintSpec,
    w_prev: Optional[np.ndarray] = None,
) -> List[cp.Constraint]:
    """
    Build a list of CVXPY constraints based on a ConstraintSpec.
    """
    constraints: List[cp.Constraint] = []

    if spec.fully_invested:
        constraints += fully_invested(w, weight_sum=spec.weight_sum)

    if spec.long_only:
        constraints += long_only(w)

    if spec.max_weight is not None:
        constraints += max_position_size(w, spec.max_weight)

    if spec.min_weight is not None:
        constraints += min_position_size(w, spec.min_weight)

    if spec.turnover_limit is not None:
        if w_prev is None:
            raise ValueError("w_prev is required when turnover_limit is set.")
        constraints += turnover_l1(w, w_prev=w_prev, turnover_limit=spec.turnover_limit)

    return constraints


# -------------------------
# Post-solve diagnostics
# -------------------------

def l1_turnover(w: np.ndarray, w_prev: np.ndarray) -> float:
    """Compute realized L1 turnover = sum(|w - w_prev|)."""
    w = np.asarray(w, dtype=float).reshape(-1)
    w_prev = np.asarray(w_prev, dtype=float).reshape(-1)
    return float(np.sum(np.abs(w - w_prev)))


def constraint_checks(
    w: np.ndarray,
    spec: ConstraintSpec,
    w_prev: Optional[np.ndarray] = None,
    atol: float = 1e-6,
) -> Dict[str, Tuple[bool, float]]:
    """
    Simple numeric checks to validate a solved weight vector.

    Returns a dict:
      key -> (is_ok, metric_value)

    metric_value is:
      - sum weights (for fully invested)
      - min weight (for long-only)
      - max weight (for max_weight constraint)
      - turnover (for turnover constraint)
    """
    w = np.asarray(w, dtype=float).reshape(-1)
    out: Dict[str, Tuple[bool, float]] = {}

    if spec.fully_invested:
        s = float(np.sum(w))
        ok = abs(s - spec.weight_sum) <= atol
        out["fully_invested_sum"] = (ok, s)

    if spec.long_only:
        m = float(np.min(w))
        ok = m >= -atol
        out["long_only_min_weight"] = (ok, m)

    if spec.max_weight is not None:
        mx = float(np.max(w))
        ok = mx <= spec.max_weight + atol
        out["max_weight_max_seen"] = (ok, mx)

    if spec.min_weight is not None:
        mn = float(np.min(w))
        ok = mn >= spec.min_weight - atol
        out["min_weight_min_seen"] = (ok, mn)

    if spec.turnover_limit is not None:
        if w_prev is None:
            raise ValueError("w_prev required for turnover check.")
        t = l1_turnover(w, w_prev)
        ok = t <= spec.turnover_limit + atol
        out["turnover_l1"] = (ok, t)

    return out