"""
Minimum-variance portfolio optimizer (CVXPY).

Objective:
  minimize w^T Sigma w

Supports practical constraints via ConstraintSpec:
- fully invested (sum weights = 1)
- long-only
- max weight cap
- turnover constraint vs previous weights
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, Optional, Tuple, Union, List

import cvxpy as cp
import numpy as np
import pandas as pd

from investment_analytics.portfolio_construction.constraints import ConstraintSpec, build_constraints, constraint_checks


@dataclass(frozen=True)
class MinVarResult:
    weights: pd.Series
    status: str
    objective_value: float
    risk_variance: float
    diagnostics: Dict[str, Tuple[bool, float]]


def _ensure_psd(cov: np.ndarray, eps: float = 1e-10) -> np.ndarray:
    """
    Numerically stabilize covariance to be PSD-ish.
    This is not 'risk modeling'—it's a practical optimization guardrail.
    """
    cov = np.asarray(cov, dtype=float)
    cov = 0.5 * (cov + cov.T)  # symmetrize
    # Add tiny ridge to diagonal to avoid singularities
    cov = cov + eps * np.eye(cov.shape[0])
    return cov


def solve_min_variance(
    cov: Union[pd.DataFrame, np.ndarray],
    assets: Optional[List[str]] = None,
    spec: Optional[ConstraintSpec] = None,
    w_prev: Optional[Union[pd.Series, np.ndarray]] = None,
    solver: str = "OSQP",
    verbose: bool = False,
) -> MinVarResult:
    """
    Solve minimum variance portfolio.

    Parameters
    ----------
    cov:
        Covariance matrix (N x N). Prefer a pd.DataFrame with labeled columns/index.
    assets:
        Optional asset names if cov is a numpy array.
    spec:
        ConstraintSpec controlling constraints. Defaults to (fully invested, long-only).
    w_prev:
        Previous weights, required if spec.turnover_limit is set.
        Can be pd.Series indexed by asset name or numpy array length N.
    solver:
        CVXPY solver. OSQP works well for convex QPs with linear constraints.
        ECOS/SCS also work, and some constraints may require different solvers later.
    """
    if spec is None:
        spec = ConstraintSpec()

    # Normalize covariance and asset naming
    if isinstance(cov, pd.DataFrame):
        cov_df = cov.copy()
        if cov_df.shape[0] != cov_df.shape[1]:
            raise ValueError("cov must be square (N x N)")
        assets_use = list(cov_df.columns)
        cov_mat = cov_df.to_numpy(dtype=float)
    else:
        cov_mat = np.asarray(cov, dtype=float)
        if cov_mat.ndim != 2 or cov_mat.shape[0] != cov_mat.shape[1]:
            raise ValueError("cov must be a square 2D array (N x N)")
        if assets is None:
            assets_use = [f"asset_{i}" for i in range(cov_mat.shape[0])]
        else:
            if len(assets) != cov_mat.shape[0]:
                raise ValueError("len(assets) must match covariance dimension")
            assets_use = list(assets)

    n = cov_mat.shape[0]
    cov_mat = _ensure_psd(cov_mat)

    # Align w_prev (if provided) to asset ordering
    w_prev_vec: Optional[np.ndarray] = None
    if w_prev is not None:
        if isinstance(w_prev, pd.Series):
            w_prev_vec = w_prev.reindex(assets_use).to_numpy(dtype=float)
        else:
            w_prev_vec = np.asarray(w_prev, dtype=float).reshape(-1)
        if w_prev_vec.shape[0] != n:
            raise ValueError("w_prev must have length N to match cov dimension")

    # CVXPY variable
    w = cp.Variable(n)

    # Objective: minimize variance = w' Σ w
    risk = cp.quad_form(w, cov_mat)
    objective = cp.Minimize(risk)

    # Constraints
    constraints = build_constraints(w, spec=spec, w_prev=w_prev_vec)

    # Solve
    prob = cp.Problem(objective, constraints)
    prob.solve(solver=solver, verbose=verbose)

    if prob.status not in ("optimal", "optimal_inaccurate"):
        raise RuntimeError(f"Min-variance optimization failed: {prob.status}")

    w_opt = np.array(w.value, dtype=float).reshape(-1)

    # Post-solve diagnostics
    checks = constraint_checks(w_opt, spec=spec, w_prev=w_prev_vec)

    # Compute realized variance (should equal objective value within tolerance)
    realized_var = float(w_opt.T @ cov_mat @ w_opt)

    weights = pd.Series(w_opt, index=assets_use, name="weight")

    return MinVarResult(
        weights=weights,
        status=str(prob.status),
        objective_value=float(prob.value),
        risk_variance=realized_var,
        diagnostics=checks,
    )