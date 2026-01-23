import numpy as np
import pandas as pd

from investment_analytics.portfolio_construction.constraints import ConstraintSpec
from investment_analytics.portfolio_construction.optimizers.min_variance import solve_min_variance

# Fake returns -> cov
np.random.seed(0)
assets = ["TOST", "CPNG", "NU", "AAPL", "TSM"]
R = np.random.normal(0, 0.02, size=(504, len(assets)))  # ~2y daily returns
cov = pd.DataFrame(np.cov(R, rowvar=False), index=assets, columns=assets)

w_prev = pd.Series([0.25, 0.25, 0.20, 0.20, 0.10], index=assets)

spec = ConstraintSpec(
    fully_invested=True,
    long_only=True,
    max_weight=0.35,
    turnover_limit=0.30
)

res = solve_min_variance(cov=cov, spec=spec, w_prev=w_prev)

print(res.weights.sort_values(ascending=False))
print(res.status, res.objective_value, res.risk_variance)
print(res.diagnostics)