src/investment_analytics/portfolio_construction/README.md.

⸻

Portfolio Construction

This module contains portfolio construction and optimization routines used to translate risk, return, and constraint assumptions into investable portfolios.

The goal is to provide clear, extensible, and production-quality implementations of core portfolio construction problems commonly used in institutional investment workflows.

Scope

The portfolio construction section focuses on optimization and constraint logic, not forecasting or risk modeling. Inputs such as expected returns or covariance matrices are assumed to be provided by upstream analytics.

Current and planned examples include:
	•	Minimum Variance Optimization
	•	Long-only, fully invested portfolios
	•	Practical constraints (position limits, turnover)
	•	Mean–Variance Optimization (planned)
	•	Tracking Error Minimization (planned)
	•	Benchmark-relative and constrained portfolios (planned)

Design Principles
	•	Separation of concerns
	•	Optimization logic is isolated from data sourcing and estimation.
	•	Explicit constraints
	•	All portfolio constraints are defined in reusable, composable form.
	•	Numerical robustness
	•	Guardrails are included to handle real-world covariance issues.
	•	Interpretability
	•	Outputs are labeled, validated, and accompanied by diagnostics.

Intended Use

These routines are designed to support:
	•	research notebooks
	•	backtests
	•	production pipelines
	•	dashboards (e.g. Streamlit)

They are not meant to be “black box” optimizers, but transparent tools for understanding how portfolio assumptions translate into allocations.

⸻