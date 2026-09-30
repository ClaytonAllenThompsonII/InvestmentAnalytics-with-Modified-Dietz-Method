#!/usr/bin/env python3
"""Fetch Alpha Vantage earnings estimates for Toast (TOST)."""

import json
import os
import sys

import requests
from dotenv import load_dotenv


API_URL = "https://www.alphavantage.co/query"


def main() -> None:
    # Load variables from the project's .env file.
    load_dotenv()

    api_key = (
        os.getenv("ALPHAVANTAGE_PREMIUM_API_KEY")
        or os.getenv("ALPHAVANTAGE_API_KEY")
    )
    if not api_key:
        sys.exit(
            "API key not found. Add ALPHAVANTAGE_PREMIUM_API_KEY to your .env file."
        )

    params = {
        "function": "EARNINGS_ESTIMATES",
        "symbol": "TOST",
        "apikey": api_key,
    }

    try:
        response = requests.get(API_URL, params=params, timeout=30)
        response.raise_for_status()
        data = response.json()
    except (requests.RequestException, json.JSONDecodeError) as exc:
        sys.exit(f"Request failed: {exc}")

    for error_field in ("Error Message", "Information", "Note"):
        if error_field in data:
            sys.exit(f"Alpha Vantage returned: {data[error_field]}")

    print(json.dumps(data, indent=2))


if __name__ == "__main__":
    main()
