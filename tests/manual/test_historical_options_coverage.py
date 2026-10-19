# Test: Historical Options Coverage
#
# Purpose:
# Evaluate Alpha Vantage historical options coverage
# across multiple securities and historical periods.
#
# No database writes are performed.

import os
import requests

from dotenv import load_dotenv


load_dotenv()

API_KEY = os.getenv("ALPHAVANTAGE_PREMIUM_API_KEY")

if not API_KEY:
    raise RuntimeError("Alpha Vantage API key not found.")

BASE_URL = "https://www.alphavantage.co/query"


# --------------------------------------------------
# Historical test contracts
# --------------------------------------------------

TEST_CONTRACTS = [
    {
        "symbol": "SQ",
        "contract": "SQ200821P00135000",
        "date": "2020-08-13",
    },
    {
        "symbol": "BHIL",
        "contract": "BHIL220218C00015000",
        "date": "2022-02-17",
    },
    {
        "symbol": "AAPL",
        "contract": "AAPL230616P00130000",
        "date": "2023-04-06",
    },
    {
        "symbol": "NU",
        "contract": "NU250620P00012000",
        "date": "2025-05-09",
    },
]


# --------------------------------------------------
# API request
# --------------------------------------------------

def fetch_historical_option(symbol, contract, date):

    params = {
        "function": "HISTORICAL_OPTIONS",
        "symbol": symbol,
        "contract": contract,
        "date": date,
        "apikey": API_KEY,
    }

    response = requests.get(
        BASE_URL,
        params=params,
        timeout=60,
    )

    # Avoid printing a request URL containing the API key.
    if response.status_code != 200:
        raise RuntimeError(
            f"Alpha Vantage returned HTTP {response.status_code}"
        )

    data = response.json()

    for key in ("Error Message", "Information", "Note"):
        if key in data:
            raise RuntimeError(f"{key}: {data[key]}")

    return data.get("data", [])


# --------------------------------------------------
# Historical coverage test
# --------------------------------------------------

def test_historical_coverage():

    print("\n=== HISTORICAL OPTIONS COVERAGE TEST ===")

    successful = 0

    for test in TEST_CONTRACTS:

        symbol = test["symbol"]
        contract = test["contract"]
        date = test["date"]

        print("\n" + "-" * 55)

        print(f"Underlying: {symbol}")
        print(f"Contract: {contract}")
        print(f"Date: {date}")

        try:
            records = fetch_historical_option(
                symbol=symbol,
                contract=contract,
                date=date,
            )

        except Exception as exc:
            print(f"API ERROR: {type(exc).__name__}")
            continue

        matching = [
            record
            for record in records
            if record.get("contractID") == contract
            and record.get("date") == date
        ]

        if not matching:
            print("Result: NO MATCHING DATA")
            print(f"Records returned: {len(records)}")
            continue

        record = matching[0]

        successful += 1

        print("Result: SUCCESS")

        print(f"Last: {record.get('last')}")
        print(f"Bid: {record.get('bid')}")
        print(f"Ask: {record.get('ask')}")
        print(f"Mark: {record.get('mark')}")

        print(f"Volume: {record.get('volume')}")
        print(f"Open Interest: {record.get('open_interest')}")

        print(
            f"Implied Volatility: "
            f"{record.get('implied_volatility')}"
        )

    print("\n" + "=" * 55)

    print("HISTORICAL COVERAGE SUMMARY")
    print(f"Contracts tested: {len(TEST_CONTRACTS)}")
    print(f"Contracts found: {successful}")
    print(f"Contracts not confirmed: {len(TEST_CONTRACTS) - successful}")


if __name__ == "__main__":
    test_historical_coverage()