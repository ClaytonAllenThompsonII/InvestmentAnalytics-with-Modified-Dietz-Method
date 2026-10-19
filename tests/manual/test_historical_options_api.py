import os
import requests
from collections import Counter


from dotenv import load_dotenv

load_dotenv()

API_KEY = os.getenv("ALPHAVANTAGE_PREMIUM_API_KEY")

if not API_KEY:
    raise RuntimeError("Alpha Vantage API key not found.")

BASE_URL = "https://www.alphavantage.co/query"

SYMBOL = "TOST"
CONTRACT = "TOST260918P00027000"

DATES = [
    "2026-07-10",
    "2026-07-13",
    "2026-07-14",
]


def fetch_options(date, contract=None):

    params = {
        "function": "HISTORICAL_OPTIONS",
        "symbol": SYMBOL,
        "date": date,
        "apikey": API_KEY,
    }

    if contract:
        params["contract"] = contract

    response = requests.get(
        BASE_URL,
        params=params,
        timeout=60,
    )

    response.raise_for_status()

    data = response.json()

    for key in ("Error Message", "Information", "Note"):
        if key in data:
            raise RuntimeError(f"{key}: {data[key]}")

    return data, len(response.content)


def test_historical_consistency():

    print("\n=== TEST 2: HISTORICAL CONSISTENCY ===")

    for date in DATES:

        data, size = fetch_options(
            date=date,
            contract=CONTRACT,
        )

        records = data.get("data", [])

        if not records:
            print(f"{date}: NO DATA")
            continue

        record = records[0]

        print(
            f"{date} | "
            f"Last: {record.get('last')} | "
            f"Mark: {record.get('mark')} | "
            f"Bid: {record.get('bid')} | "
            f"Ask: {record.get('ask')} | "
            f"IV: {record.get('implied_volatility')}"
        )


def test_full_chain():

    print("\n=== TEST 3: FULL OPTIONS CHAIN ===")

    date = "2026-07-10"

    data, size = fetch_options(
        date=date,
        contract=None,
    )

    records = data.get("data", [])

    print(f"Date: {date}")
    print(f"Contracts returned: {len(records):,}")
    print(f"Response size: {size / 1024:.2f} KB")

    matching = [
        record
        for record in records
        if record.get("contractID") == CONTRACT
    ]

    print(f"Target contract found: {bool(matching)}")

    if matching:
        record = matching[0]

        print(f"Contract: {record['contractID']}")
        print(f"Mark: {record['mark']}")
        print(f"Bid: {record['bid']}")
        print(f"Ask: {record['ask']}")

    # --------------------------------------------------
    # Analyze expiration distribution
    # --------------------------------------------------

    expiration_counts = Counter(
        record["expiration"]
        for record in records
    )

    print("\n=== EXPIRATION DISTRIBUTION ===")

    for expiration, count in sorted(expiration_counts.items()):
        print(f"{expiration}: {count}")

    print(f"\nUnique expirations: {len(expiration_counts)}")

    print(
        f"September 18 contracts: "
        f"{expiration_counts['2026-09-18']}"
    )

if __name__ == "__main__":

    test_historical_consistency()
    test_full_chain()