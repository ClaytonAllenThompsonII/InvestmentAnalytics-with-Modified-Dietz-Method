import os
import requests

from dotenv import load_dotenv


load_dotenv()

API_KEY = os.getenv("ALPHAVANTAGE_PREMIUM_API_KEY")

if not API_KEY:
    raise RuntimeError("Alpha Vantage API key not found.")

BASE_URL = "https://www.alphavantage.co/query"

FB_CONTRACT = "FB220708P00200000"
META_CONTRACT = "META220708P00200000"


def fetch_option(symbol, date, contract):

    params = {
        "function": "HISTORICAL_OPTIONS",
        "symbol": symbol,
        "date": date,
        "contract": contract,
        "apikey": API_KEY,
    }

    response = requests.get(
        BASE_URL,
        params=params,
        timeout=60,
    )

    response.raise_for_status()

    data = response.json()

    for key in ("Error Message", "Information", "Note"):
        if key in data:
            print(f"API Message: {data[key]}")
            return []

    return data.get("data", [])


def test_symbol_transition():

    print("\n=== FB / META HISTORICAL OPTIONS TEST ===")

    tests = [
        # Before ticker change
        ("2022-06-08", "FB", FB_CONTRACT),
        ("2022-06-08", "META", META_CONTRACT),

        # After ticker change
        ("2022-06-09", "FB", FB_CONTRACT),
        ("2022-06-09", "META", META_CONTRACT),
    ]

    for date, symbol, contract in tests:

        print(f"\nDate: {date}")
        print(f"Underlying: {symbol}")
        print(f"Contract: {contract}")

        records = fetch_option(
            symbol=symbol,
            date=date,
            contract=contract,
        )

        matches = [
            record
            for record in records
            if record.get("contractID") == contract
            and record.get("date") == date
        ]

        if not matches:
            print("Result: NO MATCHING DATA")
            print(f"Records returned: {len(records)}")
            continue

        record = matches[0]

        print("Result: SUCCESS")
        print(f"Returned Contract: {record['contractID']}")
        print(f"Date: {record['date']}")
        print(f"Last: {record.get('last')}")
        print(f"Bid: {record.get('bid')}")
        print(f"Ask: {record.get('ask')}")
        print(f"Mark: {record.get('mark')}")
        print(f"Volume: {record.get('volume')}")
        print(f"Open Interest: {record.get('open_interest')}")


if __name__ == "__main__":
    test_symbol_transition()