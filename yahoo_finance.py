import yfinance as yf

def fetch_current_price(ticker_symbol):
    """
    Fetches the current market price of a given stock ticker using Yahoo Finance.

    Args:
        ticker_symbol (str): The stock ticker symbol (e.g., 'CPNG').

    Returns:
        float or None: The current price if available, otherwise None.
    """
    try:
        ticker = yf.Ticker(ticker_symbol)
        print("Ticker Info:")
        print(ticker.info)  # Print the full info dictionary for inspection
        
        # Attempt to fetch the current price
        current_price = (
            ticker.info.get('currentPrice')  # Preferred key
            or ticker.info.get('regularMarketPrice')  # Fallback key
        )
        
        if current_price is not None:
            print(f"Current Price for {ticker_symbol}: {current_price}")
        else:
            print(f"No price data available for {ticker_symbol}.")
        return current_price
    except Exception as e:
        print(f"Error fetching price for {ticker_symbol}: {e}")
        return None

# Example usage
if __name__ == "__main__":
    fetch_current_price("CPNG")