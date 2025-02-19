import yfinance as yf
import logging
import time

# Configure detailed logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)s: %(message)s'
)

def fetch_current_price(ticker_symbol, retries=5, delay=10):
    """
    Fetches the current market price of a given stock ticker using Yahoo Finance.
    Tries to use the fast_info attribute first, then falls back to ticker.info.
    Retries on 429 (rate limiting) errors with exponential backoff.

    Args:
        ticker_symbol (str): The stock ticker symbol (e.g., 'CPNG').
        retries (int): Number of times to retry fetching data.
        delay (int): Initial delay (in seconds) between retries.

    Returns:
        float or None: The current price if available, otherwise None.
    """
    attempt = 0
    while attempt < retries:
        attempt += 1
        try:
            ticker = yf.Ticker(ticker_symbol)
            
            # First, try using fast_info (newer and sometimes less rate limited)
            if hasattr(ticker, 'fast_info') and ticker.fast_info:
                current_price = ticker.fast_info.get('lastPrice')
                if current_price is not None:
                    logging.info("Current Price for %s from fast_info: %s", ticker_symbol, current_price)
                    return current_price
                else:
                    logging.info("fast_info did not return a price for %s.", ticker_symbol)
            
            # Fall back to the full info dictionary for diagnostics.
            info = ticker.info
            logging.info("Ticker Info for %s: %s", ticker_symbol, info)
            
            # Try preferred keys.
            current_price = info.get('currentPrice') or info.get('regularMarketPrice')
            if current_price is not None:
                logging.info("Current Price for %s: %s", ticker_symbol, current_price)
                return current_price
            else:
                logging.warning("No price data available for %s.", ticker_symbol)
                return None

        except Exception as e:
            error_str = str(e)
            logging.error("Attempt %d: Error fetching price for %s: %s", attempt, ticker_symbol, error_str)
            
            # Check if error indicates a rate limiting issue.
            if "429" in error_str:
                logging.error("Rate limit encountered for %s. Retrying in %d seconds...", ticker_symbol, delay)
                time.sleep(delay)
                delay *= 2  # Exponential backoff.
            else:
                # If error is not clearly a rate limit, break out.
                break

    logging.error("Failed to fetch price for %s after %d attempts.", ticker_symbol, retries)
    return None

if __name__ == "__main__":
    fetch_current_price("CPNG")