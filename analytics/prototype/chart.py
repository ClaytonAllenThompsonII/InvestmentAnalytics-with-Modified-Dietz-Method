import yfinance as yf
import matplotlib.pyplot as plt

# Fetch historical data for CPNG
ticker = "CPNG"
data = yf.Ticker(ticker).history(period="5y")  # Get 5 years of data

# Plot the data
plt.figure(figsize=(12, 6))
plt.plot(data.index, data["Close"], label="CPNG Closing Price", color="red")

# Add titles and labels
plt.title("CPNG Stock Price Over Time", fontsize=16)
plt.xlabel("Date", fontsize=12)
plt.ylabel("Price (USD)", fontsize=12)
plt.axhline(y=data["Close"].iloc[-1], color="teal", linestyle="--", label=f"Current Price: {data['Close'].iloc[-1]:.2f}")
plt.fill_between(data.index, data["Close"], color="red", alpha=0.1)

# Add legend and grid
plt.legend(fontsize=12)
plt.grid(alpha=0.3)

# Show the chart
plt.show()