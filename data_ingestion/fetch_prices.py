import yfinance as yf
df = yf.download("AAPL", start="2020-01-01", end="2023-01-01", auto_adjust=True)
df.to_csv("AAPL_prices.csv")