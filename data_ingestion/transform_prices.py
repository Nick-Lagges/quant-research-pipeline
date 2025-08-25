import pandas as pd
import os
from datetime import datetime
import numpy as np

def transform_ticker(ticker, date_str):
    current_path = os.path.dirname(__file__)
    parent_dir = os.path.dirname(current_path)
    input_path = f"sp500/{date_str}/{ticker}_prices.csv"
    output_path = parent_dir + f"/transformed/{date_str}/"

    if not os.path.exists(input_path):
        print(f"❌ Missing file for {ticker}")
        return

    df = pd.read_csv(input_path)
    df['Date'] = pd.to_datetime(df['Date'])
    df.sort_values('Date', inplace=True)

    # Calculate daily returns
    df['Return'] = df['Close'].pct_change()
    df['LogReturn'] = np.log(df['Close'] / df['Close'].shift(1))

    # Optional: rolling volatility (20-day)
    df['Volatility'] = df['Return'].rolling(window=20).std()

    # Reorder columns
    columns_order = ['Date', 'Ticker', 'Close', 'Return', 'LogReturn', 'Volatility', 'Volume']
    df = df[columns_order]

    os.makedirs(output_path, exist_ok=True)
    df.to_csv(f"{output_path}/{ticker}_transformed.csv", index=False)
    print(f"✅ Transformed {ticker}")

if __name__ == "__main__":
    sp500_folder = "sp500"
    # Get the latest ingestion date folder
    date_str = sorted(os.listdir(sp500_folder))[-1]
    cleaned_tickers = f"sp500_tickers_{date_str}.csv"
    tickers = pd.read_csv(cleaned_tickers)['Cleaned'].tolist()
    for ticker in tickers:
        transform_ticker(ticker, date_str)