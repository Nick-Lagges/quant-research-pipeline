import pandas as pd
import os
import numpy as np


current_path = os.path.dirname(__file__)
parent_dir = os.path.dirname(current_path)

def analyze_ticker(ticker, date_str):
    input_path = f"{parent_dir}/transformed/{date_str}/{ticker}_transformed.csv"
    if not os.path.exists(input_path):
        print(f"❌ Missing transformed file for {ticker}")
        return None

    df = pd.read_csv(input_path)
    df['Date'] = pd.to_datetime(df['Date'])
    df.sort_values('Date', inplace=True)

    # Drop NaNs from early rolling calculations
    df = df.dropna(subset=['Return', 'LogReturn', 'Volatility'])

    # Compute analytics
    avg_return = df['Return'].mean()
    volatility = df['Volatility'].mean()
    sharpe_ratio = avg_return / volatility if volatility != 0 else np.nan
    max_drawdown = (df['Close'].cummax() - df['Close']).max()

    return {
        'Ticker': ticker,
        'AvgReturn': avg_return,
        'Volatility': volatility,
        'SharpeRatio': sharpe_ratio,
        'MaxDrawdown': max_drawdown
    }

if __name__ == "__main__":
    source_path = f"{parent_dir}/data_ingestion/"
    date_str = sorted(os.listdir(f"{source_path}/sp500"))[-1]
    tickers = pd.read_csv(f"{source_path}/sp500_tickers_{date_str}.csv")['Cleaned'].tolist()

    results = []
    for ticker in tickers:
        metrics = analyze_ticker(ticker, date_str)
        if metrics:
            results.append(metrics)

    df_results = pd.DataFrame(results)
    df_results.sort_values('SharpeRatio', ascending=False, inplace=True)
    df_results.to_csv(f"{date_str}_analytics_summary.csv", index=False)
    print("✅ Analytics summary saved.")