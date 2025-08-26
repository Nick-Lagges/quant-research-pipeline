import pandas as pd
import os


current_path = os.path.dirname(__file__)
parent_dir = os.path.dirname(current_path)

def backtest_momentum(ticker, date_str):
    input_path = f"{parent_dir}/transformed/{date_str}/{ticker}_transformed.csv"
    if not os.path.exists(input_path):
        print(f"❌ Missing transformed file for {ticker}")
        return None

    df = pd.read_csv(input_path)
    df['Date'] = pd.to_datetime(df['Date'])
    df.sort_values('Date', inplace=True)

    # Signal A: 2-day positive momentum
    df['PositiveMomentumSignal'] = (df['Return'] > 0) & (df['Return'].shift(1) > 0)

    # Strategy return: next day's return if signal is True
    df['PositiveMomentumSignalReturn'] = df['Return'].shift(-1) * df['PositiveMomentumSignal']

    # Metrics
    hit_rate_a = df['PositiveMomentumSignal'].sum() / len(df)
    avg_return_a = df['PositiveMomentumSignalReturn'].mean()
    total_return_a = df['PositiveMomentumSignalReturn'].sum()
    max_drawdown_a = (df['PositiveMomentumSignalReturn'].cumsum().cummax() - df['PositiveMomentumSignalReturn'].cumsum()).max()

    # Signal B: 2-day negative momentum (Mean Reversion)
    df['MeanReversionSignal'] = (df['Return'] < 0) & (df['Return'].shift(1) < 0)

    # Strategy return: next day's return if signal is True
    df['MeanReversionReturn'] = df['Return'].shift(-1) * df['MeanReversionSignal']

    # Metrics
    hit_rate_b = df['MeanReversionSignal'].sum() / len(df)
    avg_return_b = df['MeanReversionReturn'].mean()
    total_return_b = df['MeanReversionReturn'].sum()
    max_drawdown_b = (df['MeanReversionReturn'].cumsum().cummax() - df['MeanReversionReturn'].cumsum()).max()

    return {
        'Ticker': ticker,
        'HitRateA': hit_rate_a,
        'AvgReturnA': avg_return_a,
        'TotalReturnA': total_return_a,
        'MaxDrawdownA': max_drawdown_a,
        'HitRateB': hit_rate_b,
        'AvgReturnB': avg_return_b,
        'TotalReturnB': total_return_b,
        'MaxDrawdownB': max_drawdown_b
    }

if __name__ == "__main__":
    source_path = f"{parent_dir}/data_ingestion/"
    date_str = sorted(os.listdir(f"{source_path}/sp500"))[-1]
    tickers = pd.read_csv(f"{source_path}/sp500_tickers_{date_str}.csv")['Cleaned'].tolist()

    results = []
    for ticker in tickers:
        metrics = backtest_momentum(ticker, date_str)
        if metrics:
            results.append(metrics)

    df_results = pd.DataFrame(results)
    df_results['BestAvgReturn'] = df_results[['AvgReturnA', 'AvgReturnB']].max(axis=1)
    df_results['WinningSignal'] = df_results.apply(
        lambda row: 'Momentum' if row['AvgReturnA'] > row['AvgReturnB'] else 'MeanReversion',
        axis=1
    )
    df_results.sort_values('BestAvgReturn', ascending=False, inplace=True)
    df_results.to_csv(f"{date_str}_signal_comparison.csv", index=False)
    print("✅ Momentum backtest complete.")