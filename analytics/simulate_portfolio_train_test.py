import pandas as pd
import os
from datetime import datetime

# Paths
current_path = os.path.dirname(__file__)
parent_dir = os.path.dirname(current_path)
transformed_path = os.path.join(parent_dir, "transformed")
source_path = os.path.join(parent_dir, "data_ingestion")
analytics_path = os.path.join(parent_dir, "analytics")

# Parameters
train_start = "2020-01-01"
train_end = "2021-12-31"
test_start = "2022-01-01"
test_end = "2022-12-31"
top_n = 10

def evaluate_signals(df):
    df['Date'] = pd.to_datetime(df['Date'])
    df.sort_values('Date', inplace=True)
    df['MomentumSignal'] = (df['Return'] > 0) & (df['Return'].shift(1) > 0)
    df['MomentumReturn'] = df['Return'].shift(-1) * df['MomentumSignal']
    df['MeanReversionSignal'] = (df['Return'] < 0) & (df['Return'].shift(1) < 0)
    df['MeanReversionReturn'] = df['Return'].shift(-1) * df['MeanReversionSignal']
    return df

def summarize_train(df, ticker):
    train_df = df[(df['Date'] >= train_start) & (df['Date'] <= train_end)].dropna()
    return {
        'Ticker': ticker,
        'AvgReturnA': train_df['MomentumReturn'].mean(),
        'AvgReturnB': train_df['MeanReversionReturn'].mean()
    }

def simulate_test(df, signal_column):
    test_df = df[(df['Date'] >= test_start) & (df['Date'] <= test_end)].dropna()
    test_df['SignalReturn'] = test_df['Return'].shift(-1) * test_df[signal_column]
    return test_df[['Date', 'SignalReturn']].set_index('Date')

def simulate_portfolio(ticker_list, signal_column, signal_data):
    returns = []
    for ticker in ticker_list:
        df = signal_data[ticker]
        test_returns = simulate_test(df, signal_column)
        returns.append(test_returns)
    combined = pd.concat(returns, axis=1)
    combined.columns = ticker_list
    combined['PortfolioReturn'] = combined.mean(axis=1)
    combined['CumulativeReturn'] = combined['PortfolioReturn'].cumsum()
    cumulative_return = combined['PortfolioReturn'].sum()
    volatility = combined['PortfolioReturn'].std()
    sharpe = combined['PortfolioReturn'].mean() / volatility if volatility != 0 else None
    drawdown = (combined['CumulativeReturn'].cummax() - combined['CumulativeReturn']).max()
    return {
        'Tickers': ','.join(ticker_list),
        'CumulativeReturn': cumulative_return,
        'Volatility': volatility,
        'SharpeRatio': sharpe,
        'MaxDrawdown': drawdown
    }

def main():
    date_str = sorted(os.listdir(os.path.join(source_path, "sp500")))[-1]
    tickers_file = os.path.join(source_path, f"sp500_tickers_{date_str}.csv")
    tickers = pd.read_csv(tickers_file)['Cleaned'].tolist()

    train_metrics = []
    signal_data = {}

    for ticker in tickers:
        file_path = os.path.join(transformed_path, date_str, f"{ticker}_transformed.csv")
        if not os.path.exists(file_path):
            continue
        df = pd.read_csv(file_path)
        df = evaluate_signals(df)
        train_metrics.append(summarize_train(df, ticker))
        signal_data[ticker] = df

    df_train = pd.DataFrame(train_metrics)
    top_momentum = df_train.sort_values('AvgReturnA', ascending=False).head(top_n)['Ticker'].tolist()
    top_mean_reversion = df_train.sort_values('AvgReturnB', ascending=False).head(top_n)['Ticker'].tolist()

    summary = pd.DataFrame([
        {**simulate_portfolio(top_momentum, 'MomentumSignal', signal_data), 'Strategy': 'Momentum'},
        {**simulate_portfolio(top_mean_reversion, 'MeanReversionSignal', signal_data), 'Strategy': 'MeanReversion'}
    ])

    output_file = os.path.join(analytics_path, f"{date_str}_portfolio_backtest_train2020-2021_test2022.csv")
    summary.to_csv(output_file, index=False)
    print("✅ Realistic portfolio backtest saved.")

if __name__ == "__main__":
    main()