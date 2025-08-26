import pandas as pd
import os
import matplotlib.pyplot as plt

# Paths
current_path = os.path.dirname(__file__)
parent_dir = os.path.dirname(current_path)
transformed_path = os.path.join(parent_dir, "transformed")
source_path = os.path.join(parent_dir, "data_ingestion")

# Parameters
date_str = sorted(os.listdir(os.path.join(source_path, "sp500")))[-1]
tickers_file = os.path.join(source_path, f"sp500_tickers_{date_str}.csv")
tickers = pd.read_csv(tickers_file)['Cleaned'].tolist()

train_start = "2020-01-01"
train_end = "2021-12-31"
test_start = "2022-01-01"
test_end = "2022-12-31"
top_n = 10

def load_and_tag_signals(ticker):
    path = os.path.join(transformed_path, date_str, f"{ticker}_transformed.csv")
    if not os.path.exists(path):
        return None

    df = pd.read_csv(path)
    df['Date'] = pd.to_datetime(df['Date'])
    df.sort_values('Date', inplace=True)

    df['MomentumSignal'] = (df['Return'] > 0) & (df['Return'].shift(1) > 0)
    df['MeanReversionSignal'] = (df['Return'] < 0) & (df['Return'].shift(1) < 0)
    return df

def simulate_cumulative(df, signal_column):
    test_df = df[(df['Date'] >= test_start) & (df['Date'] <= test_end)].copy()
    test_df['SignalReturn'] = test_df['Return'].shift(-1) * test_df[signal_column]
    test_df = test_df[['Date', 'SignalReturn']].dropna()
    test_df.set_index('Date', inplace=True)
    return test_df

# Step 1: Evaluate train performance
train_metrics = []
signal_data = {}

for ticker in tickers:
    df = load_and_tag_signals(ticker)
    if df is None:
        continue

    train_df = df[(df['Date'] >= train_start) & (df['Date'] <= train_end)].dropna()
    avg_return_a = (train_df['Return'].shift(-1) * train_df['MomentumSignal']).mean()
    avg_return_b = (train_df['Return'].shift(-1) * train_df['MeanReversionSignal']).mean()

    train_metrics.append({'Ticker': ticker, 'AvgReturnA': avg_return_a, 'AvgReturnB': avg_return_b})
    signal_data[ticker] = df

df_train = pd.DataFrame(train_metrics)
top_momentum = df_train.sort_values('AvgReturnA', ascending=False).head(top_n)['Ticker'].tolist()
top_mean_reversion = df_train.sort_values('AvgReturnB', ascending=False).head(top_n)['Ticker'].tolist()

# Step 2: Simulate cumulative returns
def simulate_strategy(ticker_list, signal_column):
    returns = []
    for ticker in ticker_list:
        df = signal_data[ticker]
        r = simulate_cumulative(df, signal_column)
        returns.append(r)

    combined = pd.concat(returns, axis=1)
    combined.columns = ticker_list
    combined['PortfolioReturn'] = combined.mean(axis=1)
    combined['CumulativeReturn'] = combined['PortfolioReturn'].cumsum()
    return combined

momentum_curve = simulate_strategy(top_momentum, 'MomentumSignal')
mean_reversion_curve = simulate_strategy(top_mean_reversion, 'MeanReversionSignal')

# Step 3: Plot
plt.figure(figsize=(12, 6))
plt.plot(momentum_curve.index, momentum_curve['CumulativeReturn'], label='Momentum Portfolio', color='blue')
plt.plot(mean_reversion_curve.index, mean_reversion_curve['CumulativeReturn'], label='Mean Reversion Portfolio', color='green')
plt.title('Cumulative Return (2022) — Momentum vs. Mean Reversion')
plt.xlabel('Date')
plt.ylabel('Cumulative Return')
plt.legend()
plt.grid(True)
plt.tight_layout()
plt.show()