import pandas as pd
import os

# Resolve paths
current_path = os.path.dirname(__file__)
parent_dir = os.path.dirname(current_path)
analytics_path = os.path.join(parent_dir, "analytics")
source_path = os.path.join(parent_dir, "data_ingestion")

# Get latest ingestion date
date_str = sorted(os.listdir(os.path.join(source_path, "sp500")))[-1]
signal_file = os.path.join(analytics_path, f"{date_str}_signal_comparison.csv")

# Load signal metrics
df = pd.read_csv(signal_file)

def summarize_portfolio(df_subset, strategy_label):
    return {
        'Strategy': strategy_label,
        'TopTickers': ','.join(df_subset['Ticker'].tolist()),
        'AvgReturn': df_subset[f'AvgReturn{strategy_label}'].mean(),
        'TotalReturn': df_subset[f'TotalReturn{strategy_label}'].sum(),
        'MaxDrawdown': df_subset[f'MaxDrawdown{strategy_label}'].mean(),
        'HitRate': df_subset[f'HitRate{strategy_label}'].mean()
    }

# Select top 10 tickers by AvgReturn for each strategy
top_n = 10
momentum_df = df.sort_values('AvgReturnA', ascending=False).head(top_n)
mean_reversion_df = df.sort_values('AvgReturnB', ascending=False).head(top_n)

# Summarize portfolios
summary = pd.DataFrame([
    summarize_portfolio(momentum_df, 'A'),
    summarize_portfolio(mean_reversion_df, 'B')
])

# Save output
output_file = os.path.join(analytics_path, f"{date_str}_portfolio_summary.csv")
summary.to_csv(output_file, index=False)
print("✅ Portfolio summary saved.")