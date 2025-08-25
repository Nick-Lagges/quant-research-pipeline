import yfinance as yf
import pandas as pd
import os
from datetime import datetime
import time
import requests
from bs4 import BeautifulSoup

def get_sp500_tickers():
    url = "https://topforeignstocks.com/indices/components-of-the-sp-500-index/"
    headers = {'User-Agent': 'Mozilla/5.0'}
    response = requests.get(url, headers=headers)
    soup = BeautifulSoup(response.text, 'lxml')
    table = soup.find('table')

    tickers = []
    for row in table.find_all('tr')[1:]:
        cells = row.find_all('td')
        if len(cells) >= 2:
            symbol = cells[2].text.strip()
            tickers.append(symbol)
    return tickers

'''def get_sp500_tickers():
    url = 'https://en.wikipedia.org/wiki/List_of_S%26P_500_companies'
    headers = {'User-Agent': 'Mozilla/5.0'}
    response = requests.get(url, headers=headers)
    soup = BeautifulSoup(response.text, 'lxml')
    table = soup.find('table', {'class': 'wikitable sortable'})

    tickers = []
    for row in table.find_all('tr')[1:]:
        cells = row.find_all('td')
        if len(cells) < 1:
            continue

        symbol = cells[0].text.strip()
        tickers.append(symbol)
    return tickers'''


def clean_ticker(tickers):
    cleaned = []
    for ticker in tickers:
        ticker = ticker.replace('.', '-')

        cleaned.append(ticker)
    return cleaned


def filter_valid_tickers(tickers):
    return [t for t in tickers if t.isalpha() or '-' in t]


def save_ticker_list(tickers, cleaned_tickers):
    date_str = datetime.today().strftime('%Y-%m-%d')
    pd.DataFrame({'Raw': tickers, 'Cleaned': cleaned_tickers}).to_csv(f"sp500_tickers_{date_str}.csv", index=False)


def download_prices(tickers, start="2020-01-01", end="2023-01-01"):
    date_str = datetime.today().strftime('%Y-%m-%d')
    folder = f"sp500/{date_str}"
    os.makedirs(folder, exist_ok=True)

    for i, ticker in enumerate(tickers):
        print(f"[{i}/{len(tickers)}] Downloading {ticker}...")
        try:
            df = download_with_retry(ticker, start, end)
            if not df.empty:
                df['Ticker'] = ticker
                df['IngestedDate'] = date_str


                df.to_csv(f"{folder}/{ticker}_prices.csv")
                print(f"✅ {ticker} downloaded.")
                log_download_result(ticker, "SUCCESS")
            else:
                print(f"⚠️ {ticker} returned empty data.")
                log_download_result(ticker, "FAILED")
        except Exception as e:
            print(f"❌ Error downloading {ticker}: {e}")
            log_download_result(ticker, "ERROR", str(e))
        time.sleep(1)


def download_with_retry(ticker, start, end, retries=3, delay=5):
    for attempt in range(retries):
        try:
            df = yf.download(ticker, start=start, end=end)
            if not df.empty:
                return df
        except Exception as e:
            print(f"Retry {attempt + 1} for {ticker} due to error: {e}")
            time.sleep(delay)
    return pd.DataFrame()  # Return empty if all retries fail


def log_download_result(ticker, status, message=""):
    log_path = "logs/download_log.txt"
    os.makedirs("logs", exist_ok=True)
    with open(log_path, "a") as log:
        log.write(f"{datetime.now()} | {ticker} | {status} | {message}\n")

if __name__ == "__main__":
    sp500_tickers = get_sp500_tickers()
    cleaned_tickers = clean_ticker(sp500_tickers)
    valid_tickers = filter_valid_tickers(cleaned_tickers)
    save_ticker_list(sp500_tickers, cleaned_tickers)
    download_prices(valid_tickers)

