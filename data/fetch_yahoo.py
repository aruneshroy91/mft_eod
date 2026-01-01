import yfinance as yf
import pandas as pd
import yaml
from pathlib import Path


def load_data_config():
    with open("config/data.yaml", "r") as f:
        return yaml.safe_load(f)["data"]

cfg = load_data_config()

START_DATE = cfg["start_date"]
DATA_DIR = Path("data/prices/yahoo")

def load_universe():
    with open("config/universe.yaml", "r") as f:
        cfg = yaml.safe_load(f)
    return cfg["universe"]["tickers"]

def fetch_and_save(symbol: str):
    print(f"Fetching {symbol}...")
    df = yf.download(
        symbol,
        start=START_DATE,
        auto_adjust=False,
        progress=False
    )

    if df.empty:
        print(f"⚠️ No data for {symbol}")
        return

    df = df[["Open", "High", "Low", "Close", "Volume"]]
    df.dropna(inplace=True)

    DATA_DIR.mkdir(parents=True, exist_ok=True)
    file_path = DATA_DIR / f"{symbol}.csv"
    df.to_csv(file_path)

def main():
    tickers = load_universe()
    for ticker in tickers:
        try:
            fetch_and_save(ticker)
        except Exception as e:
            print(f"❌ Error fetching {ticker}: {e}")

if __name__ == "__main__":
    main()
