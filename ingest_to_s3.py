import boto3
import yfinance as yf
import pandas as pd
from io import StringIO

bucket_name = "ds-serverless-data-governance"
s3 = boto3.client("s3", region_name="us-east-1")

tickers = [
    "AAPL","MSFT","GOOGL","AMZN","FB","TSLA","NVDA","BRK-B","JPM","JNJ",
    "V","PG","DIS","HD","MA","BAC","XOM","PFE","KO","VZ",
    "ADBE","NFLX","CSCO","INTC","CMCSA","PEP","CRM","T","ABT","CVX"
]

start_date = "2018-03-01"
end_date   = "2026-01-01"

frames = []

for ticker in tickers:
    print(f"Fetching {ticker}...")
    df = yf.download(ticker, start=start_date, end=end_date, auto_adjust=False)

    if df.empty:
        continue

    # HARD RESET
    df = df.reset_index()

    # Flatten columns if yfinance gave MultiIndex
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = df.columns.get_level_values(0)

    # Standardize column names
    df.columns = [c.replace(" ", "_") for c in df.columns]

    # Guarantee Adj_Close
    if "Adj_Close" not in df.columns:
        df["Adj_Close"] = df["Close"]

    # Add ticker
    df["Ticker"] = ticker

    # Enforce schema strictly
    df = df[[
        "Date",
        "Ticker",
        "Open",
        "High",
        "Low",
        "Close",
        "Adj_Close",
        "Volume"
    ]]

    frames.append(df)

# Final combine
combined_df = pd.concat(frames, ignore_index=True)
combined_df.sort_values(["Date", "Ticker"], inplace=True)

# Export
csv_buffer = StringIO()
combined_df.to_csv(csv_buffer, index=False)

s3.put_object(
    Bucket=bucket_name,
    Key="combined_daily_prices_normalized.csv",
    Body=csv_buffer.getvalue()
)

print("✅ Normalized long-format CSV uploaded to S3")
