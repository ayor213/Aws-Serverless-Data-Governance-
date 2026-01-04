import boto3
import yfinance as yf
import pandas as pd
from io import StringIO

# AWS S3 configuration
bucket_name = "ds-serverless-data-governance"
s3 = boto3.client("s3", region_name="us-east-1")

# 30 stocks
tickers = [
    "AAPL", "MSFT", "GOOGL", "AMZN", "FB",
    "TSLA", "NVDA", "BRK-B", "JPM", "JNJ",
    "V", "PG", "DIS", "HD", "MA",
    "BAC", "XOM", "PFE", "KO", "VZ",
    "ADBE", "NFLX", "CSCO", "INTC", "CMCSA",
    "PEP", "CRM", "T", "ABT", "CVX"
]

# Date range
start_date = "2018-03-01"
end_date   = "2026-01-01"

# List to hold all DataFrames
all_data = []

for ticker in tickers:
    print(f"Fetching {ticker}...")
    data = yf.download(ticker, start=start_date, end=end_date)
    if not data.empty:
        data["Ticker"] = ticker  # Add Ticker column
        all_data.append(data.reset_index())

# Combine all stocks into a single DataFrame
combined_df = pd.concat(all_data, ignore_index=True)

# Convert to CSV string
csv_buffer = StringIO()
combined_df.to_csv(csv_buffer, index=False)

# Upload to S3
object_key = "combined_daily_prices.csv"
s3.put_object(Bucket=bucket_name, Key=object_key, Body=csv_buffer.getvalue())
print(f"Combined CSV successfully uploaded to S3 bucket '{bucket_name}' as '{object_key}'")
