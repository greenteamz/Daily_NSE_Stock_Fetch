import yfinance as yf
from datetime import datetime
import gspread
from google.cloud import bigquery
import os
import pytz
import time
import pandas as pd

from google.api_core.exceptions import NotFound
from google.cloud import bigquery
from google.oauth2.service_account import Credentials

# Define IST timezone
IST = pytz.timezone('Asia/Kolkata')
ist_now = datetime.now(IST)
ist_date = ist_now.date()

# Base directory and subdirectories
BASE_DIR = "NSE_History"
LOGS_DIR = os.path.join(BASE_DIR, "logs")
CSV_DIR = os.path.join(BASE_DIR, "csv")
EXCEL_DIR = os.path.join(BASE_DIR, "excel")

# Ensure all required directories exist
os.makedirs(BASE_DIR, exist_ok=True)
os.makedirs(LOGS_DIR, exist_ok=True)
os.makedirs(CSV_DIR, exist_ok=True)
os.makedirs(EXCEL_DIR, exist_ok=True)

# File paths
LOG_FILE_PATH = os.path.join(LOGS_DIR, f"Log_{ist_now.strftime('%Y-%m-%d_%H-%M-%S')}.txt")
MASTER_CSV_FILE_PATH = os.path.join(CSV_DIR, "Master_NSE_stock_history_2024.csv")
DAILY_CSV_FILE_PATH = os.path.join(CSV_DIR, f"NSE_Stock_History_Cleaned_24_Data_{ist_now.strftime('%Y-%m-%d_%H-%M-%S')}.csv")
SUMMARY_CSV_FILE_PATH = os.path.join(CSV_DIR, f"Summary_NSE_cleaned_History_24_Data_{ist_now.strftime('%Y-%m-%d_%H-%M-%S')}.csv")

# BigQuery configuration
SERVICE_ACCOUNT_FILE = "service_account.json"
BQ_DATASET = "NSE_stock_history_2024"
BQ_TABLE = "stock_history_24_monthly_cleaned"
PROJECT_ID = "stockautomation-442015"

# Initialize Google Sheets and BigQuery clients
gc = gspread.service_account(filename=SERVICE_ACCOUNT_FILE)
bigquery_client = bigquery.Client.from_service_account_json(SERVICE_ACCOUNT_FILE)


# Logging function
def log_message(message):
    timestamp = datetime.now(IST).strftime("%Y-%m-%d %H:%M:%S")
    full_message = f"[{timestamp}] {message}"
    print(full_message)
    with open(LOG_FILE_PATH, "a") as log_file:
        log_file.write(full_message + "\n")


# BigQuery table and dataset creation
def create_bigquery_resources():
    try:
        dataset_ref = bigquery.Dataset(f"{PROJECT_ID}.{BQ_DATASET}")
        try:
            bigquery_client.get_dataset(dataset_ref)
            log_message(f"Dataset '{BQ_DATASET}' already exists.")
        except NotFound:
            bigquery_client.create_dataset(dataset_ref)
            log_message(f"Created dataset '{BQ_DATASET}'.")

        table_ref = f"{PROJECT_ID}.{BQ_DATASET}.{BQ_TABLE}"
        schema = [
            bigquery.SchemaField("Date", "DATE"),
            bigquery.SchemaField("Open", "FLOAT"),
            bigquery.SchemaField("High", "FLOAT"),
            bigquery.SchemaField("Low", "FLOAT"),
            bigquery.SchemaField("Close", "FLOAT"),
            bigquery.SchemaField("Volume", "INTEGER"),
            bigquery.SchemaField("Month_Change", "FLOAT"),
            bigquery.SchemaField("Month_Volatile", "FLOAT"),
            bigquery.SchemaField("Yearly_Change", "FLOAT"),
            bigquery.SchemaField("Symbol", "STRING"),
            bigquery.SchemaField("Listed_Month", "STRING"),
        ]
        try:
            bigquery_client.get_table(table_ref)
            log_message(f"Table '{table_ref}' already exists.")
        except NotFound:
            table = bigquery.Table(table_ref, schema=schema)
            bigquery_client.create_table(table)
            log_message(f"Created table '{table_ref}'.")
    except Exception as e:
        log_message(f"Error creating BigQuery resources: {e}")


# Process stock data
def process_stock_data(data, symbol):
    data = data[data['Volume'] > 0].dropna()  # Filter rows with non-zero volume
    if data.empty:
        log_message(f"No valid data for {symbol}, skipping.")
        print(data)
        return None

    # Determine Listed Month
    listed_month = data.iloc[0]['Date'].strftime('%Y-%m')
    data['Listed_Month'] = listed_month

    # Remove data for the first listing month
    first_month = data.iloc[0]['Date'].month
    first_year = data.iloc[0]['Date'].year
    data = data[~((data['Date'].dt.month == first_month) & (data['Date'].dt.year == first_year))]

    # Calculate metrics
    data['Month_Change'] = ((data['Close'] - data['Open']) / data['Open']) * 100
    data['Month_Volatile'] = ((data['High'] - data['Low']) / data['Low']) * 100
    data['Yearly_Change'] = data['Month_Change'].cumsum()

    data[['Month_Change', 'Month_Volatile', 'Yearly_Change']] = data[
        ['Month_Change', 'Month_Volatile', 'Yearly_Change']
    ].round(2)

    data['Symbol'] = symbol  # Add symbol column
    return data


# Process symbols
def process_symbols(symbols):
    summary_data = []
    for idx, symbol in enumerate(symbols):
        try:
            log_message(f"Processing {symbol} ({idx + 1}/{len(symbols)})")
            data = yf.download(
                tickers=symbol, start="2024-01-01", end=ist_date, interval="1mo", auto_adjust=True, progress=False
            )
            data.reset_index(inplace=True)
            
            # Filter rows with valid dates (remove junk data)
            first_valid_date = data['Date'].min()
            data = data[data['Date'] >= first_valid_date]  
            
            processed_data = process_stock_data(data, symbol)

            if processed_data is not None:
                if not os.path.exists(MASTER_CSV_FILE_PATH):
                    processed_data.to_csv(MASTER_CSV_FILE_PATH, mode='w', index=False, header=True)
                else:
                    processed_data.to_csv(MASTER_CSV_FILE_PATH, mode='a', index=False, header=False)

                avg_month_change = processed_data['Month_Change'].mean()
                avg_month_volatile = processed_data['Month_Volatile'].mean()
                last_yearly_change = processed_data['Yearly_Change'].iloc[-1]

                summary_data.append({
                    "Symbol": symbol,
                    "Yearly_Change": last_yearly_change,
                    "Avg_Month_Change": round(avg_month_change, 2),
                    "Avg_Month_Volatile": round(avg_month_volatile, 2),
                })

            log_message(f"Successfully processed NSE {symbol}.")
            time.sleep(0.7)  # Delay to avoid rate-limiting
        except Exception as e:
            log_message(f"Error processing {symbol}: {e}")

    # Save summary
    pd.DataFrame(summary_data).to_csv(SUMMARY_CSV_FILE_PATH, index=False)
    log_message(f"Summary saved to {SUMMARY_CSV_FILE_PATH}.")


# Main script
try:
    log_message("Fetching symbols from Google Sheets...")
    spreadsheet = gc.open('NSE_symbol')
    worksheet = spreadsheet.worksheet('symbol')
    symbols = [symbol if symbol.endswith('.NS') else f"{symbol}.NS" for symbol in worksheet.col_values(1)[1:]]
    log_message(f"Fetched {len(symbols)} symbols.")

    create_bigquery_resources()
    process_symbols(symbols)
except Exception as e:
    log_message(f"Fatal error: {e}")
