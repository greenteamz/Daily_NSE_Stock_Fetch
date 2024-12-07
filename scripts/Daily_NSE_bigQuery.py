import yfinance as yf
from datetime import datetime, timedelta
import gspread
from google.oauth2.service_account import Credentials
from google.cloud import bigquery
from openpyxl import Workbook
import os
import pytz
import time

# Define IST timezone
IST = pytz.timezone('Asia/Kolkata')

# Current IST datetime
ist_now = datetime.now(IST)

# Extract the date part from IST datetime
ist_date = ist_now.date()

# Generate log and CSV file names
log_filename = f"log_{ist_now.strftime('%Y-%m-%d_%H-%M-%S')}.txt"
csv_filename = f"symbol_data_{ist_now.strftime('%Y-%m-%d_%H-%M-%S')}.csv"
excel_filename = f"symbol_data_{ist_now.strftime('%Y-%m-%d_%H-%M-%S')}.xlsx"

# Paths for logs, CSV, and Excel
LOG_FILE_PATH = os.path.join("logs", log_filename)
CSV_FILE_PATH = os.path.join("csv", csv_filename)
EXCEL_FILE_PATH = os.path.join("excel", excel_filename)

# Ensure directories exist
os.makedirs("logs", exist_ok=True)
os.makedirs("csv", exist_ok=True)
os.makedirs("excel", exist_ok=True)

# Log function
def log_message(message):
    """Log messages to a file and print to console."""
    timestamp = datetime.now(IST).strftime("%Y-%m-%d %H:%M:%S")
    with open(LOG_FILE_PATH, "a") as log_file:
        log_file.write(f"[{timestamp}] {message}\n")
    print(f"[{timestamp}] {message}")

# Authenticate using the same service_account.json for both BigQuery and Google Sheets
SERVICE_ACCOUNT_FILE = "service_account.json"

# Google Sheets authentication
gc = gspread.service_account(filename=SERVICE_ACCOUNT_FILE)

# BigQuery authentication
bq_client = bigquery.Client.from_service_account_json(SERVICE_ACCOUNT_FILE)

# Open Google Spreadsheet
spreadsheet = gc.open('NSE_symbol')  # Replace with your Google Sheet name
source_worksheet = spreadsheet.worksheet('symbol')  # Replace with your sheet name

# Fetch all stock symbols from the first column
symbols = source_worksheet.col_values(1)[1:]  # Skip header row
symbols = [symbol if symbol.endswith('.NS') else f"{symbol}.NS" for symbol in symbols]

# Define BigQuery dataset and table with the project ID
PROJECT_ID = "stockautomation-442015"  # Replace with your project ID
BQ_DATASET = "nse_data"  # Replace with your dataset name
BQ_TABLE = f"{PROJECT_ID}.{BQ_DATASET}.daily_stock_data"  # Fully-qualified table name

# Define schema for BigQuery table
headers = [
    "industry", "sector", "fullTimeEmployees", "auditRisk", "boardRisk",
    "compensationRisk", "shareHolderRightsRisk", "overallRisk", "maxAge", "priceHint",
    "previousClose", "open", "dayLow", "dayHigh", "regularMarketPreviousClose",
    "regularMarketOpen", "regularMarketDayLow", "regularMarketDayHigh", "dividendRate",
    "dividendYield", "exDividendDate", "payoutRatio", "fiveYearAvgDividendYield", "beta",
    "trailingPE", "forwardPE", "volume", "regularMarketVolume", "averageVolume",
    "averageVolume10days", "averageDailyVolume10Day", "marketCap", "fiftyTwoWeekLow",
    "fiftyTwoWeekHigh", "priceToSalesTrailing12Months", "fiftyDayAverage",
    "twoHundredDayAverage", "trailingAnnualDividendRate", "trailingAnnualDividendYield",
    "enterpriseValue", "profitMargins", "floatShares", "sharesOutstanding",
    "heldPercentInsiders", "heldPercentInstitutions", "impliedSharesOutstanding",
    "bookValue", "priceToBook", "earningsQuarterlyGrowth", "trailingEps", "forwardEps",
    "52WeekChange", "lastDividendValue", "lastDividendDate", "exchange", "quoteType",
    "symbol", "shortName", "longName", "currentPrice", "targetHighPrice", "targetLowPrice",
    "targetMeanPrice", "targetMedianPrice", "recommendationMean", "recommendationKey",
    "numberOfAnalystOpinions", "totalCash", "totalCashPerShare", "ebitda", "totalDebt",
    "quickRatio", "currentRatio", "totalRevenue", "debtToEquity", "revenuePerShare",
    "returnOnAssets", "returnOnEquity", "freeCashflow", "operatingCashflow",
    "earningsGrowth", "revenueGrowth", "grossMargins", "ebitdaMargins", "operatingMargins"
]

# Add "Previous Day Date" to headers
PREVIOUS_DAY_DATE = (ist_date - timedelta(days=1)).strftime('%Y-%m-%d')
headers_with_date = ["PreviousDayDate", "Symbol"] + headers

# Ensure BigQuery dataset exists
def ensure_dataset_exists():
    try:
        bq_client.get_dataset(BQ_DATASET)
        log_message(f"Dataset '{BQ_DATASET}' exists.")
    except Exception:
        dataset = bigquery.Dataset(f"{bq_client.project}.{BQ_DATASET}")
        bq_client.create_dataset(dataset)
        log_message(f"Created dataset '{BQ_DATASET}'.")

# Ensure BigQuery table exists
def ensure_table_exists():
    try:
        bq_client.get_table(BQ_TABLE)
        log_message(f"Table '{BQ_TABLE}' exists.")
    except Exception:
        schema = [
            bigquery.SchemaField("PreviousDayDate", "DATE"),
            bigquery.SchemaField("Symbol", "STRING")
        ] + [bigquery.SchemaField(header, "STRING") for header in headers]
        table = bigquery.Table(BQ_TABLE, schema=schema)
        bq_client.create_table(table)
        log_message(f"Created table '{BQ_TABLE}'.")

# Initialize Excel Workbook
workbook = Workbook()
sheet = workbook.active
sheet.append(["PreviousDayDate", "Symbol"] + headers)  # Add header row

# Fetch and update data for each symbol
def fetch_and_update_stock_data(symbol):
    try:
        log_message(f"Fetching data for {symbol}...")
        stock = yf.Ticker(symbol)
        info = stock.info

        # Extract data and include the Previous Day Date
        info_row = [PREVIOUS_DAY_DATE, symbol] + [info.get(key, '') for key in headers]

        # Append data to BigQuery
        row = dict(zip(headers_with_date, info_row))
        try:
            errors = bq_client.insert_rows_json(BQ_TABLE, [row])  # Insert single row
            if errors:
                raise Exception(f"BigQuery insert errors: {errors}")
            log_message(f"Successfully updated data for {symbol} in BigQuery.")
        except Exception as bq_error:
            log_message(f"BigQuery error for {symbol}: {bq_error}. Data saved locally.")

        # Append data to CSV
        with open(CSV_FILE_PATH, "a") as csv_file:
            csv_file.write(",".join(map(str, info_row)) + "\n")

        # Append data to Excel
        sheet.append(info_row)
        log_message(f"Data for {symbol} saved locally.")
    except Exception as e:
        log_message(f"Error fetching data for {symbol}: {e}")

# Ensure dataset and table exist
ensure_dataset_exists()
ensure_table_exists()

# Process symbols
for symbol in symbols:
    fetch_and_update_stock_data(symbol)
    time.sleep(1)  # Avoid rate limiting

# Save Excel file
workbook.save(EXCEL_FILE_PATH)
log_message(f"Excel data saved to {EXCEL_FILE_PATH}.")
log_message("All symbols processed.")
