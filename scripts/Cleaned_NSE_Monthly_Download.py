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
LOG_FILE_PATH = os.path.join(LOGS_DIR, f"Log_C_{ist_now.strftime('%Y-%m-%d_%H-%M-%S')}.txt")
MASTER_CSV_FILE_PATH = os.path.join(CSV_DIR, "C_Master_NSE_stock_history_2024.csv")
DAILY_CSV_FILE_PATH = os.path.join(CSV_DIR, f"C_NSE_Stock_History_24_Data_{ist_now.strftime('%Y-%m-%d_%H-%M-%S')}.csv")
EXCEL_FILE_PATH = os.path.join(EXCEL_DIR, f"C_Stock_24_{ist_now.strftime('%Y_%m')}.xlsx")
SUMMARY_CSV_FILE_PATH = os.path.join(CSV_DIR, f"C_Summary_NSE_Stock_24_Data_{ist_now.strftime('%Y-%m-%d_%H-%M-%S')}.csv")

# BigQuery details
SERVICE_ACCOUNT_FILE = "service_account.json"

# BigQuery dataset and table configuration
BQ_DATASET = "NSE_stock_history_2024"  # Replace with your dataset name
BQ_TABLE = "stock_history_24_monthly_Cleaned"        # Replace with your table name
PROJECT_ID = "stockautomation-442015"  # Replace with your GCP project ID

# Initialize Google Sheets and BigQuery clients
gc = gspread.service_account(filename=SERVICE_ACCOUNT_FILE)


# Logging function
def log_message(message):
    """Logs a message to the log file and console."""
    timestamp = datetime.now(IST).strftime("%Y-%m-%d %H:%M:%S")
    full_message = f"[{timestamp}] {message}"
    print(full_message)
    with open(LOG_FILE_PATH, "a") as log_file:
        log_file.write(full_message + "\n")

# BigQuery dataset and table creation
def create_bigquery_resources():
    """Creates BigQuery dataset and table if not exists."""
    try:
        # Create dataset reference
        dataset_ref = bigquery.Dataset(f"{PROJECT_ID}.{BQ_DATASET}")

        # Create dataset if not exists
        try:
            bigquery_client.get_dataset(dataset_ref)
            log_message(f"Dataset '{BQ_DATASET}' already exists.")
        except NotFound:
            bigquery_client.create_dataset(dataset_ref)
            log_message(f"Created dataset '{BQ_DATASET}'.")

        # Create table reference
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
        ]

        # Create table if not exists
        try:
            bigquery_client.get_table(table_ref)
            log_message(f"Table '{table_ref}' already exists.")
        except NotFound:
            table = bigquery.Table(table_ref, schema=schema)
            bigquery_client.create_table(table)
            log_message(f"Created table '{table_ref}'.")
    except Exception as e:
        log_message(f"Error creating BigQuery resources: {e}")

# BigQuery write function
def write_to_bigquery(df):
    """Writes data to a BigQuery table."""
    try:
        # Ensure the 'Date' column is in the correct format
        if 'Date' in df.columns:
            df['Date'] = pd.to_datetime(df['Date'])  # Convert to datetime format
            
        table_id = f"{PROJECT_ID}.{BQ_DATASET}.{BQ_TABLE}"  # Fully-qualified table name
        job = bigquery_client.load_table_from_dataframe(df, table_id)
        job.result()  # Wait for the load job to complete
        log_message(f"Data successfully written to BigQuery: {table_id}")
    except Exception as e:
        log_message(f"Error writing to BigQuery: {e}")

# Fetch symbols from Google Sheets
try:
    log_message("Fetching stock symbols from Google Sheets...")
    spreadsheet = gc.open('NSE_symbol')
    source_worksheet = spreadsheet.worksheet('symbol')
    symbols = source_worksheet.col_values(1)[1:]  # Skip header row
    symbols = [symbol if symbol.endswith('.NS') else f"{symbol}.NS" for symbol in symbols]
    log_message(f"Fetched {len(symbols)} symbols.")
except Exception as e:
    log_message(f"Error fetching symbols: {e}")
    symbols = []



# DataFrame for summary statistics
summary_data = []

# Process each symbol
processed_count = 0

# Process symbols and download stock data
for symbol in symbols:
    try:
        processed_count += 1
        log_message(f"NSE Processing symbol: {symbol} - {processed_count}/{len(symbols)}")

        # Download stock data for the symbol
        start_date = "2024-01-01"
        end_date = ist_date
        data = yf.download(tickers=symbol, start=start_date, end=end_date, interval="1mo", auto_adjust=True, progress=False)

        # Drop rows where 'Volume' is zero or NaN (indicates no trading activity)
        data = data[data['Volume'] > 0].dropna()
        
        if data.empty:
            log_message(f"No data found for {symbol}, skipping.")
            continue

        # Reset index to add 'Date' as a column
        data.reset_index(inplace=True)

        # Filter rows with valid dates (remove junk data)
        data['Date'] = pd.to_datetime(data['Date'])  # Ensure 'Date' is in datetime format
        
        first_valid_date = data['Date'].min()
        data = data[data['Date'] >= first_valid_date] 
        
        # Determine Listed Month
        log_message(f"Determine Listed Month  {symbol}")
        listed_month = pd.to_datetime(data.iloc[0]['Date']).strftime('%Y-%m')
        log_message(f"Listed Month checked  {symbol}")
        data['Listed_Month'] = listed_month
        
       # Remove data for the first listing month
        log_message(f"Checking if first listing month matches start date for {symbol}")
        first_month = data.iloc[0]['Date'].month
        first_year = data.iloc[0]['Date'].year

        # Check if the first listing month matches the download start date
        download_start_date = datetime.strptime(start_date, "%Y-%m-%d")
        if not (first_month == download_start_date.month and first_year == download_start_date.year):
            log_message(f"Removing data for the first listing month for {symbol}")
            data = data[~((data['Date'].dt.month == first_month) & (data['Date'].dt.year == first_year))]
        else:
            log_message(f"First listing month matches start date, skipping removal for {symbol}")


        # Round Open, High, Low, and Close to 2 decimal places
        data[['Open', 'High', 'Low', 'Close']] = data[['Open', 'High', 'Low', 'Close']].round(2)

        # Calculate monthly change (%)
        data['Month_Change'] = ((data['Close'] - data['Open']) / data['Open']) * 100

        # Calculate monthly volatility (%)
        data['Month_Volatile'] = ((data['High'] - data['Low']) / data['Low']) * 100

        # Calculate cumulative yearly change (%)
        data['Yearly_Change'] = data['Month_Change'].cumsum()

        data['Month_Change'] = data['Month_Change'].round(2)  # Round to 2 decimal places
        data['Month_Volatile'] = data['Month_Volatile'].round(2)  # Round to 2 decimal places
        data['Yearly_Change'] = data['Yearly_Change'].round(2)  # Round to 2 decimal places

        # Add symbol column
        data['Symbol'] = symbol

       
                
        # Save to daily CSV (append mode)
        if not os.path.exists(DAILY_CSV_FILE_PATH):
            data.to_csv(DAILY_CSV_FILE_PATH, mode='w', index=False, header=True)
        else:
            data.to_csv(DAILY_CSV_FILE_PATH, mode='a', index=False, header=False)

        # Append to master CSV
        # Save to master CSV
        if not os.path.exists(MASTER_CSV_FILE_PATH):
            data.to_csv(MASTER_CSV_FILE_PATH, mode='w', index=False, header=True)
        else:
            data.to_csv(MASTER_CSV_FILE_PATH, mode='a', index=False, header=False)


        # Add summary data
        #avg_month_change = data['Month_Change'].mean()
        #avg_month_volatile = data['Month_Volatile'].mean()
        #last_yearly_change = data['Yearly_Change'].iloc[-1]
        # Add summary data
        avg_month_change = round(data['Month_Change'].mean(), 2)
        avg_month_volatile = round(data['Month_Volatile'].mean(), 2)
        last_yearly_change = round(data['Yearly_Change'].iloc[-1], 2)

        summary_data.append({
            "Symbol": symbol,
            "Yearly_Change": last_yearly_change,
            "Avg_Month_Change": avg_month_change,
            "Avg_Month_Volatile": avg_month_volatile,
        })

        log_message(f"Data for NSE {symbol} successfully processed.")
        time.sleep(0.7)  # Delay to avoid rate-limiting
    except Exception as e:
        log_message(f"Error processing {symbol}: {e}")

# Save summary statistics to a CSV file
try:
    pd.DataFrame(summary_data).to_csv(SUMMARY_CSV_FILE_PATH, index=False)
    log_message(f"Summary data saved to {SUMMARY_CSV_FILE_PATH}.")
except Exception as e:
    log_message(f"Error saving summary data: {e}")

# Save all daily data to Excel (single sheet)
try:
    log_message("Creating/updating Excel file with daily data...")
    if not os.path.exists(EXCEL_FILE_PATH):
        combined_data = pd.read_csv(DAILY_CSV_FILE_PATH)
        with pd.ExcelWriter(EXCEL_FILE_PATH, mode='w', engine='openpyxl') as writer:
            combined_data.to_excel(writer, sheet_name=f"Stock_{ist_now.strftime('%Y_%m')}", index=False)
    else:
        log_message("Excel file already exists; skipping creation.")
except Exception as e:
    log_message(f"Error creating/updating Excel file: {e}")


bigquery_client = bigquery.Client.from_service_account_json(SERVICE_ACCOUNT_FILE)
# Create BigQuery resources
create_bigquery_resources()

# Write all data to BigQuery
try:
    all_data = pd.read_csv(DAILY_CSV_FILE_PATH)
    write_to_bigquery(all_data)
except Exception as e:
    log_message(f"Error loading data from CSV to BigQuery: {e}")

log_message("All NSE tasks completed successfully.")
