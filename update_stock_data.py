import yfinance as yf
from datetime import datetime, date
import gspread
from google.oauth2.service_account import Credentials
import os
import subprocess

# Generate log file name with date and time
log_filename = f"log_{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}.txt"
csv_filename = f"symbol_data_{datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}.csv"

# Set log and CSV file paths in the Git project
LOG_FILE_PATH = os.path.join("logs", log_filename)
CSV_FILE_PATH = os.path.join("csv", csv_filename)

# Ensure the log and CSV directories exist, create them if they don't
os.makedirs("logs", exist_ok=True)
os.makedirs("csv", exist_ok=True)

def log_message(message):
    """Write messages to log file and print them to console."""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    with open(LOG_FILE_PATH, "a") as log_file:
        log_file.write(f"[{timestamp}] {message}\n")
    print(f"[{timestamp}] {message}")

# Authenticate Google Sheets API
gc = gspread.service_account(filename="service_account.json")

# Open the Google Spreadsheet (workbook) named 'NSE_symbol'
spreadsheet = gc.open('NSE_symbol')  # Replace 'NSE_symbol' with the actual workbook name

# Access the worksheet (tab) named 'symbol'
source_worksheet = spreadsheet.worksheet('symbol')  # Access the 'symbol' sheet

# Fetch all stock symbols from the first column
symbols = source_worksheet.col_values(1)[1:]  # Skip the header row

# Add '.NS' suffix to all symbols if not present
symbols = [symbol if symbol.endswith('.NS') else f"{symbol}.NS" for symbol in symbols]

# Define the sheet name for today's data with date and time
sheet_name = f"NSE_{date.today().strftime('%d_%m_%Y')}_{datetime.now().strftime('%H-%M-%S')}"

# Check if the sheet already exists
#existing_sheet_names = [ws.title for ws in spreadsheet.worksheets()]
#if sheet_name in existing_sheet_names:
#    spreadsheet.del_worksheet(spreadsheet.worksheet(sheet_name))

# Create a new sheet for today's data
new_worksheet = spreadsheet.add_worksheet(title=sheet_name, rows="1000", cols="100")

# Define the headers for the Google Sheet
headers = [
    "Market Cap", "industry", "sector", "fullTimeEmployees", "auditRisk", "boardRisk",
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

# Set the headers in the Google Sheet
new_worksheet.append_row(['Symbol'] + headers)

# Write headers to CSV file
with open(CSV_FILE_PATH, "w") as csv_file:
    csv_file.write(",".join(['Symbol'] + headers) + "\n")

# Function to fetch and update data for a single symbol
def fetch_and_update_stock_data(symbol, worksheet):
    try:
        log_message(f"Fetching data for {symbol}...")
        stock = yf.Ticker(symbol)
        info = stock.info

        # Extract selected fields
        info_row = [symbol] + [str(info.get(key, '')) for key in headers]
        worksheet.append_row(info_row)

        # Append row to CSV file
        with open(CSV_FILE_PATH, "a") as csv_file:
            csv_file.write(",".join(info_row) + "\n")

        log_message(f"Successfully updated data for {symbol}.")
    except Exception as e:
        log_message(f"Error fetching data for {symbol}: {e}")

# Process each symbol
processed_count = 0
for symbol in symbols:
    fetch_and_update_stock_data(symbol, new_worksheet)
    processed_count += 1
    log_message(f"Processed {processed_count}/{len(symbols)} symbols.")

log_message(f"All data updated successfully in the sheet '{sheet_name}'.")

# Commit the files to Git (if they're not already committed)
def git_commit(file_path):
    try:
        # Add the file to Git and commit it
        subprocess.run(['git', 'add', file_path], check=True)
        subprocess.run(['git', 'commit', '-m', f"Added {file_path}"], check=True)
        subprocess.run(['git', 'push'], check=True)
        log_message(f"Successfully committed {file_path} to Git.")
    except subprocess.CalledProcessError as e:
        log_message(f"Error committing {file_path} to Git: {e}")

# Commit both log and CSV files
git_commit(LOG_FILE_PATH)
git_commit(CSV_FILE_PATH)

print(f"Log saved to {LOG_FILE_PATH}.")
print(f"CSV data saved to {CSV_FILE_PATH}.")
