import yfinance as yf
from datetime import datetime, date
import gspread
from google.oauth2.service_account import Credentials

# Set up logging
LOG_FILE = "update_log.txt"

def log_message(message):
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    with open(LOG_FILE, "a") as log_file:
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

# Define the sheet name for today's data
sheet_name = f"NSE_{date.today().strftime('%d_%m_%Y')}"

# Check if the sheet already exists
existing_sheet_names = [ws.title for ws in spreadsheet.worksheets()]
if sheet_name in existing_sheet_names:
    spreadsheet.del_worksheet(spreadsheet.worksheet(sheet_name))

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

# Function to fetch and update data for a single symbol
def fetch_and_update_stock_data(symbol, worksheet):
    try:
        log_message(f"Fetching data for {symbol}...")
        stock = yf.Ticker(symbol)
        info = stock.info

        # Extract selected fields
        info_row = [symbol] + [info.get(key, '') for key in headers]
        worksheet.append_row(info_row)
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
print(f"Log saved to {LOG_FILE}.")
