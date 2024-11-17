import yfinance as yf
from datetime import date
import gspread
from concurrent.futures import ThreadPoolExecutor, as_completed
from google.oauth2.service_account import Credentials

# Authenticate Google Sheets API
gc = gspread.service_account(filename="service_account.json")

# Authenticate with Google Drive API
scopes = ['https://www.googleapis.com/auth/drive']
credentials = Credentials.from_service_account_file(filename, scopes=scopes)
drive_service = build('drive', 'v3', credentials=credentials)

# Function to list all files accessible by the service account
def list_files():
    results = drive_service.files().list(pageSize=50, fields="files(id, name)").execute()
    files = results.get('files', [])

    if not files:
        print("No files found.")
    else:
        print("Files accessible by the service account:")
        for file in files:
            print(f"Name: {file['name']}, ID: {file['id']}")

# Call the function to list files
list_files()

# Function to find the file ID of a file by name
def get_file_id(file_name):
    query = f"name = '{file_name}'"
    results = drive_service.files().list(q=query, fields="files(id, name)").execute()
    files = results.get('files', [])
    if not files:
        print(f"No file named '{file_name}' found.")
        return None
    return files[0]['id']  # Return the first match's file ID

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

# Function to fetch data for a single symbol
def fetch_stock_data(symbol):
    try:
        print(f"Fetching data for {symbol}...")
        stock = yf.Ticker(symbol)
        info = stock.info

        # Extract selected fields
        info_row = [symbol] + [info.get(key, '') for key in headers]
        return info_row
    except Exception as e:
        print(f"Error fetching data for {symbol}: {e}")
        return [symbol] + ["Error"] * len(headers)

# Fetch data in parallel using ThreadPoolExecutor
def update_google_sheet(symbols):
    rows = []
    with ThreadPoolExecutor(max_workers=10) as executor:  # Use 10 workers for parallelism
        future_to_symbol = {executor.submit(fetch_stock_data, symbol): symbol for symbol in symbols}

        for future in as_completed(future_to_symbol):
            symbol = future_to_symbol[future]
            try:
                rows.append(future.result())
            except Exception as e:
                print(f"Error processing {symbol}: {e}")
    return rows

# Fetch and update data in the sheet
all_rows = update_google_sheet(symbols)
for row in all_rows:
    new_worksheet.append_row(row)

print(f"All data updated successfully in the sheet '{sheet_name}'.")
