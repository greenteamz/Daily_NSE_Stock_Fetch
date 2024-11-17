import yfinance as yf
from datetime import date, datetime
import pandas as pd
import os
import time

# Input: Local Excel file containing stock symbols
excel_file = "NSE_symbol_daily.xlsx"  # Replace with your actual file name

# Check if the Excel file exists
if not os.path.exists(excel_file):
    print(f"Error: '{excel_file}' not found in the current directory.")
    exit()

# Load the Excel file and read the 'symbol' sheet
try:
    source_df = pd.read_excel(excel_file, sheet_name='symbol')
    symbols = source_df['Symbol'].dropna().tolist()  # Get all non-empty symbols
except Exception as e:
    print(f"Error reading '{excel_file}': {e}")
    exit()

# Print the total number of symbols
total_symbols = len(symbols)
print(f"Total number of symbols: {total_symbols}")

# Add '.NS' suffix to all symbols if not present
symbols = [symbol if symbol.endswith('.NS') else f"{symbol}.NS" for symbol in symbols]

# Output filenames
today = date.today().strftime('%d_%m_%Y')
excel_output_file = f"NSE_Stock_Data_{today}.xlsx"
text_output_file = f"NSE_stock_data_{today}.txt"

# Define headers
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

# Create a new DataFrame for storing results
results_df = pd.DataFrame(columns=['Symbol'] + headers)

# Create the text output file and write the header row
with open(text_output_file, "w") as txt_file:
    txt_file.write(",".join(['Symbol'] + headers) + "\n")

# Fetch and save data for each symbol
for symbol in symbols:
    try:
        print(f"Fetching data for {symbol}...")
        stock = yf.Ticker(symbol)
        info = stock.info  # Fetch stock information

        # Extract required fields
        info_row = [symbol] + [info.get(key, '') for key in headers]

        # Append data to the DataFrame
        results_df.loc[len(results_df)] = info_row

        # Append data to the text file
        with open(text_output_file, "a") as txt_file:
            txt_file.write(",".join(map(str, info_row)) + "\n")

        print(f"Data for {symbol} saved successfully.")

        # Add a delay to avoid rate-limiting
        time.sleep(10)
    except Exception as e:
        print(f"Error fetching data for {symbol}: {e}")

# Save the results DataFrame to an Excel file
results_df.to_excel(excel_output_file, index=False)
print(f"Data successfully saved to '{excel_output_file}' and '{text_output_file}'.")
