import yfinance as yf
import pandas as pd
from datetime import date, datetime
import os
import csv

# List all the Excel files in the current directory
excel_files = [f for f in os.listdir() if f.endswith('.xlsx')]

# If there are no Excel files, print a message and exit
if not excel_files:
    print("No Excel files found in the directory.")
    exit()

# Print all Excel files found in the directory
print("Available Excel files:")
for i, file in enumerate(excel_files, start=1):
    print(f"{i}. {file}")

# Ask the user to choose an Excel file from the list
try:
    choice = int(input(f"Enter the number corresponding to the Excel file you want to use (1-{len(excel_files)}): "))
    selected_file = excel_files[choice - 1]
except (ValueError, IndexError):
    print("Invalid choice. Exiting the program.")
    exit()

# Load the selected Excel file using pandas
try:
    source_worksheet = pd.read_excel(selected_file, sheet_name='symbol')
except Exception as e:
    print(f"Error: {e}")
    exit()

# Fetch all stock symbols from the first column
symbols = source_worksheet['Symbol'].dropna().tolist()  # Drop NaN values and get the list of symbols

# Print the total number of symbols
total_symbols = len(symbols)
print(f"Total number of symbols: {total_symbols}")

# Add '.NS' suffix to all symbols
symbols = [symbol if symbol.endswith('.NS') else f"{symbol}.NS" for symbol in symbols]

# Get today's date for the new file name
today = date.today().strftime('%d_%m_%Y')
file_base_name = f"Yearly_Data_{today}.txt"

# Create a new file if it doesn't exist
with open(file_base_name, mode='w', newline='', encoding='utf-8') as file:
    writer = csv.writer(file)
    
    # Write the headers to the file
    headers = ['Date', 'Open', 'High', 'Low', 'Close', 'Adj Close', 'Volume', 'Ticker']
    writer.writerow(headers)
    
    # Fetch and write data for each symbol
    try:
        for symbol in symbols:
            print(f"Fetching data for {symbol}...")
            data = yf.download(tickers=symbol, period="max", interval="1d", auto_adjust=True)
            if data.empty:
                print(f"No data found for {symbol}. Skipping...")
                continue

            # Strip timezone info from the Date column
            data.index = data.index.tz_localize(None)

            # Add a column for the ticker symbol
            data['Ticker'] = symbol
            data.reset_index(inplace=True)

            # Write the data to the CSV file
            for _, row in data.iterrows():
                writer.writerow(row.tolist())

        print(f"Yearly data successfully written to '{file_base_name}'.")
    except Exception as e:
        print(f"Error fetching data: {e}")
