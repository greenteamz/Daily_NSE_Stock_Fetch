import gspread

try:
    # Authenticate using service account
    gc = gspread.service_account("service_account.json")
    print("Authentication successful!")

    # List accessible spreadsheets
    sheets = gc.openall()
    print("Accessible Spreadsheets:")
    for sheet in sheets:
        print(sheet.title)
except Exception as e:
    print(f"Error: {e}")
