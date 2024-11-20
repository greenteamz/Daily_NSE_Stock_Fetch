using yfinance what are all the information required to fetch to do this calculation

Using `yfinance`, you can fetch a wide array of financial data required to calculate key stock fundamentals. Here's a breakdown of the information you need and how to retrieve it:

---

### **1. Required Information and Their Purpose**

| **Metric**                | **Purpose**                                                                 | **How to Fetch**                                 |
|----------------------------|-----------------------------------------------------------------------------|-------------------------------------------------|
| **Stock Price**            | Used for P/E, Dividend Yield, and general valuation.                       | `ticker.history(period="1d")['Close']`          |
| **Earnings Per Share (EPS)** | Used for P/E calculation.                                                  | `ticker.info['trailingEps']`                   |
| **Shares Outstanding**     | Used for EPS and market cap calculations.                                  | `ticker.info['sharesOutstanding']`             |
| **Total Revenue**          | Indicates the company's revenue generation capability.                     | `ticker.financials.loc['Total Revenue']`       |
| **Net Income**             | Used for ROE and EPS calculations.                                         | `ticker.financials.loc['Net Income']`          |
| **Shareholders' Equity**   | Used for ROE and BVPS calculations.                                        | `ticker.balance_sheet.loc['Total Stockholder Equity']` |
| **Total Debt**             | Used for Debt-to-Equity ratio.                                             | `ticker.balance_sheet.loc['Total Debt']`       |
| **Dividends**              | Used to calculate Dividend Yield.                                          | `ticker.dividends`                             |
| **Free Cash Flow (FCF)**   | Measures financial flexibility and growth potential.                       | Derived from cash flow statement: <br> Operating Cash Flow - Capital Expenditures |
| **Book Value**             | Used for BVPS calculation.                                                 | `ticker.balance_sheet.loc['Total Assets'] - ticker.balance_sheet.loc['Total Liabilities']` |

---

### **2. Fetching Data with yfinance**

Here’s how to retrieve these metrics using `yfinance`:

#### **Install yfinance**:
```bash
pip install yfinance
```

#### **Example Script**:
```python
import yfinance as yf

# Fetch stock data for Infosys
ticker = yf.Ticker("INFY.NS")  # NSE stock ticker

# 1. Current Stock Price
price = ticker.history(period="1d")['Close'].iloc[-1]

# 2. EPS
eps = ticker.info.get('trailingEps', None)

# 3. P/E Ratio
pe_ratio = ticker.info.get('trailingPE', None)

# 4. Shares Outstanding
shares_outstanding = ticker.info.get('sharesOutstanding', None)

# 5. Total Revenue
total_revenue = ticker.financials.loc['Total Revenue'][0]

# 6. Net Income
net_income = ticker.financials.loc['Net Income'][0]

# 7. Total Debt
total_debt = ticker.balance_sheet.loc['Total Debt'][0]

# 8. Shareholder's Equity
shareholders_equity = ticker.balance_sheet.loc['Total Stockholder Equity'][0]

# 9. Dividend Yield
dividend_yield = ticker.info.get('dividendYield', 0) * 100

# 10. Free Cash Flow (Derived)
cash_flow = ticker.cashflow
operating_cash_flow = cash_flow.loc['Total Cash From Operating Activities'][0]
capital_expenditures = cash_flow.loc['Capital Expenditures'][0]
free_cash_flow = operating_cash_flow + capital_expenditures  # CapEx is negative

# Print Results
print(f"Price: {price}")
print(f"EPS: {eps}")
print(f"P/E Ratio: {pe_ratio}")
print(f"Total Revenue: {total_revenue}")
print(f"Net Income: {net_income}")
print(f"Total Debt: {total_debt}")
print(f"Shareholder's Equity: {shareholders_equity}")
print(f"Dividend Yield: {dividend_yield}%")
print(f"Free Cash Flow: {free_cash_flow}")
```

---

### **3. Calculations Based on Fetched Data**

#### **a. Price-to-Earnings Ratio (P/E):**
\[
\text{P/E} = \frac{\text{Price}}{\text{EPS}}
\]

#### **b. Return on Equity (ROE):**
\[
\text{ROE} = \frac{\text{Net Income}}{\text{Shareholder's Equity}}
\]

#### **c. Debt-to-Equity Ratio (D/E):**
\[
\text{D/E} = \frac{\text{Total Debt}}{\text{Shareholder's Equity}}
\]

#### **d. Dividend Yield:**
\[
\text{Dividend Yield} = \frac{\text{Annual Dividend}}{\text{Stock Price}} \times 100
\]

#### **e. Book Value Per Share (BVPS):**
\[
\text{BVPS} = \frac{\text{Shareholder's Equity}}{\text{Shares Outstanding}}
\]

---

### **4. Example Calculation Output**
With Infosys data (hypothetical values):
- **Stock Price:** ₹1,500
- **EPS:** ₹60
- **Net Income:** ₹20,000 crore
- **Shareholder's Equity:** ₹1,00,000 crore
- **Shares Outstanding:** 2,000 crore
- **Dividends:** ₹30/share
- **Total Debt:** ₹10,000 crore

Calculations:
1. **P/E Ratio:** \( 1500 / 60 = 25 \)
2. **ROE:** \( 20000 / 100000 = 20\% \)
3. **D/E Ratio:** \( 10000 / 100000 = 0.1 \)
4. **Dividend Yield:** \( (30 / 1500) \times 100 = 2\% \)
5. **BVPS:** \( 100000 / 2000 = ₹50 \)

---

Would you like further assistance on scripting, interpreting results, or automating analysis for multiple stocks?
