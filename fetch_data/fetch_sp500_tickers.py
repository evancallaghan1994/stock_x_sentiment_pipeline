import pandas as pd

# URL of the Wikipedia page with S&P 500 companies
url = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"

# Read all tables from the page
tables = pd.read_html(url)

# The first table is the S&P 500 constituents table
sp500_table = tables[0]

# Keep only relevant columns (Symbol and Security name)
sp500_tickers = sp500_table[['Symbol', 'Security']]

# Optionally save for reuse
sp500_tickers.to_csv("sp500_tickers.csv", index=False)
sp500_tickers.to_json("sp500_tickers.json", orient="records", lines=True)

print(f"Pulled {len(sp500_tickers)} S&P 500 companies")
print(sp500_tickers.head())
