import yfinance as yf

msft = yf.Ticker("TSLA")

# get stock info

# get historical market data
data = msft.history(start="2020-04-20", end="2020-04-21")

print(data)