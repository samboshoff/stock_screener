import yfinance as yf

msft = yf.Ticker("MSFT")

# get historical market data
hist = msft.history(period="5d")

print(type(hist))