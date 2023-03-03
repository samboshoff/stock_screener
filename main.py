import pandas as pd
import yahooquery as yq
from utils.yahooquery_utils import Screener

screener = Screener(25,50,100,'1y', 'https://en.wikipedia.org/wiki/List_of_S%26P_500_companies')

tickers_string = Screener.SP500_stocks_string(screener)
print(tickers_string)