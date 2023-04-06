import pandas as pd
import yahooquery as yq
from utils.yahooquery_utils import Screener, _count_stocks_owned

screener = Screener(25,50,100,'1y', 'https://en.wikipedia.org/wiki/List_of_S%26P_500_companies')

tickers_string = Screener.SP500_stocks_string(screener)
print(tickers_string)

 # start by reading stocks owned csv. If count of stocks is greater than 5 then don't run buy criteria. Instead skip to maintainer functions. 
stocks_csv = ''
current_stocks_owned = _count_stocks_owned()

if row_count of filtered_df > 0:
    new_stocks_to_buy

else: 
    print('no good opportunities to buy')