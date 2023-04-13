from typing import final
import pandas as pd
import yahooquery as yq
from utils.screener_utils import Screener
from utils.maintainer_utils import Maintainer

screener = Screener(25,50,100,'1y', 'https://en.wikipedia.org/wiki/List_of_S%26P_500_companies')
maintainer = Maintainer(25,50,100,'1y')

# get price history
tickers_string = Screener.SP500_stocks_string(screener)
df = Screener.get_price_history(screener)
df.to_csv('price_history_download.csv')

# run maintainer function on current portfolio

Maintainer.maintain_portfolio(maintainer)
maintained_portfolio_df = pd.read_csv('portfolio.csv')
print('maintained portfolio df:')
print(maintained_portfolio_df)

# check if portfolio size = 5. If so end. Else continue
current_stocks_owned = Screener._count_stocks_owned(screener)

if current_stocks_owned == 5:
    raise Exception("Portfolio full. No need to buy stocks") 

else: # run screener function to identify new stocks and add them to portfolio.csv 
    Screener.df = pd.read_csv('price_history_download.csv')
    ma_df = Screener.calculate_MAs(screener, Screener.df)
    indicators_df = Screener.calculate_ma_indicators(screener, ma_df)
    latest_date_df = Screener.latest_date_df(screener, indicators_df)
    filtered_df = Screener.filter_on_criteria(screener)
    new_stocks = Screener.new_stocks_to_buy(screener)

final_portfolio_df = pd.read_csv('portfolio.csv')
print('final portfolio: ')
print(final_portfolio_df)