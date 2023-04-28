import os 
import datetime
import pandas as pd
import yahooquery as yq
import time

from utils.screener_utils import Screener
from utils.maintainer_utils import Maintainer
from utils.general_utils import get_price_history, check_datetime_of_price_history, _count_stocks_owned

"""This is the script for maintaining the portfolio and running the stock screener on a daily basis"""

# set variables for classes:
fast_ma = 25
mid_ma = 50
slow_ma = 100
stock_period = '1y'
stock_list_url = 'https://en.wikipedia.org/wiki/List_of_S%26P_500_companies'
portfolio_path = 'files/portfolio.csv'
price_history_path = 'files/price_history_download.csv'

# Update price_history_download.csv if the date it was created != today's date
if datetime.datetime.today().strftime('%Y-%m-%d') != check_datetime_of_price_history(price_history_path):
    print('updating price_history_csv')
    df = get_price_history(stock_list_url, stock_period)
    df.to_csv(price_history_path)

# run maintainer function on current portfolio if we own one or more stocks
if _count_stocks_owned(portfolio_path) > 0:
    maintainer = Maintainer(fast_ma, mid_ma, slow_ma, stock_period, portfolio_path, price_history_path)
    Maintainer.maintain_portfolio(maintainer)
    maintained_portfolio_df = pd.read_csv(portfolio_path)
    print(f'maintained portfolio df: \n {maintained_portfolio_df}')
    time.sleep(2)

# check if portfolio size = 5. If so end. Else screen for new stocks and add to portfolio.cs
if _count_stocks_owned(portfolio_path) == 5:
    raise Exception("Portfolio full. No need to buy stocks") 

else: 
    screener = Screener(fast_ma, mid_ma, slow_ma, stock_period, portfolio_path)
    price_history_df = pd.read_csv(price_history_path)
    screener.screener_for_new_stocks(price_history_df)
    screener.new_stocks_to_buy()

final_portfolio_df = pd.read_csv(portfolio_path)
print(f'final portfolio: \n {final_portfolio_df}')