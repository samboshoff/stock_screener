import pandas as pd

from stockscreener.utils.general_utils import get_price_history
from utils.maintainer_utils import Maintainer
from utils.screener_utils import Screener

# 1) set variables for tester
bt_portfolio_path = 'backtest_portfolio.csv'
bt_price_history_path = 'bt_price_history_download.csv'
stock_period = '1y'
start_date = '2022/04/27'
fast_ma = 25
mid_ma = 50
slow_ma = 100
stock_list_url = 'https://en.wikipedia.org/wiki/List_of_S%26P_500_companies'

bt_price_history_df = pd.read_csv(bt_price_history_path)

# 2) create price_history_download from start date to end_date. 



# 3) iterate through each date in price_history 
price_history_df_dates = bt_price_history_df['date']

# for date in price_history_df_dates: 
#     maintainer = Maintainer(fast_ma, mid_ma, slow_ma, stock_period, bt_portfolio_path, bt_price_history_path, date)
#     df  = price_history_df[date]

#     screener = Screener(fast_ma, mid_ma, slow_ma, stock_period, bt_portfolio_path)

if __name__ == "__main__":
    print(price_history_df_dates)
