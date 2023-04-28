import os 
import datetime
import pandas as pd
import yahooquery as yq

def get_price_history(stock_url, stock_period) -> pd.DataFrame:
    """Get a price history from the yahooquery library using a period set out in the Screener init.
    Takes: A url of the stocks we want to take, the period of time to get stocks for. 
    Calls: function to get a list of stocks from a url. 
    Returns: A dataframe of the stock price history.
    Todo: Add a start date as well, e.g. I want price history for 1 year starting from 6 months ago and going back to 18 months from now"""

    tickers_string = SP500_stocks_string(stock_url)
    data = yq.Ticker(tickers_string)
    df = data.history(period = stock_period) 

    return df 

def check_datetime_of_price_history(path):
    """Get the date of the price_history_download.csv file. Used in main.py to prevent us recalling the Yahoo API if we already have downloaded the data today. 
    Takes: path of the price_history_download.csv
    Returns: Date in YYYY/MM/DD format as a string"""

    time = os.path.getmtime(path)
    date = datetime.datetime.fromtimestamp(time).date()
    str_date = str(date)
    return str_date

def SP500_stocks_string(stock_url) -> str:
    """"Get a list of stocks in the S&P500 using the wikipedia page. Potential to swap out the wikipedia pages for other stock markets """
    tickers = pd.read_html(stock_url)[0]['Symbol'].tolist()
    tickers_string = ' '.join(tickers).lower()

    return tickers_string


def _count_stocks_owned(portfolio_path):
    """Count how many stocks in the portfolio csv are currently owned.
    Function used to early screener early if our portfolio is full and to identify how many new stocks to buy if portfolio isn't full"""

    existing_portfolio_df = pd.read_csv(portfolio_path)
    row_count = len(existing_portfolio_df[existing_portfolio_df["currently_owned"]==True])
    print(f'row count of portfolio is {row_count}')
    return row_count