import pandas as pd
from utils.yahooquery_utils import Screener

def check_status_of_portfolio(price_history_df: pd.DataFrame, list_of_portfolio_stocks:list) -> list: 
    """
    Take the price_history_df and my list of portfolio stocks and run maintain criteria on them. 
    """
    list_of_stocks_to_sell = []
    
    portfolio_df = _filter_dataframe(price_history_df, list_of_portfolio_stocks)

    #apply criteria filters here 

    #compare two dataframes and return the stocks that got dropped from the criteria i.e. no longer want to hold. 

    return list_of_stocks_to_sell 


def _filter_dataframe(price_history_df: pd.DataFrame, list_of_portfolio_stocks:list) -> pd.DataFrame:

    filtered_df = price_history_df[price_history_df.index.isin(list_of_portfolio_stocks, level=0)]

    return filtered_df