import re
import pandas as pd
from utils.df_utils import Moving_Average, _latest_date_df


class Maintainer:
    def __init__(self, fast_ma: int, mid_ma: int, slow_ma: int, stock_period: str, portfolio_path: str, price_history_path: str, date):
        self.fast_ma = fast_ma
        self.mid_ma = mid_ma
        self.slow_ma = slow_ma
        self.stock_period = stock_period
        self.date = date
        self.portfolio_path = portfolio_path
        self.existing_portfolio_df = pd.read_csv(self.portfolio_path)
        self.price_history_path = price_history_path
        self.price_history_df = pd.read_csv(price_history_path)
        self.owned_portfolio_stocks = self.existing_portfolio_df[self.existing_portfolio_df["currently_owned"]==True].copy()
        self.list_of_owned_portfolio_stocks = self.owned_portfolio_stocks['symbol'].tolist()

        self.moving_average = Moving_Average(self.fast_ma, self.mid_ma, self.slow_ma)

    def _price_history_for_portfolio_dataframe(self) -> pd.DataFrame:
        """Filter the price_history_df for only the stocks we already hold in our portfolio"""

        self.price_history_for_portfolio_df = self.price_history_df[self.price_history_df['symbol'].isin(self.list_of_owned_portfolio_stocks)]
        return self.price_history_for_portfolio_df

    def maintain_portfolio(self) -> list: 
        """
        Take the price_history_df and my list of portfolio stocks and run maintain criteria on them. 
        """
        self.price_history_for_portfolio_df = self._price_history_for_portfolio_dataframe()

        # create ma's and indicators using functions in screener_utils. Todo: create different indicators for maintaining? 
        self.portfolio_latest_date_with_indicators_df = Maintainer._create_MAs_and_indicators(self, self.price_history_for_portfolio_df)

        # apply criteria filters here. 
        self.list_of_stocks_to_be_sold = self._maintainer_filter_on_criteria(self.portfolio_latest_date_with_indicators_df)

        # update the portfolio.csv
        if len(self.list_of_stocks_to_be_sold) == False:
            return print('No stocks need to be sold')

        else: 
            print("stocks need to be sold")
            self.updated_portfolio_df = self._update_portfolio_csv()
            print(self.updated_portfolio_df)
            self.updated_portfolio_df.to_csv(self.portfolio_path, index=False)

    def _create_MAs_and_indicators(self, price_history_for_portfolio_df: pd.DataFrame) -> pd.DataFrame:
        """Orchestrator to create the MA and indicator columns, and return the latest date by calling
        the same functions from the screener_utils.py file.
        Return a dataframe with the indicators and filtered to the latest date."""
        portfolio_ma_df = self.moving_average._calculate_MAs(price_history_for_portfolio_df)
        portfolio_indicators_df = self.moving_average._calculate_ma_indicators(portfolio_ma_df)
        portfolio_latest_df = _latest_date_df(portfolio_indicators_df)
        
        return portfolio_latest_df

    def _maintainer_filter_on_criteria(self, latest_date_df: pd.DataFrame) -> pd.DataFrame:
        """"Apply the maintainer based on the indicators we have made. 
        Criteria 1: fast_ma is lower than slow_ma. Reversal of the trend. 
        Criteria 2: fast_ma is lower than close price. Stock trending downwards? 
        Return a list of stocks to be sold. We will use this list to update our portfolio.csv. 
        
        Todo: iterate through using historic data to find the best threshold rather than picking arbitrarily."""

        filtered_df = latest_date_df[
        (latest_date_df[f'pc_sma{self.fast_ma}_by_sma{self.slow_ma}']<0 
        & (latest_date_df[f'pc_sma{self.fast_ma}_by_close']<0)
        & (latest_date_df[f'pc_sma{self.fast_ma}_by_close']>5))
        ]

        self.list_of_stocks_to_be_sold = filtered_df['symbol'].tolist()

        return self.list_of_stocks_to_be_sold
        
    def _update_portfolio_csv(self):
        """Update the portfolio.csv to convert stocks that should be sold to 'currently_owned' = False.
        Add a date in the 'date sold' column. We want to keep a history of stocks we've bought and sold rather than just dropping the stocks from the csv. 
        Takes: 
        1) list of stocks to be sold 
        2) portfolio.csv
        3) portfolio_latest_date_with_indicators_df 
        Returns: a csv of the same name with updated data in rows"""

        # match rows where dataframe in list into a separate dataframe
        self.stocks_to_be_sold_df = self.existing_portfolio_df[self.existing_portfolio_df['symbol'].isin(self.list_of_stocks_to_be_sold)].copy()

        # change value of currently_owned from True to False for those stocks to be sold
        self.stocks_to_be_sold_df['currently_owned'] = False

        # add date sold
        if self.portfolio_path == 'files/portfolio.csv':
            self.stocks_to_be_sold_df['date_sold'] = pd.Timestamp.today().strftime('%Y-%m-%d')
        
        # conditional case for the backtester
        else:
            self.stocks_to_be_sold_df['date_sold'] = self.date

        # add sell_price
        self.stocks_to_be_sold_df = self._add_sell_price()

        # create copy of portfolio with stocks to keep 
        stocks_to_keep_df = self.existing_portfolio_df[~self.existing_portfolio_df['symbol'].isin(self.list_of_stocks_to_be_sold)].copy()

        # concat stocks to keep with stocks to sell
        self.updated_portfolio_df = pd.concat([self.stocks_to_be_sold_df, stocks_to_keep_df], ignore_index=True)
    
        return self.updated_portfolio_df

    def _add_sell_price(self):
        """
        Take: list of stocks to be sold, portfolio_latest_date_with_indicators_df. 
        then pull out the adjclose for those stocks. concat that back to stocks_to_be_sold_df. 
        """
        portfolio_with_sell_price_df = self.portfolio_latest_date_with_indicators_df[self.portfolio_latest_date_with_indicators_df['symbol'].isin(self.list_of_stocks_to_be_sold)].copy()
        portfolio_with_sell_price_df = portfolio_with_sell_price_df[['symbol', 'adjclose']]

        # join sell price to stocks to be sold df
        stocks_to_be_sold_with_sell_price_df = self.stocks_to_be_sold_df.merge(portfolio_with_sell_price_df, on='symbol')
        stocks_to_be_sold_with_sell_price_df['sell_price'] = stocks_to_be_sold_with_sell_price_df['adjclose'] 
        stocks_to_be_sold_with_sell_price_df = stocks_to_be_sold_with_sell_price_df.drop('adjclose', axis=1)

        return stocks_to_be_sold_with_sell_price_df

if __name__ == "__main__":
    maintainer = Maintainer(25,50,100,'1y')

    Maintainer.maintain_portfolio(maintainer)

    # filtered_df = Maintainer._price_history_for_portfolio_dataframe(maintainer)
    # portfolio_ma_df = Maintainer.check_status_of_portfolio(maintainer)
    # print(portfolio_ma_df)