import pandas as pd
import yahooquery as yq
from df_utils import Moving_Average, _calculate_pc_ma, _calculate_pc_ma_difference, _latest_date_df
from general_utils import _count_stocks_owned

class Screener:
    def __init__(self, fast_ma: int, mid_ma: int, slow_ma: int, stock_period: str, portfolio_path: str):
        self.fast_ma = fast_ma
        self.mid_ma = mid_ma
        self.slow_ma = slow_ma
        self.stock_period = stock_period
        self.porfolio_path = portfolio_path
        self.existing_portfolio_df = pd.read_csv(self.porfolio_path)
    
    def screener_for_new_stocks(self, price_history_df):
        """Orchestrater function to:
        1) take the price history for stock data
        2) Calculate MAs
        3) Calculate indicators based on MAs
        4) Take the latest date from that data
        5) Apply the criteria to filter down the stocks 
        
        Takes: price_history_df
        Returns: filtered dataframe with only stocks that have passed the criteria and should be bought"""

        moving_average = Moving_Average(self.fast_ma, self.mid_ma, self.slow_ma)
        ma_df = moving_average._calculate_MAs(price_history_df)
        indicators_df = moving_average._calculate_ma_indicators(ma_df)
        self.latest_day_df = _latest_date_df(indicators_df)
        self.screened_df = self._screener_filter_on_criteria()
        return self.screened_df
    
    def new_stocks_to_buy(self): 
        """Identifies the stocks to buy if the currently owned stocks is less than 5. 
        If current stocks owned is less than 5: then add stocks to stocks_owned csv
        Takes: Screened stocks df, existing portfolio df
        Returns: Nothing. Adds new stocks to portfolio.csv 
        """

        self.row_count = _count_stocks_owned(self.porfolio_path)
        if self.row_count >=5:
            raise Exception("Number of currently owned stocks is 5. No need to add new stocks")


        else:
            number_of_new_stocks_needed = 5 - self.row_count

            # _filter_for_unique_stocks()
            new_screened_stocks_df = self._only_take_unique_stocks(self.screened_df, self.existing_portfolio_df)

            # we only want to hold 5 stocks so take the best stocks from filtered_df and only take symbol column
            self.top_screened_stocks_df = new_screened_stocks_df.head(number_of_new_stocks_needed).copy()
            self.top_screened_stocks_df['date_bought'] = self.top_screened_stocks_df['date']
            self.top_screened_stocks_df['buy_price'] = self.top_screened_stocks_df['adjclose']
            self.top_screened_stocks_df['date_sold'] = ''
            self.top_screened_stocks_df['sell_price'] = ''
            self.top_screened_stocks_df['currently_owned'] = True

            # append top stocks to dataframe, append if csv already exists
            self.stocks_to_buy_df = self.top_screened_stocks_df[['symbol', 'date_bought', 'buy_price', 'date_sold', 'sell_price', 'currently_owned']].copy()

            self._reupload_current_stocks_df()

    def _screener_filter_on_criteria(self) -> pd.DataFrame:
        """"Apply the screener based on the indicators we have made. 
        Criteria 1: fast_ma is greater than slow_ma. Have picked that it should be 5% greater arbitrarily.
        Criteria 2: fast_ma is greater than close price. Perhaps indicates that a stock is undervalued on a given day due to short term causes.
        
         Todo: iterate through using historic data to find the best threshold rather than picking arbitrarily."""

        filtered_df = self.latest_day_df[
        (self.latest_day_df[f'pc_sma{self.fast_ma}_by_sma{self.slow_ma}']>5 
        & (self.latest_day_df[f'pc_sma{self.fast_ma}_by_close']<-0)
        & (self.latest_day_df[f'pc_sma{self.fast_ma}_by_close']>-5))
        ]

        filtered_df = filtered_df.sort_values(f'pc_sma{self.fast_ma}_by_close')
        return filtered_df

    def _reupload_current_stocks_df(self):
        """Take current stocks owned csv and concat to new stocks to buy. Then overwrite to 'portfolio.csv'"""

        unioned_df = pd.concat([self.existing_portfolio_df, self.stocks_to_buy_df], ignore_index=True)
        unioned_df = unioned_df[['symbol', 'date_bought', 'buy_price', 'date_sold', 'sell_price', 'currently_owned']]
        return unioned_df.to_csv(self.porfolio_path, index=False)

    def _only_take_unique_stocks(self, identified_stocks_df, portfolio_df):
        """Take two dataframes and filter identified_stocks_df to only include stocks that aren't in portfolio_df
        Filter portfolio_df to only include currently owned stocks (currently_owned == True).
        Left outer join on 'symbol'.
        Do this to prevent us adding a stock we already own.
        Todo: maybe remove this unique filter and replace by doubling down on a position if indicators point to it being a better bet. 
        """

        currently_owned_portfolio_df = portfolio_df[portfolio_df["currently_owned"]==True].copy()

        new_identified_stocks_df = pd.merge(identified_stocks_df, currently_owned_portfolio_df, on=['symbol'], how='outer', indicator=True).query('_merge=="left_only"')
        return new_identified_stocks_df


if __name__ == "__main__":
    portfolio_path = 'files/portfolio.csv'
    price_history_path = 'files/price_history_download.csv'

    screener = Screener(25,50,100,'1y', 'https://en.wikipedia.org/wiki/List_of_S%26P_500_companies', portfolio_path)

    price_history_df = pd.read_csv(price_history_path)
    screener.screener_for_new_stocks(price_history_df)
    screener.new_stocks_to_buy()