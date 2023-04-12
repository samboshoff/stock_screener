import pandas as pd
import yahooquery as yq

class Screener:
    def __init__(self, fast_ma: int, mid_ma: int, slow_ma: int, stock_period: str, stock_url:str):
        self.fast_ma = fast_ma
        self.mid_ma = mid_ma
        self.slow_ma = slow_ma
        self.stock_period = stock_period
        self.stock_url = stock_url
        self.existing_portfolio_df = pd.read_csv('porfolio.csv')

    def SP500_stocks_string(self) -> str:
        """"Get a list of stocks in the S&P500 using the wikipedia page. Potential to swap out the wikipedia pages for other stock markets """
        tickers = pd.read_html(self.stock_url)[0]['Symbol'].tolist()
        self.tickers_string = ' '.join(tickers).lower()

        return self.tickers_string

    def get_price_history(self) -> pd.DataFrame:
        """Get a price history from the yahooquery library using a period set out in the Screener init.
        Todo: Add a start date as well, e.g. I want price history for 1 year starting from 6 months ago and going back to 18 months from now"""

        data = yq.Ticker(self.tickers_string)
        self.df = data.history(period = self.stock_period) 

        return self.df 

    def calculate_MAs(self, df) -> pd.DataFrame:
        """Calculate the moving averages and indicators"""
        self.ma_df = df.copy()

        self.ma_df['rolling_30day_high'] = self.ma_df['high'].rolling(30).max()
        self.ma_df[f'sma{self.fast_ma}'] = self.ma_df['adjclose'].rolling(self.fast_ma).mean()
        self.ma_df[f'sma{self.mid_ma}'] = self.ma_df['adjclose'].rolling(self.mid_ma).mean()
        self.ma_df[f'sma{self.slow_ma}'] = self.ma_df['adjclose'].rolling(self.slow_ma).mean()

        return self.ma_df

    def calculate_ma_indicators(self, ma_df) -> pd.DataFrame:
        """Calculate moving average indicators
        1: percentage difference of fast_ma and yesterdays's close price
        2: percentage difference of mid_ma and yesterdays's close price
        3: percentage difference of fast_ma and slow_ma
        4: mean of the mid_ma from the last 5 days. 
        """
        individual_df_list = []
        
        for symbol, new_df in ma_df.groupby(by='symbol'):
            new_df = new_df.sort_index().tail(5)

            new_df[f'pc_sma{self.fast_ma}_by_close'] = new_df.apply(lambda x: _calculate_pc_ma(x[f'sma{self.fast_ma}'], x['adjclose']), axis=1)
            new_df[f'pc_sma{self.mid_ma}_by_close'] = new_df.apply(lambda x: _calculate_pc_ma(x[f'sma{self.mid_ma}'], x['adjclose']), axis=1)
            new_df[f'pc_sma{self.fast_ma}_by_sma{self.slow_ma}'] = new_df.apply(lambda x: _calculate_pc_ma_difference(x[f'sma{self.mid_ma}'], x[f'sma{self.slow_ma}']), axis=1)
            new_df[f'mean_of_sma{self.mid_ma}_by_close_last_5_days'] = new_df[f'pc_sma{self.mid_ma}_by_close'].mean()

            individual_df_list.append(new_df)

        self.joined_df = pd.concat(individual_df_list)

        return self.joined_df

    def latest_date_df(self, indicators_df) -> pd.DataFrame: 
        """"Filter the dataframe to only take the most recent date of data after having calculated the indicators"""

        df_list = []

        for symbol, new_df in self.joined_df.groupby(by='symbol'):
            new_df = new_df.sort_index().tail(1)
            df_list.append(new_df)

        self.latest_day_df = pd.concat(df_list)

        return self.latest_day_df

    def screener_filter_on_criteria(self) -> pd.DataFrame:
        """"Apply the screener based on the indicators we have made. 
        Criteria 1: fast_ma is greater than slow_ma. Have picked that it should be 5% greater arbitrarily.
        Criteria 2: fast_ma is greater than close price. Perhaps indicates that a stock is undervalued on a given day due to short term causes.
        
         Todo: iterate through using historic data to find the best threshold rather than picking arbitrarily."""

        self.filtered_df = self.latest_day_df[
        (self.latest_day_df[f'pc_sma{self.fast_ma}_by_sma{self.slow_ma}']>5 
        & (self.latest_day_df[f'pc_sma{self.fast_ma}_by_close']<-0)
        & (self.latest_day_df[f'pc_sma{self.fast_ma}_by_close']>-5))
        ]

        self.filtered_df = self.filtered_df.sort_values(f'pc_sma{self.fast_ma}_by_close')
        
        return self.filtered_df
        
    def new_stocks_to_buy(self): 
        """Identifies the stocks to buy if the currently owned stocks is less than 5. 
        If current stocks owned is less than 5: then add stocks to stocks_owned csv"""

        self.row_count = self._count_stocks_owned()
        if self.row_count >=5:
            print("Number of currently owned stocks is 5. No need to add new stocks")
            pass

        else:
            number_of_new_stocks_needed = 5 - self.row_count

            # _filter_for_unique_stocks()
            new_filtered_stocks_df = self._only_take_unique_stocks(self.filtered_df, self.existing_portfolio_df)

            # we only want to hold 5 stocks so take the best stocks from filtered_df and only take symbol column
            self.top_filtered_stocks_df = new_filtered_stocks_df.head(number_of_new_stocks_needed).copy()
        
            self.top_filtered_stocks_df['date_bought'] = pd.Timestamp.today().strftime('%Y-%m-%d')
            self.top_filtered_stocks_df['date_sold'] = ''
            self.top_filtered_stocks_df['currently_owned'] = True

            # append top stocks to dataframe, append if csv already exists
            self.stocks_to_buy_df = self.top_filtered_stocks_df[['symbol', 'date_bought', 'date_sold','currently_owned']].copy()

            self._reupload_current_stocks_df()

    def _reupload_current_stocks_df(self):
        """Take current stocks owned csv and concat to new stocks to buy. Then overwrite to 'porfolio.csv'"""

        unioned_df = pd.concat([self.existing_portfolio_df, self.stocks_to_buy_df], ignore_index=True)
        unioned_df = unioned_df[['symbol', 'date_bought', 'date_sold','currently_owned']]
        
        return unioned_df.to_csv('porfolio.csv', index=False)

    def _only_take_unique_stocks(self, identified_stocks_df, porfolio_df):
        """Take two dataframes and filter identified_stocks_df to only include stocks that aren't in portfolio_df
        Filter portfolio_df to only include currently owned stocks (currently_owned == True).
        Left outer join on 'symbol'.
        Do this to prevent us adding a stock we already own.
        Todo: maybe remove this unique filter and replace by doubling down on a position if indicators point to it being a better bet. 
        """

        currently_owned_portfolio_df = porfolio_df[porfolio_df["currently_owned"]==True].copy()

        new_identified_stocks_df = pd.merge(identified_stocks_df, currently_owned_portfolio_df, on=['symbol'], how='outer', indicator=True).query('_merge=="left_only"')
        print(new_identified_stocks_df)
        
        return new_identified_stocks_df

    def _count_stocks_owned(self):
        """Count how many stocks in the portfolio csv are currently owned.
        Function used to early screener early if our portfolio is full and to identify how many new stocks to buy if portfolio isn't full"""

        self.row_count = len(self.existing_portfolio_df[self.existing_portfolio_df["currently_owned"]==True])
        print(f'row count of portfolio is {self.row_count}')
        return self.row_count

def _calculate_pc_ma(name_of_ma_row, name_of_adj_close_row):
    """Calculate the percentage difference between rows"""
    return ((name_of_adj_close_row - name_of_ma_row)/name_of_ma_row) * 100

def _calculate_pc_ma_difference(fast_moving_average, slow_moving_average):
    """Calculate the percentage difference between the fast and slow moving average"""
    return ((fast_moving_average - slow_moving_average)/slow_moving_average) * 100



if __name__ == "__main__":
    
    screener = Screener(25,50,100,'1y', 'https://en.wikipedia.org/wiki/List_of_S%26P_500_companies')

    # tickers_string = Screener.SP500_stocks_string(screener)
    # df = Screener.get_price_history(screener)
    # df.to_csv('price_history_download.csv')
    
    Screener.df = pd.read_csv('price_history_download.csv')
    ma_df = Screener.calculate_MAs(screener, Screener.df)
    indicators_df = Screener.calculate_ma_indicators(screener, ma_df)
    latest_date_df = Screener.latest_date_df(screener, indicators_df)
    filtered_df = Screener.screener_filter_on_criteria(screener)
    new_stocks = Screener.new_stocks_to_buy(screener)

    print('done')