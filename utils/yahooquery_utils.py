import pandas as pd
import yahooquery as yq

class Screener:
    def __init__(self, fast_ma: int, mid_ma: int, slow_ma: int, stock_period: str, stock_url:str):
        self.fast_ma = fast_ma
        self.mid_ma = mid_ma
        self.slow_ma = slow_ma
        self.stock_period = stock_period
        self.stock_url = stock_url

    def SP500_stocks_string(self) -> str:

        tickers = pd.read_html(self.stock_url)[0]['Symbol'].tolist()
        self.tickers_string = ' '.join(tickers).lower()

        return self.tickers_string

    def get_price_history(self) -> pd.DataFrame:
        
        data = yq.Ticker(self.tickers_string)
        self.df = data.history(period = self.stock_period) 

        return self.df 

    def calculate_MAs(self) -> pd.DataFrame:

        self.ma_df = self.df 

        self.ma_df['rolling_30day_high'] = self.ma_df['high'].rolling(30).max()
        self.ma_df[f'sma{self.fast_ma}'] = self.ma_df['adjclose'].rolling(self.fast_ma).mean()
        self.ma_df[f'sma{self.mid_ma}'] = self.ma_df['adjclose'].rolling(self.mid_ma).mean()
        self.ma_df[f'sma{self.slow_ma}'] = self.ma_df['adjclose'].rolling(self.slow_ma).mean()

        return self.ma_df

    def calculate_ma_indicators(self) -> pd.DataFrame:
        
        individual_df_list = []

        for symbol, new_df in self.ma_df.groupby(level=0):
            new_df = new_df.sort_index().tail(5)

            new_df[f'pc_sma{self.fast_ma}_by_close'] = new_df.apply(lambda x: _calculate_pc_ma(x[f'sma{self.fast_ma}'], x['adjclose']), axis=1)
            new_df[f'pc_sma{self.mid_ma}_by_close'] = new_df.apply(lambda x: _calculate_pc_ma(x[f'sma{self.mid_ma}'], x['adjclose']), axis=1)
            new_df[f'pc_sma{self.fast_ma}_by_sma{self.slow_ma}'] = new_df.apply(lambda x: _calculate_pc_ma_difference(x[f'sma{self.mid_ma}'], x[f'sma{self.slow_ma}']), axis=1)
            new_df[f'mean_of_sma{self.mid_ma}_by_close_last_5_days'] = new_df[f'pc_sma{self.mid_ma}_by_close'].mean()

            individual_df_list.append(new_df)

        self.joined_df = pd.concat(individual_df_list)

        return self.joined_df

    def latest_date_df(self) -> pd.DataFrame: 

        df_list = []

        for symbol, new_df in self.joined_df.groupby(level=0):
            new_df = new_df.sort_index().tail(1)
            df_list.append(new_df)

        self.latest_day_df = pd.concat(df_list)

        return self.latest_day_df

    def filter_on_criteria(self) -> pd.DataFrame:

        # filter where fast_ma is greater than close price and fast_ma is greater than slow_ma. both indicate a stock is trending. 
        self.filtered_df = self.latest_day_df[
        (self.latest_day_df[f'pc_sma{self.fast_ma}_by_sma{self.slow_ma}']>5) 
        & (self.latest_day_df[f'pc_sma{self.fast_ma}_by_close']<-8)
        & (self.latest_day_df[f'pc_sma{self.fast_ma}_by_close']>-13)
        ]

        self.filtered_df = self.filtered_df.sort_values(f'pc_sma{self.fast_ma}_by_close')
        
        return self.filtered_df

def _calculate_pc_ma(name_of_ma_row, name_of_adj_close_row):
    return ((name_of_adj_close_row - name_of_ma_row)/name_of_ma_row) * 100

def _calculate_pc_ma_difference(fast_moving_average, slow_moving_average):
    return ((fast_moving_average - slow_moving_average)/slow_moving_average) * 100

if __name__ == "__main__":
    
    screener = Screener(25,50,100,'1y', 'https://en.wikipedia.org/wiki/List_of_S%26P_500_companies')

    tickers_string = Screener.SP500_stocks_string(screener)
    df = Screener.get_price_history(screener)
    ma_df = Screener.calculate_MAs(screener)
    indicators_df = Screener.calculate_ma_indicators(screener)
    latest_date_df = Screener.latest_date_df(screener)
    filtered_df = Screener.filter_on_criteria(screener)
    print(filtered_df)
    filtered_df.to_csv('filtered_stock_df.csv')
