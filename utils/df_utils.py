import pandas as pd

class Moving_Average:
    def __init__(self, fast_ma: int, mid_ma: int, slow_ma: int):
        self.fast_ma = fast_ma 
        self.mid_ma = mid_ma
        self.slow_ma = slow_ma

    def _calculate_MAs(self, df) -> pd.DataFrame:
        """Calculate the moving averages"""
        ma_df = df.copy()

        ma_df['rolling_30day_high'] = ma_df['high'].rolling(30).max()
        ma_df[f'sma{self.fast_ma}'] = ma_df['adjclose'].rolling(self.fast_ma).mean()
        ma_df[f'sma{self.mid_ma}'] = ma_df['adjclose'].rolling(self.mid_ma).mean()
        ma_df[f'sma{self.slow_ma}'] = ma_df['adjclose'].rolling(self.slow_ma).mean()

        return ma_df

    def _calculate_ma_indicators(self, ma_df) -> pd.DataFrame:
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

        indicators_df = pd.concat(individual_df_list)
        return indicators_df

def _latest_date_df(indicators_df) -> pd.DataFrame: 
    """"Filter the dataframe to only take the most recent date of data after having calculated the indicators"""

    df_list = []

    for symbol, new_df in indicators_df.groupby(by='symbol'):
        new_df = new_df.sort_index().tail(1)
        df_list.append(new_df)

    latest_day_df = pd.concat(df_list)
    return latest_day_df

def _calculate_pc_ma(name_of_ma_row, name_of_adj_close_row):
    """Calculate the percentage difference between rows"""
    return ((name_of_adj_close_row - name_of_ma_row)/name_of_ma_row) * 100

def _calculate_pc_ma_difference(fast_moving_average, slow_moving_average):
    """Calculate the percentage difference between the fast and slow moving average"""
    return ((fast_moving_average - slow_moving_average)/slow_moving_average) * 100