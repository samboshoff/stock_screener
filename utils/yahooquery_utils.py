import pandas as pd
import yahooquery as yq

def SP500_stocks_string(URL: str) -> str:

    tickers = pd.read_html(URL)[0]['Symbol'].tolist()

    tickers_string = ' '.join(tickers).lower()

    return tickers_string

def get_price_history(ticker_string: str, period: str, ) -> pd.DataFrame:
    
    data = yq.Ticker(ticker_string)

    df = data.history(period = period) 

    return df 

def calculate_MAs(df: pd.DataFrame) -> pd.DataFrame:

    df['sma10'] = df['adjclose'].rolling(10).mean()
    df['sma30'] = df['adjclose'].rolling(30).mean()
    df['sma100'] = df['adjclose'].rolling(100).mean()

    return df

def _calculate_pc_ma(name_of_ma_row, name_of_adj_close_row):
    return ((name_of_adj_close_row - name_of_ma_row)/name_of_ma_row) * 100

def _calculate_pc_ma_difference(fast_moving_average, slow_moving_average):
    return ((fast_moving_average - slow_moving_average)/slow_moving_average) * 100


def calculate_ma_indicators(df: pd.DataFrame) -> pd.DataFrame:
    
    individual_df_list = []

    for symbol, new_df in df.groupby(level=0):

        new_df = new_df.sort_index().tail(5)

        new_df['pc_sma10_by_close'] = new_df.apply(lambda x: _calculate_pc_ma(x['sma10'], x['adjclose']), axis=1)
        new_df['pc_sma30_by_close'] = new_df.apply(lambda x: _calculate_pc_ma(x['sma30'], x['adjclose']), axis=1)
        new_df['pc_sma30_by_sma100'] = new_df.apply(lambda x: _calculate_pc_ma_difference(x['sma30'], x['sma100']), axis=1)
        new_df['mean_of_sma30_by_close_last_5_days'] = new_df['pc_sma30_by_close'].mean()

        individual_df_list.append(new_df)

    joined_df = pd.concat(individual_df_list)

    return joined_df


def latest_date_df(df: pd.DataFrame) -> pd.DataFrame: 

    df_list = []

    for symbol, new_df in df.groupby(level=0):

        new_df = new_df.sort_index().tail(1)

        df_list.append(new_df)

    latest_date_df = pd.concat(df_list)

    return latest_date_df


def filter_on_criteria(df: pd.DataFrame) -> pd.DataFrame:

    filtered_df = df[(df['pc_sma10_by_close']<0) & df['pc_sma30_by_sma100']>0]
    filtered_df = filtered_df.sort_values('pc_sma10_by_close')
    
    return filtered_df


if __name__ == "__main__":
    
    URL = 'https://en.wikipedia.org/wiki/List_of_S%26P_500_companies'

    tickers_string = SP500_stocks_string(URL)
    df = get_price_history(tickers_string, '1y')
    ma_df = calculate_MAs(df)
    indicators_df = calculate_ma_indicators(ma_df)
    latest_date_df = latest_date_df(indicators_df)

    latest_date_df = latest_date_df.sort_values('mean_of_sma30_by_close_last_5_days', ascending=False).head(30)
    print(latest_date_df)