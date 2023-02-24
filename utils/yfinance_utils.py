import pandas as pd
import yfinance as yf
import numpy as np

#These functions are for dealing with multiple stocks. 

def list_of_SP500_stocks_info(URL: str) -> pd.DataFrame:

    tickers = pd.read_html(URL)[0]['Symbol'].tolist()
    industry = pd.read_html(URL)[0]['GICS Sector'].tolist()

    data = {
    'Ticker': tickers,
    'Industry': industry
    }

    df = pd.DataFrame(data)
    
    return df, tickers


def create_stock_df(tickers: list) -> pd.DataFrame:

    df = yf.download(tickers, start='2022-02-01', end='2023-02-01')

    df = df[['Open', 'Adj Close', 'High', 'Low']]

    df = _calculate_SMA(df)

    df = df.T

    df.index.set_names(['Metric','Ticker'], inplace=True)

    df = df.swaplevel(0, 1)
    
    df.sort_index(axis=1, level=0, inplace=True)

    return df


def _calculate_SMA(df: pd.DataFrame):

    # create 30 day SMA
    sma30_df = df['Adj Close'].rolling(30).mean()
    sma30_label = "SMA30"
    sma30_df.columns = pd.MultiIndex.from_product([[sma30_label],sma30_df.columns.unique()])

    # create 200 day SMA
    sma200_df = df['Adj Close'].rolling(200).mean()
    sma200_label = "SMA200"
    sma200_df.columns = pd.MultiIndex.from_product([[sma200_label],sma200_df.columns.unique()])

    # concatenate to back to main df
    final_df = pd.concat([df, sma30_df, sma200_df],axis=1)

    return final_df

def _join_industry_info_to_stock_data(all_stocks_df: pd.DataFrame, stock_info_df: pd.DataFrame) -> pd.DataFrame:
    
    joined_df = pd.merge(all_stocks_df.reset_index(), stock_info_df.reset_index(), on=['Ticker'], how='inner').set_index(['Industry','Ticker','Metric'])
    joined_df = joined_df.sort_index()
    joined_df = joined_df.drop('index', axis=1)

    return joined_df


URL = 'https://en.wikipedia.org/wiki/List_of_S%26P_500_companies'

stock_info_df, list_of_stocks = list_of_SP500_stocks_info(URL)

all_stocks_df = create_stock_df(list_of_stocks)

joined_df = _join_industry_info_to_stock_data(all_stocks_df, stock_info_df)

joined_df