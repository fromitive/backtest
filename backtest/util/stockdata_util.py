from backtest.domains.stockdata import StockData
from datetime import datetime
import pandas as pd 


def genrate_empty_stockData(from_date,to_date=datetime.now().strftime('%Y-%m-%d'),symbol=''):
    date_series=pd.date_range(from_date,to_date)
    df=pd.DataFrame(columns=['open', 'high', 'low', 'close',
                                               'volume'],index=date_series).fillna(0)
    return StockData(symbol=symbol,data=df)