import json
from datetime import datetime

import requests

from backtest.domains.stockdata import StockData
from backtest.module_compet.pandas import pd


def generate_empty_stockData(from_date='1999-01-01', to_date=datetime.now().strftime('%Y-%m-%d'), symbol=''):
    if to_date == '':
        to_date = datetime.now().strftime('%Y-%m-%d')
    date_series = pd.date_range(start=from_date, end=to_date)
    df = pd.DataFrame(columns=['open', 'high', 'low', 'close',
                               'volume'], index=date_series).fillna(0)
    df = df.rename_axis('date')
    return StockData(symbol=symbol, data=df)


def get_greed_fear_index():
    rsp = requests.get('https://api.alternative.me/fng/?limit=10000000')
    result = json.loads(rsp.text)
    df = pd.DataFrame(result['data'])
    df['value'] = df['value'].astype('int')
    df['timestamp'] = df['timestamp'].astype('int')
    df['date'] = pd.to_datetime(df['timestamp'], unit='s')
    df.set_index('date', inplace=True)
    df = df[['value', 'value_classification']]
    df.astype({'value': 'int'})
    df.sort_index()
    return df
