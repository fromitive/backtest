import json
import requests

from backtest.domains.stockdata import StockData
from backtest.module_compet.pandas import pd


def generate_empty_stockData(indexes, symbol=''):

    df = pd.DataFrame(columns=['open', 'high', 'low', 'close',
                               'volume'], index=indexes).fillna(0)
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
    df.index = df.index.strftime('%Y-%m-%d %H:%M:%S')
    df = df[['value', 'value_classification']]
    df.astype({'value': 'int'})
    df.sort_index()
    return df
