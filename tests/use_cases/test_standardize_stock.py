from datetime import datetime

from backtest.use_cases.standardize_stock import standardize_stock
from backtest.util.stockdata_util import generate_empty_stockData


def test_standardize_stock():
    stockdata1 = generate_empty_stockData(['2019-01-01 00:00:00'])
    stockdata2 = generate_empty_stockData(['2018-11-01 00:00:00'])
    stockdata1.data['close'] = 1.1
    standardize_stock(stockdata_list=[stockdata1, stockdata2])

    assert stockdata1.data.index[0] == '2018-11-01 00:00:00'
    assert len(stockdata2.data) == len(stockdata1.data)
    assert stockdata1.data['open'].iloc[0] == 0.0
    assert stockdata1.data['close'].iloc[0] == 0.0
    assert stockdata1.data['close'].iloc[-1] == 1.1
    assert stockdata1.data['high'].iloc[0] == 0.0
    assert stockdata1.data['low'].iloc[0] == 0.0
    assert stockdata1.data['volume'].iloc[0] == 0.0
