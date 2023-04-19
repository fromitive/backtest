import pandas as pd
from pandas import DatetimeIndex

from backtest.domains.stockdata import StockData


def test_init_stockdata_without_parameters():
    sto = StockData()
    assert sto.symbol == ''
    # assert list(sto.data.columns) == ['open',
    #                                  'high',
    #                                  'low',
    #                                  'close',
    #                                  'volume']
    # assert isinstance(sto.data.index, DatetimeIndex)


def test_init_stockdate_with_parameters():
    sto = StockData(symbol="test",
                    data=pd.DataFrame(columns=['open', 'high', 'low', 'close',
                                               'volume'],
                                      index=DatetimeIndex([])))
    assert sto.symbol == 'test'
    assert list(sto.data.columns) == ['open',
                                      'high',
                                      'low',
                                      'close',
                                      'volume']
    assert isinstance(sto.data.index, DatetimeIndex)


def test_init_stockdata_from_dict(dict_stock_data):
    sto = StockData.from_dict(dict_stock_data)
    assert sto.symbol == ''
    assert list(sto.data.columns) == ['open',
                                      'high',
                                      'low',
                                      'close',
                                      'volume']
    assert isinstance(sto.data.index, DatetimeIndex)


def test_stockdata_len_return_data_len(dict_stock_data):
    sto = StockData.from_dict(dict_stock_data)
    assert sto.symbol == ''
    assert list(sto.data.columns) == ['open',
                                      'high',
                                      'low',
                                      'close',
                                      'volume']
    assert len(sto) == 2
