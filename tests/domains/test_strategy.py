from backtest.domains.strategy import Strategy
from backtest.domains.stockdata import StockData
from backtest.domains.strategy_function import StrategyFunction
import typing
import pandas as pd
import pytest


@pytest.fixture(scope='function')
def sample_stock_data(dict_stock_data):
    return StockData.from_dict(dict_stock_data)


@pytest.fixture(scope='function')
def sample_stock_data(dict_stock_data):
    stockdata = StockData.from_dict(dict_stock_data, symbol='TEST')
    return stockdata


def test_init_strategy_with_empty_parameters():
    strategy = Strategy()
    assert strategy.name == ''
    assert strategy.data == []
    assert strategy.function == None
    assert strategy.weight == 0
    assert strategy.target == 'ALL'


def test_init_strategy_with_parameters(sample_stock_data):
    strategy = Strategy(name='test strategy',
                        data=sample_stock_data, weight=1,target='TARGET')
    assert strategy.name == 'test strategy'
    assert isinstance(strategy.data, typing.List)
    assert strategy.weight == 1
    assert strategy.function == None
    assert strategy.target == 'TARGET'
    assert strategy.options == {}

def test_strategy_len_return_data_len(sample_stock_data):
    strategy = Strategy(name='test strategy',
                        data=sample_stock_data, target='SYMBOL')
    assert len(strategy.data) == 1
