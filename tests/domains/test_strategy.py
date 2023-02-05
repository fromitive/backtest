from backtest.domains.strategy import Strategy
from backtest.domains.strategy import StrategyType
from backtest.domains.stockdata import StockData
import pandas as pd
import pytest


@pytest.fixture(scope='function')
def sample_df():
    return pd.DataFrame(index=pd.DatetimeIndex([]))


@pytest.fixture(scope='function')
def sample_stock_data(dict_stock_data):
    stockdata = StockData.from_dict(dict_stock_data, symbol='TEST')
    return stockdata


def test_init_strategy_with_empty_parameters():
    strategy = Strategy()
    assert strategy.name == ''
    assert strategy.type == StrategyType.no_type
    assert isinstance(strategy.data, pd.DataFrame)
    assert isinstance(strategy.data.index, pd.DatetimeIndex)
    assert strategy.weight == 0


@pytest.mark.parametrize("type", [StrategyType.no_type,
                                  StrategyType.with_onchain,
                                  StrategyType.with_stockdata,
                                  StrategyType.with_userdata,
                                  StrategyType.with_stockdata_and_subdata])
def test_init_strategy_with_parameters(sample_df, type):
    strategy = Strategy(name='test strategy',
                        type=type,
                        data=sample_df, weight=1, subdata=sample_df)
    assert strategy.name == 'test strategy'
    assert strategy.type == type
    assert isinstance(strategy.data.index, pd.DatetimeIndex)
    assert strategy.weight == 1
    assert isinstance(strategy.subdata.index, pd.DatetimeIndex)
    assert strategy.target == 'ALL'


@pytest.mark.parametrize("type", [StrategyType.no_type,
                                  StrategyType.with_onchain,
                                  StrategyType.with_stockdata,
                                  StrategyType.with_userdata,
                                  StrategyType.with_stockdata_and_subdata])
def test_strategy_len_return_data_len(sample_df, type):
    strategy = Strategy(name='test strategy',
                        type=type,
                        data=sample_df, weight=1, subdata=sample_df, target='SYMBOL')
    assert len(strategy.data) == 0


@pytest.mark.parametrize("type", [StrategyType.no_type,
                                  StrategyType.with_onchain,
                                  StrategyType.with_stockdata,
                                  StrategyType.with_userdata,
                                  StrategyType.with_stockdata_and_subdata])
def test_init_strategy_with_stockdata_return_target_symbol_equal_stockdata_symbol(sample_stock_data, type):
    strategy = Strategy(name='test strategy',
                        type=type,
                        data=sample_stock_data, weight=1)
    assert isinstance(strategy.data, pd.DataFrame)
    assert isinstance(strategy.data.index, pd.DatetimeIndex)
    assert sample_stock_data.symbol == strategy.target
