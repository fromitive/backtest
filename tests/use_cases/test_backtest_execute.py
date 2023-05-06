from unittest import mock

import pytest

from backtest.domains.backtest import Backtest
from backtest.domains.backtest_result import BacktestResult
from backtest.domains.stockdata import StockData
from backtest.domains.strategy import Strategy
from backtest.domains.strategy_result import (StrategyResult,
                                              StrategyResultColumnType)
from backtest.module_compet.pandas import pd
from backtest.response import ResponseSuccess
from backtest.use_cases.backtest_execute import backtest_execute


@pytest.fixture(scope='function')
def dict_stock_data_list():
    return [{'open': [0.00000, '1.11111', '3.3333', '0.0000', '1.0000'],
            'high': [0.00000, '1.11111', '3.3333', '0.0000', '1.0000'],
             'low': [0.00000, '1.11111', '3.3333', '0.0000', '1.0000'],
             'close': [0.00000, '1.11111', '3.3333', '0.0000', '1.0000'],
             'volume': [0.00000, '1.11111', '3.3333', '0.0000', '1.0000'],
             'date': ['2022-01-01', '2022-01-02', '2022-01-03', '2022-01-04', '2022-01-05']},
            {'open': [0.00000, '1.11111', '2.2222', '0.0000', '1.0000'],
            'high': [0.00000, '1.11111', '2.2222', '0.0000', '1.0000'],
             'low': [0.00000, '1.11111', '2.2222', '0.0000', '1.0000'],
             'close': [0.00000, '1.11111', '2.2222', '0.0000', '1.0000'],
             'volume': [0.00000, '1.11111', '2.2222', '0.0000', '1.0000'],
             'date': ['2022-01-01', '2022-01-02', '2022-01-03', '2022-01-04', '2022-01-05']},
            {'open': [0.00000, '1.11111', '3.3333', '0.0000', '1.0000'],
            'high': [0.00000, '1.11111', '4.4444', '0.0000', '1.0000'],
             'low': [0.00000, '1.11111', '4.4444', '0.0000', '1.0000'],
             'close': [0.00000, '1.11111', '4.4444', '0.0000', '1.0000'],
             'volume': [0.00000, '1.11111', '5.5555', '0.0000', '1.0000'],
             'date': ['2022-01-01', '2022-01-02', '2022-01-03', '2022-01-04', '2022-01-05']}]


@pytest.fixture(scope='function')
def strategy_list():
    strategy1 = Strategy(name='test1')
    strategy2 = Strategy(name='test2')
    strategy3 = Strategy(name='test3')
    return [
        strategy1, strategy2, strategy3
    ]


@pytest.fixture(scope='function')
def stockdata_list(dict_stock_data_list):
    stockdata1 = StockData.from_dict(
        dict_data=dict_stock_data_list[0], symbol='teststock1')
    stockdata2 = StockData.from_dict(
        dict_data=dict_stock_data_list[1], symbol='teststock2')
    stockdata3 = StockData.from_dict(
        dict_data=dict_stock_data_list[2], symbol='teststock3')
    return [
        stockdata1, stockdata2, stockdata3
    ]


@pytest.fixture(scope='function')
def dict_strategy_result():
    return {'name': [(StrategyResultColumnType.BUY, 1),  # type and strategy weight
                     (StrategyResultColumnType.BUY, 1),
                     (StrategyResultColumnType.SELL, 1),
                     (StrategyResultColumnType.KEEP, 1),
                     (StrategyResultColumnType.KEEP, 1)],
            'date': ['2022-01-01',
                     '2022-01-02',
                     '2022-01-03',
                     '2022-01-04',
                     '2022-01-05']}


@pytest.fixture(scope='function')
def _inner_strategy_execute_result(dict_strategy_result):
    df = pd.DataFrame(dict_strategy_result,
                      columns=['name', 'date'])
    df.set_index('date', inplace=True)
    df.index = pd.to_datetime(df.index)
    return ResponseSuccess(df)


@mock.patch('backtest.use_cases.strategy_execute._inner_strategy_execute')
def test_backtest_execute_without_options(strategy_execute_result, strategy_list, stockdata_list, _inner_strategy_execute_result):
    strategy_execute_result.return_value = _inner_strategy_execute_result
    strategies = strategy_list
    stockdata = stockdata_list
    backtest = Backtest(strategy_list=strategies,
                        stockdata_list=stockdata, buy_price=5000.0)
    response = backtest_execute(backtest)
    assert isinstance(response, ResponseSuccess)
    assert isinstance(response.value, BacktestResult)
    assert bool(response) is True
    backtest_result = response.value
    assert isinstance(backtest_result, BacktestResult)
    assert isinstance(backtest_result.value, pd.DataFrame)
    assert backtest_result.value.index[0].strftime("%Y-%m-%d") == '2022-01-01'
    assert isinstance(backtest_result.value.index, pd.DatetimeIndex)
    assert list(backtest_result.value.columns) == [
        'current_money', 'stock_bucket', 'total_potential_earn', 'total_potential_profit']
