from backtest.use_cases.backtest_execute import backtest_execute
from backtest.domains.backtest import Backtest
from backtest.domains.strategy import Strategy
from backtest.domains.stockdata import StockData
from backtest.domains.backtest_result import BacktestResult
from backtest.domains.strategy_result import StrategyResult, StrategyResultColumnType
from backtest.response import ResponseSuccess
import pytest
import pandas as pd
from unittest import mock


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
def strategy_result_data():
    sample_dict = {'date': ['2022-01-01', '2022-01-02', '2022-01-03', '2022-01-04', '2022-01-05'],
                   'name': [(StrategyResultColumnType.KEEP, 100),
                            (StrategyResultColumnType.BUY, 100),
                            (StrategyResultColumnType.SELL, 100),
                            (StrategyResultColumnType.SELL, 100),
                            (StrategyResultColumnType.BUY, 100),]}
    return StrategyResult.from_dict(sample_dict, target='ALL')


@mock.patch('backtest.use_cases.backtest_execute.strategy_execute')
def test_backtest_execute_without_options(strategy_execute, strategy_list, stockdata_list, strategy_result_data):
    strategy_execute.return_value = ResponseSuccess(strategy_result_data)
    strategies = strategy_list
    stockdata = stockdata_list
    backtest = Backtest(strategy_list=strategies, stockdata_list=stockdata)
    response = backtest_execute(backtest)
    strategy_execute.assert_called()
    assert isinstance(response, ResponseSuccess)
    assert isinstance(response.value, BacktestResult)
    assert bool(response) == True
    backtest_result = response.value
    assert isinstance(backtest_result, BacktestResult)
    assert isinstance(backtest_result.value, pd.DataFrame)
    assert backtest_result.value.index[0].strftime("%Y-%m-%d") == '2022-01-01'
    assert isinstance(backtest_result.value.index, pd.DatetimeIndex)
    assert list(backtest_result.value.columns) == ['total_profit',
                                                   'stock_bucket']
