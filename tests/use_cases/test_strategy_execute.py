from unittest import mock

import pytest

from backtest.domains.stockdata import StockData
from backtest.domains.strategy import Strategy
from backtest.domains.strategy_result import (StrategyResult,
                                              StrategyResultColumnType)
from backtest.module_compet.pandas import pd
from backtest.response import ResponseSuccess
from backtest.use_cases.strategy_execute import (basic_function,
                                                 strategy_execute)


@pytest.fixture(scope='function')
def sample_stock_data(dict_stock_data):
    return StockData.from_dict(dict_stock_data)


@pytest.fixture(scope='function')
def dict_stock_data_list():
    return [{'open': [0.00000, '1.11111', '3.3333', '0.0000', '1.0000'],
            'high': [0.00000, '1.11111', '3.3333', '0.0000', '1.0000'],
             'low': [0.00000, '1.11111', '3.3333', '0.0000', '1.0000'],
             'close': [0.00000, '1.11111', '3.3333', '0.0000', '1.0000'],
             'volume': [0.00000, '1.11111', '3.3333', '0.0000', '1.0000'],
             'date': ['2022-01-01', '2022-01-02', '2022-01-03', '2022-01-04', '2022-01-05']},
            {'open': [0.00000, '1.11111', '2.2222', '1.0000'],
            'high': [0.00000, '1.11111', '2.2222', '1.0000'],
             'low': [0.00000, '1.11111', '2.2222', '1.0000'],
             'close': [0.00000, '1.11111', '2.2222', '1.0000'],
             'volume': [0.00000, '1.11111', '2.2222', '1.0000'],
             'date': ['2022-01-01', '2022-01-02', '2022-01-03', '2022-01-05']},
            {'open': [0.00000, '2.2222', '1.0000'],
            'high': [0.00000, '2.2222', '1.0000'],
             'low': [0.00000, '2.2222', '1.0000'],
             'close': [0.00000, '2.2222', '1.0000'],
             'volume': [0.00000, '2.2222', '1.0000'],
             'date': ['2022-01-01', '2022-01-02', '2022-01-03']}]


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
def strategy_list():
    strategy1 = Strategy(name='test1')
    strategy2 = Strategy(name='test2')
    strategy3 = Strategy(name='test3')
    return [
        strategy1, strategy2, strategy3
    ]


@pytest.fixture(scope='function')
def dict_strategy_result():
    return {'test strategy': [(StrategyResultColumnType.BUY, 1),  # type and strategy weight
                              (StrategyResultColumnType.BUY, 1),
                              (StrategyResultColumnType.SELL, 1),
                              (StrategyResultColumnType.KEEP, 1)],
            'date': ['2022-10-30',
                     1388070000000,
                     '2022-02-09',
                     '2022-04-07']}


@pytest.fixture(scope='function')
def strategy_result_data_frame(dict_strategy_result):
    df = pd.DataFrame(dict_strategy_result,
                      columns=['test strategy', 'date'])
    df.set_index('date', inplace=True)
    df.index = pd.to_datetime(df.index)
    df.index = df.index.strftime('%Y-%m-%d %H:%M:%S')
    return df


@pytest.fixture(scope='function')
def sample_strategy_result(dict_strategy_result):
    strategy_result = StrategyResult.from_dict(
        dict_strategy_result, target='TEST')
    return strategy_result


def test_strategy_execute(sample_stock_data, strategy_result_data_frame):
    test_function = mock.Mock()
    test_function.return_value = strategy_result_data_frame
    strategy = Strategy(name='test strategy', weight=1,
                        target='TARGET', function=test_function, options={'param1': 'value1', 'param2': 'value2'})
    response = strategy_execute(
        strategy_list=[strategy], stockdata=sample_stock_data)
    # test_function.assert_called_once_with(
    #    data=sample_stock_data, weight=strategy.weight, name=strategy.name, param1='value1', param2='value2')
    strategy_result = response.value
    assert isinstance(response, ResponseSuccess)
    assert isinstance(strategy_result, StrategyResult)


def test_strategy_basic_function(stockdata_list):
    strategy = Strategy(name='test strategy', weight=1999,
                        target='TARGET', function=basic_function)
    for stockdata in stockdata_list:
        response = strategy_execute(
            strategy_list=[strategy], stockdata=stockdata)
        strategy_result = response.value
        assert isinstance(response, ResponseSuccess)
        assert isinstance(strategy_result, StrategyResult)
        assert len(strategy_result) == len(stockdata)
