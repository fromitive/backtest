from backtest.use_cases.strategy_execute import strategy_execute, basic_function
from backtest.domains.strategy import Strategy
from backtest.domains.strategy_result import StrategyResult, StrategyResultColumnType
from backtest.domains.stockdata import StockData
from backtest.response import ResponseSuccess, ResponseFailure
import pytest
import pandas as pd
from unittest import mock


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
    return {'name': [(StrategyResultColumnType.BUY, 1),  # type and strategy weight
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
                      columns=['name', 'date'])
    df.set_index('date', inplace=True)
    df.index = pd.to_datetime(df.index)
    return df


@pytest.fixture(scope='function')
def sample_strategy_result(dict_strategy_result):
    strategy_result = StrategyResult.from_dict(
        dict_strategy_result, target='TEST')
    return strategy_result


def test_strategy_execute(sample_stock_data, sample_strategy_result):
    test_function = mock.Mock()
    test_function.return_value = sample_strategy_result
    strategy = Strategy(name='test strategy', weight=1,
                        target='TARGET', function=test_function, options={'param1': 'value1', 'param2': 'value2'})
    response = strategy_execute(strategy=strategy, data=sample_stock_data)
    test_function.assert_called_once_with(
        data=sample_stock_data, weight=strategy.weight, name=strategy.name, param1='value1', param2='value2')
    strategy_result = response.value
    assert isinstance(response, ResponseSuccess)
    assert isinstance(strategy_result, StrategyResult)
    assert strategy_result.target == 'TEST'


def test_strategy_basic_function(stockdata_list):
    strategy = Strategy(name='test strategy', weight=1999,
                        target='TARGET', function=basic_function)
    for stockdata in stockdata_list:
        response = strategy_execute(strategy=strategy, data=stockdata)
        strategy_result = response.value
        assert isinstance(response, ResponseSuccess)
        assert isinstance(strategy_result, StrategyResult)
        assert len(strategy_result) == len(stockdata)
