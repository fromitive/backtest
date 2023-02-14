from backtest.use_cases.strategy_execute import strategy_execute
from backtest.domains.strategy import Strategy, StrategyType
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
    basic_function = mock.Mock()
    basic_function.return_value = sample_strategy_result
    strategy = Strategy(name='test strategy',weight=1,
                        target='TARGET', function=basic_function,options={'param1':'value1','param2':'value2'})
    response = strategy_execute(strategy=strategy,data=sample_stock_data)
    basic_function.assert_called_once_with(data=sample_stock_data,param1='value1',param2='value2')
    strategy_result = response.value
    assert isinstance(response, ResponseSuccess)
    assert isinstance(strategy_result, StrategyResult)
    assert strategy_result.target == 'TEST'
