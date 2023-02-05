from backtest.use_cases.strategy_execute import strategy_execute
from backtest.domains.strategy import Strategy, StrategyType
from backtest.domains.strategy_result import StrategyResult, StrategyResultColumnType
from backtest.domains.stockdata import StockData
import pytest
import pandas as pd
from unittest import mock


@pytest.fixture(scope='function')
def sample_stock_data(dict_stock_data):
    return StockData.from_dict(dict_stock_data)


@pytest.fixture(scope='function')
def sample_strategy(sample_stock_data):
    return Strategy(name='test', data=sample_stock_data)


@pytest.fixture(scope='function')
def sample_strategy_with_type(sample_stock_data):
    return Strategy(name='test', type=StrategyType.with_stockdata, data=sample_stock_data)


@pytest.fixture(scope='function')
def sample_strategy_result(sample_strategy):
    strategy_result = StrategyResult(value=pd.DataFrame(
        index=sample_strategy.data.index, columns=[sample_strategy.name]))
    strategy_result.value[sample_strategy.name] = StrategyResultColumnType.KEEP
    return strategy_result


@pytest.fixture(scope='function')
def sample_strategy_result_with_type(sample_strategy_with_type):
    strategy_result = StrategyResult(value=pd.DataFrame(
        index=sample_strategy_with_type.data.index, columns=[sample_strategy_with_type.name]))
    strategy_result.value[sample_strategy_with_type.name] = [(
        StrategyResultColumnType.KEEP, sample_strategy_with_type.weight)] * len(sample_strategy_with_type)
    return strategy_result


def test_strategy_execute_without_parameters(sample_strategy, sample_strategy_result):
    strategy_function = mock.Mock()
    strategy_function.return_value = sample_strategy_result
    response = strategy_execute(
        sample_strategy, strategy_fucntion=strategy_function)
    assert bool(response) == False
    assert response.message == 'Exception: type not defined..'


def test_strategy_execute_with_parameters(sample_strategy_with_type, sample_strategy_result_with_type):
    strategy_function = mock.Mock()
    strategy_function.return_value = sample_strategy_result_with_type
    response = strategy_execute(
        sample_strategy_with_type, strategy_fucntion=strategy_function)
    strategy_function.assert_called_with(strategy=sample_strategy_with_type)
    assert bool(response) == True
    assert sample_strategy_with_type.name in response.value.value.columns
    assert len(response.value.value.columns) == 1
    assert len(
        response.value.value[sample_strategy_with_type.name].iloc[0]) == 2
    assert response.value.value[sample_strategy_with_type.name].iloc[0][0] in StrategyResultColumnType
    assert response.value.value[sample_strategy_with_type.name].iloc[0][1] == 0
