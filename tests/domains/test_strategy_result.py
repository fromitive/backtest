import pytest
import pandas as pd
from backtest.domains.strategy_result import StrategyResult
from backtest.domains.strategy_result import StrategyResultColumnType


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
def dict_strategy_result2():
    return {'date': ['2022-01-01', '2022-01-02', '2022-01-03', '2022-01-04', '2022-01-05'],
            'strategy1': [(StrategyResultColumnType.KEEP, 100),
                          (StrategyResultColumnType.BUY, 100),
                          (StrategyResultColumnType.SELL, 100),
                          (StrategyResultColumnType.SELL, 100),
                          (StrategyResultColumnType.BUY, 100),]}


@pytest.fixture(scope='function')
def strategy_result_data_frame(dict_strategy_result):
    df = pd.DataFrame(dict_strategy_result,
                      columns=['name', 'date'])
    df.set_index('date', inplace=True)
    df.index = pd.to_datetime(df.index)
    return df


def test_init_strategy_result_without_parameter():
    strategy_result = StrategyResult()
    assert isinstance(strategy_result.value, pd.DataFrame)
    # assert isinstance(strategy_result.value.index, pd.DatetimeIndex)
    # assert '' in strategy_result.value.columns


def test_init_strategy_result_with_parameter(strategy_result_data_frame):
    strategy_result = StrategyResult(value=strategy_result_data_frame)
    assert isinstance(strategy_result.value, pd.DataFrame)
    assert isinstance(strategy_result.value.index, pd.DatetimeIndex)
    assert strategy_result.value.columns == ['name']
    assert strategy_result.target == 'ALL'
    assert len(strategy_result) == 4
    assert len(strategy_result.value['name'].iloc[0]) == 2


def test_init_strategy_result_with_parameter(strategy_result_data_frame):
    strategy_result = StrategyResult(
        value=strategy_result_data_frame, target='TEST')
    assert isinstance(strategy_result.value, pd.DataFrame)
    assert isinstance(strategy_result.value.index, pd.DatetimeIndex)
    assert strategy_result.value.columns == ['name']
    assert strategy_result.target == 'TEST'
    assert len(strategy_result) == 4
    assert len(strategy_result.value['name'].iloc[0]) == 2


def test_init_strategy_result_from_dict(dict_strategy_result):
    strategy_result = StrategyResult.from_dict(
        dict_strategy_result, target='TEST')
    assert len(strategy_result) == 4
    assert isinstance(strategy_result.value, pd.DataFrame)
    assert isinstance(strategy_result.value.index, pd.DatetimeIndex)
    assert strategy_result.value.columns == ['name']
    assert strategy_result.target == 'TEST'
    assert len(strategy_result.value['name'].iloc[0]) == 2


def test_init_strategy_result_from_dict(dict_strategy_result2):
    strategy_result = StrategyResult.from_dict(
        dict_strategy_result2, target='TEST')
    assert len(strategy_result) == 5
    assert isinstance(strategy_result.value, pd.DataFrame)
    assert isinstance(strategy_result.value.index, pd.DatetimeIndex)
    assert strategy_result.value.columns == ['strategy1']
    assert strategy_result.target == 'TEST'
    assert len(strategy_result.value['strategy1'].iloc[0]) == 2
