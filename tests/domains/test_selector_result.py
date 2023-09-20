import pandas as pd
import pytest

from backtest.domains.selector_result import SelectorResult, SelectorResultColumnType


@pytest.fixture(scope="function")
def dict_selector_result1():
    return {
        "name": [
            (SelectorResultColumnType.SELECT, 1),  # type and strategy weight
            (SelectorResultColumnType.KEEP, 1),
            (SelectorResultColumnType.SELECT, 1),
            (SelectorResultColumnType.KEEP, 1),
        ],
        "date": ["2022-10-30", 1388070000000, "2022-02-09", "2022-04-07"],
    }


@pytest.fixture(scope="function")
def dict_selector_result2():
    return {
        "date": ["2022-01-01", "2022-01-02", "2022-01-03", "2022-01-04", "2022-01-05"],
        "strategy1": [
            (SelectorResultColumnType.SELECT, 100),
            (SelectorResultColumnType.KEEP, 100),
            (SelectorResultColumnType.SELECT, 100),
            (SelectorResultColumnType.KEEP, 100),
            (SelectorResultColumnType.SELECT, 100),
        ],
    }


@pytest.fixture(scope="function")
def selector_result_dataframe(dict_selector_result1):
    df = pd.DataFrame(dict_selector_result1, columns=["name", "date"])
    df.set_index("date", inplace=True)
    df.index = pd.to_datetime(df.index)
    df.index = df.index.strftime("%Y-%m-%d %H:%M:%S")
    return df


def test_init_selector_result_without_parameter():
    selector_result = SelectorResult()
    assert isinstance(selector_result.value, pd.DataFrame)
    # assert isinstance(strategy_result.value.index, pd.DatetimeIndex)
    # assert '' in strategy_result.value.columns


def test_init_strategy_result_with_parameter(selector_result_dataframe):
    selector_result = SelectorResult(value=selector_result_dataframe)
    assert isinstance(selector_result.value, pd.DataFrame)
    # assert isinstance(selector_result.value.index, pd.DatetimeIndex)
    assert selector_result.value.columns == ["name"]
    assert len(selector_result) == 4
    assert len(selector_result.value["name"].iloc[0]) == 2


def test_init_strategy_result_from_dict(dict_selector_result2):
    selector_result = SelectorResult.from_dict(dict_selector_result2)
    assert len(selector_result) == 5
    assert isinstance(selector_result.value, pd.DataFrame)
    # assert isinstance(selector_result.value.index, pd.DatetimeIndex)
    assert selector_result.value.columns == ["strategy1"]
    assert len(selector_result.value["strategy1"].iloc[0]) == 2
