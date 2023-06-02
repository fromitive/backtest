from unittest import mock

import pandas as pd
import pytest
from pandas import DatetimeIndex

from backtest.domains.selector_reference import SelectorReference


@pytest.fixture(scope='function')
def dict_selector_reference():
    return {'date': ['2022-10-11', '2022-11-11'],
            'dummy': [1.1, 2.2],
            'dummy2': ['11', '22']}


@pytest.fixture(scope='function')
def dataframe_reference(dict_selector_reference):
    return pd.DataFrame(dict_selector_reference)


def test_init_selector_reference_without_parameters():
    selector_reference = SelectorReference()
    assert selector_reference.symbol == ''
    assert len(selector_reference.data.columns) == 0


def test_init_selector_reference_with_parameters():
    selector_reference = SelectorReference(symbol="test")
    assert selector_reference.symbol == 'test'
    assert len(selector_reference.data.columns) == 0


def test_init_selector_reference_from_dict(dict_selector_reference):
    symbol = 'test'
    selector_reference = SelectorReference.from_dict(
        dict_selector_reference, symbol=symbol)
    assert selector_reference.symbol == symbol
    # assert isinstance(selector_reference.data.index, DatetimeIndex)
    # default type is float
    assert selector_reference.data['dummy'].dtypes == float
    assert selector_reference.data['dummy2'].dtypes == float
    assert len(selector_reference) == 2


def test_init_selector_reference_from_dict_type_specified(dict_selector_reference):
    symbol = 'test'
    selector_reference = SelectorReference.from_dict(
        dict_selector_reference, symbol=symbol, type_options={'dummy': float, 'dummy2': int})
    assert selector_reference.symbol == symbol
    # assert isinstance(selector_reference.data.index, DatetimeIndex)
    assert selector_reference.data['dummy'].dtypes == float
    assert selector_reference.data['dummy2'].dtypes == int
    assert len(selector_reference) == 2


@mock.patch('pandas.read_csv')
def test_init_selector_reference_from_csv_file(read_csv, dataframe_reference):
    read_csv.return_value = dataframe_reference
    selector_reference = SelectorReference.from_csv(
        'TEST.csv', symbol='TEST', type_options={'dummy': float, 'dummy2': int})
    read_csv.assert_called_with('TEST.csv')
    assert selector_reference.symbol == 'TEST'
    assert list(selector_reference.data.columns) == ['dummy', 'dummy2']
    # assert isinstance(selector_reference.data.index, DatetimeIndex)
    assert selector_reference.data['dummy'].dtypes == float
    assert selector_reference.data['dummy2'].dtypes == int
    assert len(selector_reference) == 2
