from unittest import mock

import pytest

from backtest.domains.selector import Selector
from backtest.domains.selector_reference import SelectorReference
from backtest.domains.selector_result import (SelectorResult,
                                              SelectorResultColumnType)
from backtest.response import ResponseSuccess
from backtest.use_cases.selector_execute import selector_execute


@pytest.fixture(scope='function')
def sample_selector_reference():
    dict = {'date': ['2022-10-11', '2022-11-11'],
            'dummy1': [1.1, 2.2],
            'dummy2': ['11', '22']}
    return SelectorReference.from_dict(dict, symbol='TEST')


@pytest.fixture(scope='function')
def sample_selector_result():
    dict = {'SYMBOL1': [(SelectorResultColumnType.KEEP, 100), (SelectorResultColumnType.SELECT, 100)],
            'SYMBOL2': [(SelectorResultColumnType.KEEP, 100), (SelectorResultColumnType.SELECT, 100)],
            'date': ['2022-10-30', '2022-11-11']}
    return SelectorResult.from_dict(adict=dict)


def test_selector_execute(sample_selector_reference, sample_selector_result):
    symbol_list = ['SYMBOL1', 'SYMBOL2']
    test_function = mock.Mock()
    test_function.return_value = sample_selector_result
    selector = Selector(name='test selector', weight=100, selector_function=test_function, options={
                        'param1': 'value1', 'param2': 'value2'}, reference=sample_selector_reference)
    response = selector_execute(
        selector=selector, symbol_list=['SYMBOL1', 'SYMBOL2'])
    test_function.assert_called_once_with(
        symbol_list=symbol_list, weight=selector.weight, name=selector.name, reference=selector.reference, param1='value1', param2='value2')
    selector_result = response.value
    assert isinstance(response, ResponseSuccess)
    assert isinstance(selector_result, SelectorResult)
    assert list(selector_result.value.columns) == ['SYMBOL1', 'SYMBOL2']
