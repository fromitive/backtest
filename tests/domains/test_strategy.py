from backtest.domains.strategy import Strategy
from backtest.domains.stockdata import StockData
import typing
import pytest

def test_init_strategy_with_empty_parameters():
    strategy = Strategy()
    assert strategy.name == ''
    assert strategy.function == None
    assert strategy.weight == 0
    assert strategy.target == 'ALL'


def test_init_strategy_with_parameters():
    strategy = Strategy(name='test strategy', weight=1, target='TARGET',options={'option1':'value1','option2':'value2'})
    assert strategy.name == 'test strategy'
    assert strategy.weight == 1
    assert strategy.function == None
    assert strategy.target == 'TARGET'
    assert strategy.options == {'option1': 'value1', 'option2': 'value2'}

