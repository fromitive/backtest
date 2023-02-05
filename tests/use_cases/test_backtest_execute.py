from backtest.use_cases.backtest_execute import backtest_execute
from backtest.domains.backtest import Backtest
from backtest.domains.strategy import Strategy
from backtest.domains.stockdata import StockData
from backtest.domains.backtest_result import BacktestResult
from backtest.response import ResponseSuccess
import pytest


@pytest.fixture(scope='function')
def strategy_list():
    strategy1 = Strategy()
    strategy2 = Strategy()
    strategy3 = Strategy()
    return [
        strategy1, strategy2, strategy3
    ]


@pytest.fixture(scope='function')
def stockdata_list():
    stockdata1 = StockData()
    stockdata2 = StockData()
    stockdata3 = StockData()
    return [
        stockdata1, stockdata2, stockdata3
    ]


def test_backtest_execute_without_options(strategy_list, stockdata_list):
    strategies = strategy_list
    stockdata = stockdata_list
    backtest = Backtest(strategy_list=strategies, stockdata_list=stockdata)
    response = backtest_execute(backtest)
    assert isinstance(response.value, BacktestResult)
    assert bool(response) == True
