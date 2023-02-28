import pytest
from backtest.domains.backtest import Backtest
from backtest.domains.strategy import Strategy
from backtest.domains.stockdata import StockData


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


def test_init_backtest_with_parameters(strategy_list, stockdata_list):
    strategies = strategy_list
    stockdata = stockdata_list
    backtest = Backtest(strategy_list=strategies, stockdata_list=stockdata,buy_price=5000.0)
    assert backtest.strategy_list == strategies
    assert backtest.strategy_list[1] == strategies[1]
    assert len(backtest.strategy_list) == 3
    assert backtest.stockdata_list == stockdata
    assert len(backtest.stockdata_list) == 3
    assert backtest.stockdata_list[1] == stockdata[1]
