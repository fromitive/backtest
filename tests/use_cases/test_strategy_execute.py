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
