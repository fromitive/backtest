from enum import Enum
import dataclasses
import pandas as pd
from backtest.domains.stockdata import StockData
from backtest.domains.strategy_function import StrategyFunction
import typing

@dataclasses.dataclass
class Strategy:
    name: str = ''
    function : StrategyFunction = None
    weight: int = 0
    target: str = 'ALL'
    options: dict = dataclasses.field(default_factory=dict)