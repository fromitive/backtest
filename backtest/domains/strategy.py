from enum import Enum
import dataclasses
import pandas as pd
from backtest.domains.stockdata import StockData
from backtest.domains.strategy_function import StrategyFunction
import typing


class StrategyType(Enum):
    with_onchain = 1
    with_stockdata = 2
    with_userdata = 3
    no_type = 4
    with_stockdata_and_subdata = 5


@dataclasses.dataclass
class Strategy:
    name: str = ''
    type: StrategyType = StrategyType.no_type
    data: typing.List[StockData] = dataclasses.field(default_factory=list)
    function : StrategyFunction = None
    weight: int = 0
    target: str = 'ALL'
    options: dict = dataclasses.field(default_factory=dict)

    def __post_init__(self):
        if isinstance(self.data, StockData):
            self.data = [self.data]

    def __len__(self):
        return len(self.data)
