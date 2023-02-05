from enum import Enum
import dataclasses
import pandas as pd
from backtest.domains.stockdata import StockData


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
    data: pd.DataFrame = pd.DataFrame(index=pd.DatetimeIndex([]))
    weight: int = 0
    subdata: pd.DataFrame = pd.DataFrame(index=pd.DatetimeIndex([]))
    target: str = 'ALL'

    def __post_init__(self):
        if isinstance(self.data, StockData):
            self.target = self.data.symbol
            self.data = self.data.data

    def __len__(self):
        return len(self.data)
