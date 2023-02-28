import dataclasses
from typing import List
from backtest.domains.stockdata import StockData
from backtest.domains.strategy import Strategy


@dataclasses.dataclass
class Backtest:
    strategy_list: List[Strategy]
    stockdata_list: List[StockData]
    buy_price : float = 5000.0