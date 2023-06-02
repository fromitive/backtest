from datetime import datetime
from typing import List

from backtest.domains.stockdata import StockData
from backtest.util.stockdata_util import generate_empty_stockData


def standardize_stock(stockdata_list: List[StockData], from_date='', to_date=''):
    if len(stockdata_list) == 0:
        return []
    indexes = set()
    for stockdata in stockdata_list:
        indexes.update(list(stockdata.data.index))
    indexes = list(indexes)
    indexes.sort()
    empty_stockData = generate_empty_stockData(indexes)
    for stockdata in stockdata_list:
        stockdata += empty_stockData
