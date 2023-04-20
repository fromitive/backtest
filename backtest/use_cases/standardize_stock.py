from datetime import datetime
from typing import List

from backtest.domains.stockdata import StockData
from backtest.util.stockdata_util import generate_empty_stockData


def standardize_stock(stockdata_list: List[StockData]):
    if len(stockdata_list) == 0:
        return []
    mindate = stockdata_list[0].data.index[0]
    for stockdata in stockdata_list:
        mindate = min(stockdata.data.index[0], mindate)
    empty_stockData = generate_empty_stockData(
        from_date=datetime.strftime(mindate, "%Y-%m-%d"))
    len(empty_stockData)
    for stockdata in stockdata_list:
        stockdata += empty_stockData
