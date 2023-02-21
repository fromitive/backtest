from backtest.domains.strategy import Strategy
from backtest.domains.stockdata import StockData
from backtest.domains.strategy_result import StrategyResult, StrategyResultColumnType
from backtest.response import ResponseFailure, ResponseSuccess, ResponseTypes
import pandas as pd


def basic_function(data: StockData, weight: int, name: str):
    response = StrategyResult(value=pd.DataFrame(
        index=data.data.index, columns=[name]))
    response.value[name] = [(
        StrategyResultColumnType.KEEP, weight)] * len(data)
    return response


def strategy_execute(strategy: Strategy, data: StockData):
    try:
        if not strategy.function:
            strategy.function = basic_function
        response = strategy.function(
            data=data, weight=strategy.weight, name=strategy.name, **strategy.options)
        return ResponseSuccess(response)
    except Exception as e:
        return ResponseFailure(ResponseTypes.SYSTEM_ERROR, e)
