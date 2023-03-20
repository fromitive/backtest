from backtest.domains.strategy import Strategy
from backtest.domains.stockdata import StockData
from backtest.domains.strategy_result import StrategyResult, StrategyResultColumnType
from backtest.response import ResponseFailure, ResponseSuccess, ResponseTypes
from backtest.util.stockdata_util import get_greed_fear_index
import pandas as pd


def basic_function(data: StockData, weight: int, name: str):
    response = StrategyResult(value=pd.DataFrame(
        index=data.data.index, columns=[name]))
    response.value[name] = [(
        StrategyResultColumnType.KEEP, weight)] * len(data)
    return response


def _dataframe_rsi(df: pd.DataFrame, weight: int, rolling=100):
    df['sma'] = df['close'].rolling(rolling).mean().fillna(0)
    df['smashift'] = df['sma'].shift(1).fillna(0)

    def _rsi_internal(r):
        if (r.smashift - r.sma) > 0.0:
            return (StrategyResultColumnType.SELL, weight)
        elif (r.smashift - r.sma) == 0.0:
            return (StrategyResultColumnType.KEEP, weight)
        else:
            return (StrategyResultColumnType.BUY, weight)
    df['result'] = df.apply(lambda r: _rsi_internal(r), axis=1)
    return df


def rsi_function(data: StockData, weight: int, name: str, rolling=100):
    response = StrategyResult(value=pd.DataFrame(
        index=data.data.index, columns=[name]))
    """
    strategyfunction here
    """
    response.value[name] = _dataframe_rsi(data.data, weight, rolling)['result']
    return response


def rsi_big_stock_function(data: StockData, weight: int, name: str, big_stock: StockData, rolling=100):
    response = StrategyResult(value=pd.DataFrame(
        index=data.data.index, columns=[name]))
    """
    strategyfunction here
    """
    response.value[name] = _dataframe_rsi(
        big_stock.data, weight, rolling)['result']
    return response

def greed_fear_index_function(data: StockData, weight: int, name: str, greed_fear_index_data: pd.DataFrame,index_fear: int,index_greed: int):
    response = StrategyResult(value=pd.DataFrame(
    index=data.data.index, columns=[name]))
    raw_result=data.data.join(greed_fear_index_data,how='inner')
    def _greed_fear_index(r):
        if (r['value']) < index_fear: # extreme greed
            return (StrategyResultColumnType.BUY, weight)
        elif (r['value'] > index_greed):
            return (StrategyResultColumnType.BUY, weight)
        else:
            return (StrategyResultColumnType.KEEP, weight)
    response.value[name] = raw_result.apply(lambda r: _greed_fear_index(r), axis=1)
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
