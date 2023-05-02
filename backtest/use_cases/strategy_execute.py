from typing import List

import numpy

from backtest.domains.stockdata import StockData
from backtest.domains.strategy import Strategy, StrategyExecuteFlagType
from backtest.domains.strategy_result import (StrategyResult,
                                              StrategyResultColumnType)
from backtest.module_compet.pandas import pd
from backtest.response import ResponseFailure, ResponseSuccess, ResponseTypes


def basic_function(data: StockData, weight: int, name: str):
    response = pd.DataFrame(
        index=data.data.index, columns=[name])
    response[name] = [(
        StrategyResultColumnType.KEEP, weight)] * len(data)
    return response


def min_max_function(data: StockData, weight: int, name: str, avg_rolling: int = 7, avg_vol_rate: float = 2.0, high_low_diff_rate: float = 0.10):
    response = pd.DataFrame(
        index=data.data.index, columns=[name])
    temp_df = data.data.copy()
    temp_df['avg_vol'] = temp_df['volume'].rolling(avg_rolling).mean()

    def _min_max_function(r):
        current_avg_vol_rate = (r.volume / r.avg_vol)
        current_high_low_rate = (r.high - r.low)
        if current_avg_vol_rate >= avg_vol_rate and current_high_low_rate >= high_low_diff_rate:
            return (StrategyResultColumnType.BUY, weight)
        else:
            return (StrategyResultColumnType.KEEP, 0)
    temp_df['result'] = temp_df.apply(lambda r: _min_max_function(r), axis=1)

    response[name] = temp_df['result']
    return response


def _dataframe_sma(df: pd.DataFrame, weight: int, rolling=100):
    df['sma'] = df['close'].rolling(rolling).mean().fillna(0)
    df['smashift'] = df['sma'].shift(1).fillna(0)

    def _sma_internal(r):
        if (r.smashift - r.sma) > 0.0:
            return (StrategyResultColumnType.SELL, weight)
        elif (r.smashift - r.sma) == 0.0:
            return (StrategyResultColumnType.KEEP, weight)
        else:
            return (StrategyResultColumnType.BUY, weight)
    df['result'] = df.apply(lambda r: _sma_internal(r), axis=1)
    return df


def _dataframe_sma_multi(df: pd.DataFrame, weight: int, rolling_list: List[int]):
    for rolling in rolling_list:
        df['sma_{}'.format(rolling)] = df['close'].rolling(
            rolling).mean().fillna(0)

    def _sma_internal(r):
        if all([(r.close - r['sma_{}'.format(rolling)]) > 0.0 for rolling in rolling_list]):
            return (StrategyResultColumnType.SELL, weight)
        elif all([(r.close - r['sma_{}'.format(rolling)]) < 0.0 for rolling in rolling_list]):
            return (StrategyResultColumnType.BUY, weight)
        else:
            return (StrategyResultColumnType.KEEP, weight)
    df['result'] = df.apply(lambda r: _sma_internal(r), axis=1)
    return df


def sma_function(data: StockData, weight: int, name: str, rolling=100):
    response = pd.DataFrame(
        index=data.data.index, columns=[name])
    """
    strategyfunction here
    """
    response = _dataframe_sma(data.data, weight, rolling)
    response = response.rename({'result': name}, axis=1)
    return response[[name]]


def buy_rate_function(data: StockData, weight: int, name: str,
                      buy_rolling: int = 30, buy_rate: float = 0.5):
    temp_df = data.data.copy()
    temp_df['buy_rolling'] = temp_df['high'].rolling(
        buy_rolling).max()
    """
    strategyfunction here
    """

    def _buy_rate(r: pd.Series):
        if pd.isna(r.buy_rolling) or r.buy_rolling == 0.0:
            return (StrategyResultColumnType.KEEP, 0)
        else:
            current_buy_rate = r.close / r.buy_rolling
            if current_buy_rate <= buy_rate:
                return (StrategyResultColumnType.BUY, weight)
        return (StrategyResultColumnType.KEEP, 0)

    temp_df[name] = temp_df.apply(lambda r: _buy_rate(r), axis=1)
    return temp_df[[name]]


def sell_rate_function(data: StockData, weight: int, name: str,
                       sell_rolling: int = 30, sell_rate: float = 0.5, base: str = "top"):
    temp_df = data.data.copy()
    if base.lower() == "bottom":
        temp_df['sell_rolling'] = temp_df['low'].rolling(sell_rolling).min()
    else:
        temp_df['sell_rolling'] = temp_df['high'].rolling(
            sell_rolling).max()
    """
    strategyfunction here 
    """
    def _sell_rate(r: pd.Series):
        if pd.isna(r.sell_rolling) or r.sell_rolling == 0.0:
            return (StrategyResultColumnType.KEEP, 0)
        else:
            current_sell_rate = r.close / r.sell_rolling
            if current_sell_rate >= sell_rate:
                return (StrategyResultColumnType.SELL, weight)
        return (StrategyResultColumnType.KEEP, 0)

    temp_df[name] = temp_df.apply(lambda r: _sell_rate(r), axis=1)
    return temp_df[[name]]


def sma_big_stock_function(data: StockData, weight: int, name: str, big_stock: StockData, rolling=100):
    response = pd.DataFrame(
        index=data.data.index, columns=[name])
    """
    strategyfunction here
    """
    response = _dataframe_sma(
        big_stock.data, weight, rolling)
    response = response.rename({'result': name}, axis=1)
    return response[[name]]


def sma_multi_function(data: StockData, weight: int, name: str, rolling_list: List[int] = [15, 100]):
    response = pd.DataFrame(
        index=data.data.index, columns=[name])
    """
    strategyfunction here
    """
    response = _dataframe_sma_multi(
        data.data, weight, rolling_list)
    response = response.rename({'result': name}, axis=1)
    return response[[name]]


def sma_multi_big_stock_function(data: StockData, weight: int, name: str, big_stock: StockData, rolling_list: List[int] = [15, 100]):
    response = pd.DataFrame(
        index=data.data.index, columns=[name])
    """
    strategyfunction here
    """
    response = _dataframe_sma_multi(
        big_stock.data, weight, rolling_list)
    response = response.rename({'result': name}, axis=1)
    return response[[name]]


def _calculate_rsi(data, period):
    delta = data.diff().dropna()
    gain = delta.where(delta > 0, 0)
    loss = -delta.where(delta < 0, 0)

    avg_gain = gain.rolling(window=period).mean()
    avg_loss = loss.rolling(window=period).mean()

    rs = avg_gain / avg_loss
    rsi = 100 - (100 / (1 + rs))
    return rsi


def rsi_function(data: StockData, weight: int, name: str, period: int, sell_score: int, buy_score: int, keep_weight: int = -1):
    response = pd.DataFrame(
        index=data.data.index, columns=[name])
    data.data['rsi'] = _calculate_rsi(data.data['close'], period)

    def _rsi_function(r):
        if r <= buy_score:
            return (StrategyResultColumnType.BUY, weight)
        elif r >= sell_score:
            return (StrategyResultColumnType.SELL, weight)
        else:
            if keep_weight == -1:
                return (StrategyResultColumnType.KEEP, weight)
            else:
                return (StrategyResultColumnType.KEEP, keep_weight)
    response[name] = data.data.apply(
        lambda r: _rsi_function(r['rsi']), axis=1)
    return response


def rsi_big_stock_function(data: StockData, weight: int, name: str, big_stock: StockData, period: int, sell_score: int, buy_score: int, keep_weight: int = -1):
    response = pd.DataFrame(
        index=data.data.index, columns=[name])
    data.data['rsi'] = _calculate_rsi(big_stock.data['close'], period)

    def _rsi_function(r):
        if r <= buy_score:
            return (StrategyResultColumnType.BUY, weight)
        elif r >= sell_score:
            return (StrategyResultColumnType.SELL, weight)
        else:
            if keep_weight == -1:
                return (StrategyResultColumnType.KEEP, weight)
            else:
                return (StrategyResultColumnType.KEEP, keep_weight)
    response[name] = data.data.apply(
        lambda r: _rsi_function(r['rsi']), axis=1)
    return response


def greed_fear_index_function(data: StockData, weight: int, name: str, greed_fear_index_data: pd.DataFrame, index_fear: int, index_greed: int):
    response = pd.DataFrame(
        index=data.data.index, columns=[name])
    raw_result = data.data.join(greed_fear_index_data, how='inner')

    def _greed_fear_index(r):
        if (r['value']) <= index_fear:  # extreme greed
            return (StrategyResultColumnType.BUY, weight)
        elif (r['value'] >= index_greed):
            return (StrategyResultColumnType.SELL, weight)
        else:
            return (StrategyResultColumnType.KEEP, weight)
    response[name] = raw_result.apply(
        lambda r: _greed_fear_index(r), axis=1)
    return response


def _inner_strategy_execute(strategy: Strategy, data: StockData):
    try:
        if not strategy.function:
            strategy.function = basic_function
        response = strategy.function(
            data=data, weight=strategy.weight, name=strategy.name, **strategy.options)
        return ResponseSuccess(response)
    except Exception as e:
        return ResponseFailure(ResponseTypes.SYSTEM_ERROR, e)


def _sum_strategy(series: pd.Series, stockdata: StockData, weight_score_function):
    total_result = {StrategyResultColumnType.KEEP: 0,
                    StrategyResultColumnType.SELL: 0,
                    StrategyResultColumnType.BUY: 0}
    for idx in series.index:
        type, weight = series[idx]
        if stockdata.data['volume'][series.name] == 0.0:
            total_result[StrategyResultColumnType.KEEP] += weight
        else:
            total_result[type] += weight
    score_value = sorted(total_result.values(), reverse=True)
    strategy_rate = weight_score_function(
        first=score_value[0], second=score_value[1], third=score_value[2])
    return [max(total_result, key=total_result.get), strategy_rate]


def _inverse_strategy(row: pd.Series, name: str):
    col_type, weight = row[name]
    if col_type == StrategyResultColumnType.BUY:
        return (StrategyResultColumnType.SELL, weight)
    elif col_type == StrategyResultColumnType.SELL:
        return (StrategyResultColumnType.BUY, weight)
    else:
        return (StrategyResultColumnType.KEEP, weight)


def _sellonly_strategy(row: pd.Series, name: str):
    col_type, weight = row[name]
    if col_type == StrategyResultColumnType.BUY:
        return (StrategyResultColumnType.KEEP, 0)
    else:
        return (col_type, weight)


def _buyonly_strategy(row: pd.Series, name: str):
    col_type, weight = row[name]
    if col_type == StrategyResultColumnType.SELL:
        return (StrategyResultColumnType.KEEP, 0)
    else:
        return (col_type, weight)


def _basic_weight_score_function(first: int, second: int, third: int):
    return ((first + 1) * 2) / \
        (1 + second + third)

def strategy_execute(strategy_list: List[Strategy], stockdata: StockData, save_strategy_result: bool = False, weight_score_function=_basic_weight_score_function):
    strategy_total_result = pd.DataFrame(
        index=stockdata.data.index)
    strategy_bucket = set()
    strategy_name_list = set([strategy.name for strategy in strategy_list])
    strategy_dict = {bucket_item: 1 for bucket_item in strategy_name_list}
    for strategy in strategy_list:
        if strategy.name in strategy_bucket:
            strategy_dict[strategy.name] += 1
            strategy.name = "{}_{}".format(
                strategy.name, strategy_dict[strategy.name])
        else:
            strategy_bucket.add(strategy.name)
        response = _inner_strategy_execute(
            strategy=strategy, data=stockdata)
        if isinstance(response, ResponseFailure):
            return ResponseFailure(ResponseTypes.SYSTEM_ERROR, "strategy function error occured!")
        else:
            strategy_result = response.value
            if strategy.inverse:
                strategy_result[strategy.name] = strategy_result.apply(
                    lambda row: _inverse_strategy(row, strategy.name), axis=1)
            if strategy.flag == StrategyExecuteFlagType.SELLONLY:
                strategy_result[strategy.name] = strategy_result.apply(
                    lambda row: _sellonly_strategy(row, strategy.name), axis=1)
            elif strategy.flag == StrategyExecuteFlagType.BUYONLY:
                strategy_result[strategy.name] = strategy_result.apply(
                    lambda row: _buyonly_strategy(row, strategy.name), axis=1)

            if len(stockdata) >= len(strategy_result):
                strategy_total_result = strategy_total_result.join(
                    strategy_result, how='left', rsuffix='{}_'.format(strategy.name))
            else:
                strategy_total_result = strategy_total_result.join(
                    strategy_result, how='inner', rsuffix='{}_'.format(strategy.name))
    # fill na with
    for column in strategy_total_result.columns:
        strategy_total_result[column] = strategy_total_result[column].fillna(
            {i: (StrategyResultColumnType.KEEP, 0) for i in strategy_total_result.index})
    if save_strategy_result:
        strategy_total_result.to_csv(
            "{}_strategy_total_result.csv".format(stockdata.symbol))
    return ResponseSuccess(StrategyResult(strategy_total_result.apply(
        lambda row: _sum_strategy(row, stockdata, weight_score_function), axis=1)))
