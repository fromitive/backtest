from backtest.domains.backtest import Backtest
from backtest.domains.backtest_result import BacktestResult
from backtest.domains.strategy import Strategy
from backtest.domains.strategy import StockData
from backtest.domains.strategy_result import StrategyResultColumnType
from backtest.use_cases.strategy_execute import strategy_execute
from backtest.response import ResponseFailure, ResponseSuccess, ResponseTypes

import pandas as pd
import numpy as np
import math


def _sum_strategy(series: pd.Series):
    total_result = {StrategyResultColumnType.KEEP: 0,
                    StrategyResultColumnType.SELL: 0,
                    StrategyResultColumnType.BUY: 0}
    for idx in series.index:
        type, weight = series[idx]
        total_result[type] += weight
    return max(total_result, key=total_result.get)


def backtest_execute(backtest: Backtest):
    # try:
    backtest_result_raw = pd.DataFrame(
        index=pd.DatetimeIndex([]), columns=['total_profit', 'bucket'])
    for stockdata in backtest.stockdata_list:
        stockdata_raw = stockdata.data
        strategy_total_result = pd.DataFrame(index=pd.DatetimeIndex([]))
        for strategy in backtest.strategy_list:
            response = strategy_execute(strategy=strategy, data=stockdata)
            if isinstance(response, ResponseFailure):
                raise Exception('strategy_response error!')
            else:
                strategy_result = response.value
                if strategy_result.value.columns[0] not in strategy_total_result.columns:
                    if strategy_result.target == stockdata.symbol or strategy_result.target == 'ALL':
                        strategy_total_result = strategy_total_result.join(
                            strategy_result.value, how='outer')
        # fill na with
        for column in strategy_total_result.columns:
            strategy_total_result[column] = strategy_total_result[column].fillna(
                {i: (StrategyResultColumnType.KEEP, 0) for i in strategy_total_result.index})

        stockdata_raw['total'] = strategy_total_result.apply(
            lambda row: _sum_strategy(row), axis=1)
        # calculate profit and bucket
        stockdata_raw['total_profit'] = 0
        stockdata_raw['stock_bucket'] = np.nan
        stockdata_raw['stock_bucket'].astype('object')
        stockdata_raw.at[stockdata_raw.index[0], 'stock_bucket'] = 'DUMMY'
        backtest_bucket = []
        for index in stockdata_raw.index:
            if stockdata_raw['total'][index] == StrategyResultColumnType.BUY:
                backtest_bucket.append((stockdata.symbol, index))
            elif stockdata_raw['total'][index] == StrategyResultColumnType.SELL:
                sell_profit = 0
                if len(backtest_bucket) > 0:
                    for symbol, profit_index in backtest_bucket:
                        profit_earn = (
                            stockdata_raw['close'][index] - stockdata_raw['close'][profit_index])
                        profit_base = stockdata_raw['close'][profit_index]
                        sell_profit += profit_earn / profit_base
                stockdata_raw['total_profit'][index] = sell_profit
                backtest_bucket = []
            stockdata_raw.at[index, 'stock_bucket'] = backtest_bucket[:]
        stockdata_raw = stockdata_raw[['total_profit', 'stock_bucket']]
        # append total_result
        if len(backtest_result_raw.index) == 0:
            backtest_result_raw = stockdata_raw.copy()
        elif len(backtest_result_raw.index) > len(stockdata_raw.index):
            stockdata_raw = stockdata_raw.reindex(
                index=backtest_result_raw.index)
            stockdata_raw['total_profit'] = stockdata_raw['total_profit'].apply(
                lambda d: d if not math.isnan(d) else 0.0)
            stockdata_raw['stock_bucket'] = stockdata_raw['stock_bucket'].apply(
                lambda d: d if isinstance(d, list) else [])
        elif len(backtest_result_raw.index) < len(backtest_result_raw.index):
            backtest_result_raw = backtest_result_raw.reindex(
                index=stockdata_raw.index)
            backtest_result_raw['total_profit'] = backtest_result_raw['total_profit'].apply(
                lambda d: d if not math.isnan(d) else 0.0)
            backtest_result_raw['stock_bucket'] = backtest_result_raw['stock_bucket'].apply(
                lambda d: d if isinstance(d, list) else [])
        backtest_result_raw += stockdata_raw
    return ResponseSuccess(BacktestResult(value=backtest_result_raw))
    # except Exception as e:
    #    print(e)
    #    return ResponseFailure(ResponseTypes.SYSTEM_ERROR, e)
