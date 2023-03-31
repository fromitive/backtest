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

def _calc_stock_count(stock_bucket):
    summary_bucket = dict()
    for symbol,index in stock_bucket:
        if symbol not in summary_bucket:
            summary_bucket[symbol] = 1
        else:
            summary_bucket[symbol] +=1
    return summary_bucket 

def backtest_execute(backtest: Backtest):
    try:
        stockdata_cnt = len(backtest.stockdata_list)
        # divide pre post strategy list
        pre_strategy_list = [ strategy for strategy in backtest.strategy_list if not strategy.after ]
        post_strategy_list = [ strategy for strategy in backtest.strategy_list if strategy.after ]
        backtest_result_raw = pd.DataFrame(
            index=pd.DatetimeIndex([]), columns=['total_profit', 'bucket'])
        for stockdata in backtest.stockdata_list:
            stockdata_raw = stockdata.data
            strategy_pre_total_result = pd.DataFrame(index=pd.DatetimeIndex([]))
            #pre strategy execute
            for strategy in pre_strategy_list:
                response = strategy_execute(strategy=strategy, data=stockdata)
                if isinstance(response, ResponseFailure):
                    raise Exception('strategy_response error!')
                else:
                    strategy_result = response.value
                    if strategy_result.value.columns[0] not in strategy_pre_total_result.columns:
                        if strategy_result.target == stockdata.symbol or strategy_result.target == 'ALL':
                            strategy_pre_total_result = strategy_pre_total_result.join(
                                strategy_result.value, how='outer')
            # fill na with
            for column in strategy_pre_total_result.columns:
                strategy_pre_total_result[column] = strategy_pre_total_result[column].fillna(
                    {i: (StrategyResultColumnType.KEEP, 0) for i in strategy_pre_total_result.index})
            stockdata_raw['total_before'] = strategy_pre_total_result.apply(
                lambda row: _sum_strategy(row), axis=1)
            

            # calculate profit and bucket
            stockdata_raw['total_profit'] = 0
            stockdata_raw['stock_bucket'] = np.nan
            stockdata_raw['stock_bucket'].astype('object')
            stockdata_raw.at[stockdata_raw.index[0], 'stock_bucket'] = 'DUMMY'
            backtest_bucket = []
            for index in stockdata_raw.index:
                sell_profit = 0
                if len(backtest_bucket) > 0:
                    bucket_len = len(backtest_bucket)
                    for symbol, profit_index in backtest_bucket:
                        profit_earn = (stockdata_raw['close'][index] - stockdata_raw['close'][profit_index])
                        profit_base = stockdata_raw['close'][profit_index]
                        sell_profit += ((profit_earn / profit_base) / bucket_len) / stockdata_cnt
                
                if stockdata_raw['total_before'][index] == StrategyResultColumnType.BUY:
                    backtest_bucket.append(
                        (stockdata.symbol, index))
                elif stockdata_raw['total_before'][index] == StrategyResultColumnType.SELL:
                    stockdata_raw.at[index, 'total_profit'] = sell_profit
                    backtest_bucket = []

                stockdata_raw.at[index, 'stock_bucket'] = backtest_bucket[:]
                stockdata_raw.at[index, 'total_potential_profit'] = sell_profit

            #post strategy execute
            strategy_post_total_result = pd.DataFrame(index=pd.DatetimeIndex([]))
            for strategy in post_strategy_list:
                response = strategy_execute(strategy=strategy, data=stockdata)
                if isinstance(response, ResponseFailure):
                    raise Exception('strategy_response error!')
                else:
                    strategy_result = response.value
                    if strategy_result.value.columns[0] not in strategy_post_total_result.columns:
                        if strategy_result.target == stockdata.symbol or strategy_result.target == 'ALL':
                            strategy_post_total_result = strategy_post_total_result.join(
                                strategy_result.value, how='outer')
                            
            # fill na with
            for column in strategy_post_total_result.columns:
                strategy_post_total_result[column] = strategy_post_total_result[column].fillna(
                    {i: (StrategyResultColumnType.KEEP, 0) for i in strategy_post_total_result.index})
            stockdata_raw['total_after'] = strategy_post_total_result.apply(
                lambda row: _sum_strategy(row), axis=1)

            stockdata_raw = stockdata_raw[['total_profit', 'stock_bucket','total_potential_profit']]
            # append total_result
            if len(backtest_result_raw.index) == 0:
                backtest_result_raw = stockdata_raw.copy()
                continue
            elif len(backtest_result_raw.index) > len(stockdata_raw.index):
                stockdata_raw = stockdata_raw.reindex(
                    index=backtest_result_raw.index)
                stockdata_raw['total_profit'] = stockdata_raw['total_profit'].apply(
                    lambda d: d if not math.isnan(d) else 0.0)
                stockdata_raw['total_potential_profit'] = stockdata_raw['total_potential_profit'].apply(
                    lambda d: d if not math.isnan(d) else 0.0)
                stockdata_raw['stock_bucket'] = stockdata_raw['stock_bucket'].apply(
                    lambda d: d if isinstance(d, list) else [])
            elif len(backtest_result_raw.index) < len(stockdata_raw.index):
                backtest_result_raw = backtest_result_raw.reindex(
                    index=stockdata_raw.index)
                backtest_result_raw['total_profit'] = backtest_result_raw['total_profit'].apply(
                    lambda d: d if not math.isnan(d) else 0.0)
                backtest_result_raw['total_potential_profit'] = backtest_result_raw['total_potential_profit'].apply(
                    lambda d: d if not math.isnan(d) else 0.0)
                backtest_result_raw['stock_bucket'] = backtest_result_raw['stock_bucket'].apply(
                    lambda d: d if isinstance(d, list) else [])
            backtest_result_raw += stockdata_raw
        backtest_result_raw['total_stock_count'] = backtest_result_raw['stock_bucket'].apply(
            lambda d: len(d) )
        backtest_result_raw['stock_count'] = backtest_result_raw['stock_bucket'].apply(
            lambda d: _calc_stock_count(d))
        return ResponseSuccess(BacktestResult(value=backtest_result_raw))
    except Exception as e:
        print(e)
        return ResponseFailure(ResponseTypes.SYSTEM_ERROR, e)
