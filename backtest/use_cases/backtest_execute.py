from backtest.domains.backtest import Backtest
from backtest.domains.backtest_result import BacktestResult
from backtest.domains.strategy import Strategy
from backtest.domains.strategy import StockData
from backtest.domains.strategy_result import StrategyResultColumnType
from backtest.use_cases.strategy_execute import strategy_execute
from backtest.use_cases.standardize_stock import standardize_stock
from backtest.response import ResponseFailure, ResponseSuccess, ResponseTypes
from typing import List

import pandas as pd
import numpy as np
import math
import sys
import os


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
    for symbol in stock_bucket:
        symbol_bucket_len=len(stock_bucket[symbol])
        if symbol_bucket_len > 0:
            summary_bucket[symbol] = symbol_bucket_len
    return summary_bucket


def _generate_strategy_execute_result(strategy_list: List[Strategy], stockdata: StockData):
    strategy_total_result = pd.DataFrame(
        index=stockdata.data.index)

    for strategy in strategy_list:
        response = strategy_execute(
            strategy=strategy, data=stockdata)
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
    return strategy_total_result.apply(lambda row: _sum_strategy(row), axis=1)


def backtest_execute(backtest: Backtest):
    try:
        stockdata_cnt = len(backtest.stockdata_list)
        standardize_stock(stockdata_list=backtest.stockdata_list)
        base_index = backtest.stockdata_list[0].data.index

        # init dict
        strategy_result_dict = {
            stockdata.symbol: None for stockdata in backtest.stockdata_list}
        stockdata_dict = {
            stockdata.symbol: stockdata for stockdata in backtest.stockdata_list}
        stock_bucket_dict = {
            stockdata.symbol: [] for stockdata in backtest.stockdata_list}
        stock_profit_dict = {
            stockdata.symbol: 0.0 for stockdata in backtest.stockdata_list}

        # divide pre, post strategy list
        pre_strategy_list = [
            strategy for strategy in backtest.strategy_list if not strategy.after]
        post_strategy_list = [
            strategy for strategy in backtest.strategy_list if strategy.after]
        
        #init backtest_result
        backtest_result_raw = pd.DataFrame(
            index=base_index, columns=['total_profit', 'stock_bucket', 'total_potential_profit'])
        backtest_result_raw['total_profit']=0.0
        backtest_result_raw['stock_bucket'] = np.nan
        backtest_result_raw['stock_bucket'].astype('object')
        backtest_result_raw.at[backtest_result_raw.index[0],
                                'stock_bucket'] = 'DUMMY'
        backtest_result_raw['total_potential_profit']=0.0

        # loop base_index
        for index in base_index:
            pick_stockdata_list = backtest.stockdata_list  # future fix
            #calc potential profit and each symbol stock profit
            total_potential_profit = 0.0
            for symbol in stockdata_dict:
                symbol_profit=0.0
                for profit_index in stock_bucket_dict[symbol]:
                    profit_earn = stockdata_dict[symbol].data['close'][index] - \
                        stockdata_dict[symbol].data['close'][profit_index]
                    profit_base = stockdata_dict[symbol].data['close'][profit_index]
                    symbol_profit += (profit_earn /
                                    profit_base) / stockdata_cnt
                stock_profit_dict[symbol] = symbol_profit
                total_potential_profit += symbol_profit
            backtest_result_raw.at[index,'total_potential_profit'] = total_potential_profit
            for stockdata in pick_stockdata_list:
                # if not calc pre_strategy_result calc it.
                if not isinstance(strategy_result_dict[stockdata.symbol],pd.Series):
                    strategy_result_dict[stockdata.symbol] = _generate_strategy_execute_result(
                        strategy_list=pre_strategy_list, stockdata=stockdata)
                strategy_result_of_day = strategy_result_dict[stockdata.symbol][index]
                if strategy_result_of_day == StrategyResultColumnType.BUY:
                    stock_bucket_dict[stockdata.symbol].append(index)
                elif strategy_result_of_day == StrategyResultColumnType.SELL:
                    stock_bucket_dict[stockdata.symbol] = []
                    backtest_result_raw.at[index,'total_profit']+=stock_profit_dict[symbol]
                #TODO: post_strategy_execute
            backtest_result_raw.at[index,'stock_bucket'] = _calc_stock_count(stock_bucket_dict)
                    # calculate profit and bucket
            # post strategy execute
        return ResponseSuccess(BacktestResult(value=backtest_result_raw))
    except Exception as e:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        print(exc_type, fname, exc_tb.tb_lineno)
        return ResponseFailure(ResponseTypes.SYSTEM_ERROR, e)
