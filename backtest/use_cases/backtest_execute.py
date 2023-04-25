import os
import sys

import numpy as np

from backtest.domains.backtest import Backtest
from backtest.domains.backtest_result import BacktestResult
from backtest.domains.strategy_result import (StrategyResult,
                                              StrategyResultColumnType)
from backtest.module_compet.pandas import pd
from backtest.response import ResponseFailure, ResponseSuccess, ResponseTypes
from backtest.use_cases.standardize_stock import standardize_stock
from backtest.use_cases.strategy_execute import strategy_execute


def _calc_stock_count(stock_bucket):
    summary_bucket = dict()
    for symbol in stock_bucket:
        symbol_bucket_len = len(stock_bucket[symbol])
        if symbol_bucket_len > 0:
            summary_bucket[symbol] = symbol_bucket_len
    return summary_bucket


def _recalc_profit(backtest_result: pd.Series, max_bucket_cnt: int, column_name: str):
    profit = backtest_result[column_name]
    bucket_cnt = 0
    if backtest_result['shift_stock_bucket'] is not None:
        for symbol in backtest_result['shift_stock_bucket']:
            bucket_cnt += backtest_result['shift_stock_bucket'][symbol]
    return profit * (bucket_cnt / max_bucket_cnt)


def backtest_execute(backtest: Backtest):
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
    # future : to apply post_strategy_list
    # post_strategy_list = [
    #     strategy for strategy in backtest.strategy_list if strategy.after]

    # init backtest_result
    backtest_result_raw = pd.DataFrame(
        index=base_index, columns=['total_profit', 'stock_bucket', 'total_potential_profit'])
    backtest_result_raw['total_profit'] = 0.0
    backtest_result_raw['stock_bucket'] = np.nan
    backtest_result_raw['stock_bucket'].astype('object')
    backtest_result_raw.at[backtest_result_raw.index[0],
                           'stock_bucket'] = 'DUMMY'
    backtest_result_raw['total_potential_profit'] = 0.0

    # loop base_index
    bucket_cnt = 0
    max_bucket_cnt = 0
    for index in base_index:
        pick_stockdata_list = backtest.stockdata_list  # future fix
        # calc potential profit and each symbol stock profit
        total_potential_profit = 0.0
        for stockdata in pick_stockdata_list:
            # if not calc pre_strategy_result calc it.
            if not isinstance(strategy_result_dict[stockdata.symbol], StrategyResult):
                response = strategy_execute(
                    strategy_list=pre_strategy_list, stockdata=stockdata)
                if isinstance(response, ResponseSuccess):
                    strategy_result_dict[stockdata.symbol] = response.value
                else:
                    return ResponseFailure(ResponseTypes.SYSTEM_ERROR, "[ERROR] strategy_execute")
            strategy_result_of_day = strategy_result_dict[stockdata.symbol].value[index]
            if strategy_result_of_day == StrategyResultColumnType.BUY:
                stock_bucket_dict[stockdata.symbol].append(index)
                bucket_cnt += 1

        for symbol in stockdata_dict:
            symbol_profit = 0.0
            for profit_index in stock_bucket_dict[symbol]:
                profit_earn = stockdata_dict[symbol].data['close'][index] - \
                    stockdata_dict[symbol].data['close'][profit_index]
                profit_base = stockdata_dict[symbol].data['close'][profit_index]
                symbol_profit += ((profit_earn / profit_base) /
                                  bucket_cnt) / stockdata_cnt
            stock_profit_dict[symbol] = symbol_profit
            total_potential_profit += symbol_profit

            if len(stock_bucket_dict[symbol]) > 0:  # already calcuated stock
                strategy_result_of_day = strategy_result_dict[symbol].value[index]
                # sell strategy execute
                if strategy_result_of_day == StrategyResultColumnType.SELL:
                    bucket_cnt -= len(stock_bucket_dict[symbol])
                    stock_bucket_dict[symbol] = []
                    backtest_result_raw.at[index,
                                           'total_profit'] += stock_profit_dict[symbol]
                    total_potential_profit -= stock_profit_dict[symbol]
        backtest_result_raw.at[index,
                               'total_potential_profit'] = total_potential_profit

        # TODO: post_strategy_execute
        backtest_result_raw.at[index, 'stock_bucket'] = _calc_stock_count(
            stock_bucket_dict)
        max_bucket_cnt = max(max_bucket_cnt, bucket_cnt)
        # calculate profit and bucket
        # post strategy execute
    backtest_result_raw['shift_stock_bucket'] = backtest_result_raw['stock_bucket'].shift(
        1)
    backtest_result_raw['total_profit'] = backtest_result_raw.apply(
        lambda r: _recalc_profit(r, max_bucket_cnt, 'total_profit'), axis=1)
    backtest_result_raw['total_potential_profit'] = backtest_result_raw.apply(
        lambda r: _recalc_profit(r, max_bucket_cnt, 'total_potential_profit'), axis=1)
    backtest_result_raw = backtest_result_raw.drop(
        ['shift_stock_bucket'], axis=1)
    return ResponseSuccess(BacktestResult(value=backtest_result_raw))
    # except Exception as e:
    #    exc_type, exc_obj, exc_tb = sys.exc_info()
    #    fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
    #    print(exc_type, fname, exc_tb.tb_lineno)
    #    return ResponseFailure(ResponseTypes.SYSTEM_ERROR, e)
