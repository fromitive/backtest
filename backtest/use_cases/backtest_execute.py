from typing import Tuple

import numpy as np

from backtest.domains.backtest import Backtest
from backtest.domains.backtest_result import BacktestResult
from backtest.domains.selector_result import SelectorResultColumnType
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
    if max_bucket_cnt == 0:
        return 0.0
    profit = backtest_result[column_name]
    bucket_cnt = 0
    if backtest_result['shift_stock_bucket'] is not None:
        for symbol in backtest_result['shift_stock_bucket']:
            bucket_cnt += backtest_result['shift_stock_bucket'][symbol]
    return profit * (bucket_cnt / max_bucket_cnt)


def _calc_diff(bucket_item, symbol, stockdata_dict, index):
    profit_index = bucket_item
    return stockdata_dict[symbol].data['close'][index] - \
        stockdata_dict[symbol].data['close'][profit_index]


def _calc_symbol_profit(profit_price: float, current_price: float, bucket_cnt: int) -> float:
    profit_earn = current_price - profit_price
    symbol_profit = (profit_earn / profit_price) / bucket_cnt
    return symbol_profit


def backtest_execute(backtest: Backtest, verbose: bool = True):
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
    # TODO: future : to apply post_strategy_list
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
    index_len = len(base_index)
    for num, index in enumerate(base_index, start=1):

        if verbose == True:
            print(
                'calc backtest {current} / {total}'.format(current=num, total=index_len))
        stockdata_cnt = 0
        pick_stockdata_list = []
        if backtest.selector_result is None:
            stockdata_cnt = len(backtest.stockdata_list)
            pick_stockdata_list = backtest.stockdata_list
        else:
            selector_result_df = backtest.selector_result.value
            stockdata_symbol_list_df = selector_result_df.apply(
                lambda row: row.index[row == SelectorResultColumnType.SELECT].tolist(), axis=1)
            stockdata_symbol_list = stockdata_symbol_list_df[index]
            stockdata_cnt = len(stockdata_symbol_list)
            pick_stockdata_list = [
                stockdata for stockdata in backtest.stockdata_list if stockdata.symbol in stockdata_symbol_list]

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
            strategy_result_of_day, strategy_rate = strategy_result_dict[
                stockdata.symbol].value[index]
            if strategy_result_of_day == StrategyResultColumnType.BUY:
                stock_bucket_dict[stockdata.symbol].append(index)
                bucket_cnt += 1
        has_bucket_symbol_list = [
            symbol for symbol in stock_bucket_dict if stock_bucket_dict[symbol]]
        for symbol in has_bucket_symbol_list:
            symbol_profit = 0.0
            for profit_index in stock_bucket_dict[symbol]:
                symbol_profit += _calc_symbol_profit(profit_price=stockdata_dict[symbol].data['close'][profit_index],
                                                     current_price=stockdata_dict[symbol].data['close'][index],
                                                     bucket_cnt=bucket_cnt)
            stock_profit_dict[symbol] = symbol_profit
            total_potential_profit += symbol_profit

            if len(stock_bucket_dict[symbol]) > 0:  # already calcuated stock
                strategy_result_of_day, strategy_rate = strategy_result_dict[symbol].value[index]
                # sell strategy execute
                if strategy_result_of_day == StrategyResultColumnType.SELL:
                    # cacluate sell_rate
                    sell_bucket_length = int(
                        len(stock_bucket_dict[symbol]) * strategy_rate)
                    bucket_cnt -= sell_bucket_length
                    sorted_bucket_list = sorted(
                        stock_bucket_dict[symbol], key=lambda v: _calc_diff(v, symbol, stockdata_dict, index))
                    sell_profit = 0.0
                    for count in range(sell_bucket_length):
                        profit_index = sorted_bucket_list.pop()
                        sell_profit += _calc_symbol_profit(profit_price=stockdata_dict[symbol].data['close'][profit_index],
                                                           current_price=stockdata_dict[symbol].data['close'][index],
                                                           bucket_cnt=bucket_cnt)
                    stock_profit_dict[symbol] -= sell_profit
                    stock_bucket_dict[symbol] = sorted_bucket_list
                    backtest_result_raw.at[index,
                                           'total_profit'] += sell_profit
                    total_potential_profit -= stock_profit_dict[symbol]
        backtest_result_raw.at[index,
                               'total_potential_profit'] = total_potential_profit

        # TODO: post_strategy_execute
        backtest_result_raw.at[index, 'stock_bucket'] = _calc_stock_count(
            stock_bucket_dict)
        max_bucket_cnt = max(max_bucket_cnt, bucket_cnt)

        if index == base_index[-2]:
            print('today coin list')
            for today_symbol in pick_stockdata_list:
                print('{symbol_name} : {strategy_result}'.format(
                    symbol_name=today_symbol.symbol, strategy_result=strategy_result_dict[today_symbol.symbol].value[index]))
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
