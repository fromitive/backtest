import math

import numpy as np

from backtest.domains.backtest import Backtest
from backtest.domains.backtest_result import BacktestResult
from backtest.domains.selector_result import SelectorResultColumnType
from backtest.domains.strategy_result import (StrategyResult,
                                              StrategyResultColumnType)
from backtest.module_compet.pandas import pd
from backtest.response import ResponseFailure, ResponseSuccess, ResponseTypes
from backtest.use_cases.standardize_stock import standardize_stock
from backtest.use_cases.strategy_execute import (_basic_weight_score_function,
                                                 strategy_execute)


def _calc_stock_count(stock_bucket):
    summary_bucket = dict()
    for symbol in stock_bucket:
        stock_bucket_df = stock_bucket[symbol]
        has_stock_bucket = stock_bucket_df.loc[stock_bucket_df['bucket'] > 0]
        symbol_bucket_len = has_stock_bucket['bucket'].sum()
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


def _calc_stock_profit_hash_table(index_list, stockdata_dict, verbose: bool = False):
    result_dict = {
        symbol: pd.DataFrame(index=index_list) for symbol in stockdata_dict}
    stock_len = len(result_dict)
    index_len = len(index_list)
    for count, symbol in enumerate(result_dict, start=1):
        if verbose:
            print("\rgenerate stock profit table {current} / {total}".format(
                current=count, total=stock_len), end='', flush=True)
        copy_df = stockdata_dict[symbol].data

        for i in range(index_len):
            result_dict[symbol][copy_df.index[i]] = (
                copy_df['close'] - copy_df['close'][copy_df.index[i]]) / copy_df['close'][copy_df.index[i]]
        # use example e.g result_dict['BTC']['previous_index']['current_index']
        result_dict[symbol] = result_dict[symbol]
    return result_dict


def backtest_execute(backtest: Backtest, init_invest_money: float = 10000000.0, invest_rate: float = 0.01, minimum_buy_count: float = 5.0, verbose: bool = False, save_strategy_result: bool = False, save_raw_csv_file: str = '', weight_score_function=_basic_weight_score_function):
    standardize_stock(stockdata_list=backtest.stockdata_list)
    base_index = backtest.stockdata_list[0].data.index

    # init dict
    strategy_result_dict = {
        stockdata.symbol: None for stockdata in backtest.stockdata_list}
    stockdata_dict = {
        stockdata.symbol: stockdata for stockdata in backtest.stockdata_list}
    stock_bucket_dict = {
        stockdata.symbol: pd.DataFrame(columns=['bucket', 'invest_money'], index=base_index).fillna(0) for stockdata in backtest.stockdata_list}
    stock_temp_dict = dict()
    # stock_profit_hash_table = _calc_stock_profit_hash_table(
    #    index_list=base_index, stockdata_dict=stockdata_dict, verbose=verbose)

    # divide pre, post strategy list
    pre_strategy_list = [
        strategy for strategy in backtest.strategy_list if not strategy.after]
    # TODO: future : to apply post_strategy_list
    # post_strategy_list = [
    #     strategy for strategy in backtest.strategy_list if strategy.after]

    # init backtest_result
    backtest_result_raw = pd.DataFrame(
        index=base_index, columns=['current_money', 'stock_bucket', 'total_potential_earn', 'total_potential_profit'])
    backtest_result_raw['current_money'] = 0.0
    backtest_result_raw['stock_bucket'] = np.nan
    backtest_result_raw['stock_bucket'].astype('object')
    backtest_result_raw.at[backtest_result_raw.index[0],
                           'stock_bucket'] = 'DUMMY'
    backtest_result_raw['total_potential_earn'] = 0.0
    backtest_result_raw['total_potential_profit'] = 0.0

    # loop base_index
    total_bucket_cnt = 0
    index_len = len(base_index)
    visited_index = []
    if save_raw_csv_file:
        debug_raw_dict = {
            "symbol": [],
            "buy_date": [],
            "buy_price": [],
            "sell_date": [],
            "sell_price": [],
            "sell_count": [],
        }
    current_invest_money = init_invest_money
    buy_money = init_invest_money * invest_rate
    for num, index in enumerate(base_index, start=1):
        visited_index.append(index)
        if verbose:
            print(
                'calc backtest {current} / {total}'.format(current=num, total=index_len))
        pick_stockdata_list = []
        if backtest.selector_result is None:
            pick_stockdata_list = backtest.stockdata_list
        else:
            selector_result_df = backtest.selector_result.value
            stockdata_symbol_list_df = selector_result_df.apply(
                lambda row: row.index[row == SelectorResultColumnType.SELECT].tolist(), axis=1)
            stockdata_symbol_list = stockdata_symbol_list_df[index]
            pick_stockdata_list = [
                stockdata for stockdata in backtest.stockdata_list if stockdata.symbol in stockdata_symbol_list]

        total_potential_earn = 0.0
        for stockdata in pick_stockdata_list:
            # if not calc pre_strategy_result calc it.
            if not isinstance(strategy_result_dict[stockdata.symbol], StrategyResult):
                response = strategy_execute(
                    strategy_list=pre_strategy_list, stockdata=stockdata, save_strategy_result=save_strategy_result, weight_score_function=weight_score_function)
                if isinstance(response, ResponseSuccess):
                    strategy_result_dict[stockdata.symbol] = response.value
                else:
                    return ResponseFailure(ResponseTypes.SYSTEM_ERROR, "[ERROR] strategy_execute")
            strategy_result_of_day, weight_score = strategy_result_dict[
                stockdata.symbol].value[index]
            # if minimum buy count less than weight_score don't buy
            if weight_score < minimum_buy_count and strategy_result_of_day == StrategyResultColumnType.BUY:
                strategy_result_dict[stockdata.symbol].value[index] = (
                    StrategyResultColumnType.KEEP, weight_score)
                strategy_result_of_day = StrategyResultColumnType.KEEP

            if strategy_result_of_day == StrategyResultColumnType.BUY:
                buy_score = math.ceil(weight_score)
                # check is avaliable to buy stock
                buy_total_money = buy_score * buy_money
                if buy_total_money <= current_invest_money:
                    # buy stock
                    if stockdata.symbol not in stock_temp_dict:
                        stock_temp_dict[stockdata.symbol] = buy_score
                    else:
                        stock_temp_dict[stockdata.symbol] += buy_score
                    stock_bucket_dict[stockdata.symbol].at[index,
                                                           'bucket'] += buy_score
                    stock_bucket_dict[stockdata.symbol].at[index,
                                                           'invest_money'] = buy_money
                    # buy stock end
                    total_bucket_cnt += buy_score
                    current_invest_money -= buy_total_money

        # calc total profit, potential profit
        for symbol in stock_temp_dict:
            symbol_earn = 0.0
            stock_bucket_df = stock_bucket_dict[symbol]
            buy_df = stock_bucket_df.loc[stock_bucket_df['bucket'] > 0]
            buy_list = list(buy_df.index)
            buy_list.sort(
                key=lambda profit_index: stockdata_dict[symbol].data.at[index, 'close'] - stockdata_dict[symbol].data.at[profit_index, 'close'])
            for profit_index in buy_list:
                bucket_cnt = stock_bucket_df.at[profit_index, 'bucket']
                profit_money = stock_bucket_df.at[profit_index, 'invest_money'] * (stockdata_dict[symbol].data.at[index, 'close'] /
                                                                                   stockdata_dict[symbol].data.at[profit_index, 'close'])
                symbol_earn += bucket_cnt * profit_money

            total_potential_earn += symbol_earn
            strategy_result_of_day, weight_score = strategy_result_dict[symbol].value[index]
            # sell strategy execute
            if strategy_result_of_day == StrategyResultColumnType.SELL:
                # cacluate sell_rate
                total_stock_length = buy_df['bucket'].sum()
                sell_cnt = math.ceil(
                    (weight_score / 100) * total_stock_length)
                if sell_cnt >= total_stock_length:
                    sell_cnt = total_stock_length
                sell_earn = 0.0
                tmp_sell_cnt = sell_cnt
                while buy_list != []:
                    if sell_cnt == 0:
                        break
                    profit_index = buy_list.pop()
                    bucket_cnt = stock_bucket_df.at[profit_index, 'bucket']
                    profit_money = stock_bucket_df.at[profit_index, 'invest_money'] * (stockdata_dict[symbol].data.at[index, 'close'] /
                                                                                       stockdata_dict[symbol].data.at[profit_index, 'close'])
                    if bucket_cnt < sell_cnt:
                        if save_raw_csv_file:
                            debug_raw_dict['symbol'].append(stockdata.symbol)
                            debug_raw_dict['buy_date'].append(profit_index)
                            debug_raw_dict['buy_price'].append(
                                stockdata_dict[symbol].data.at[profit_index, 'close'])
                            debug_raw_dict['sell_date'].append(index)
                            debug_raw_dict['sell_price'].append(
                                stockdata_dict[symbol].data.at[index, 'close'])
                            debug_raw_dict['sell_count'].append(bucket_cnt)
                        sell_earn += bucket_cnt * profit_money
                        sell_cnt -= bucket_cnt
                        stock_bucket_df.at[profit_index, 'bucket'] = 0
                        bucket_cnt = 0
                    else:  # bucket_cnt >= sell_cnt
                        if save_raw_csv_file:
                            debug_raw_dict['symbol'].append(stockdata.symbol)
                            debug_raw_dict['buy_date'].append(profit_index)
                            debug_raw_dict['buy_price'].append(
                                stockdata_dict[symbol].data.at[profit_index, 'close'])
                            debug_raw_dict['sell_date'].append(index)
                            debug_raw_dict['sell_price'].append(
                                stockdata_dict[symbol].data.at[index, 'close'])
                            debug_raw_dict['sell_count'].append(sell_cnt)
                        sell_earn += sell_cnt * profit_money
                        stock_bucket_df.at[profit_index, 'bucket'] -= sell_cnt
                        sell_cnt = 0
                total_bucket_cnt -= tmp_sell_cnt
                current_invest_money += sell_earn

        # calculates total potiential profit and current currencies in current date
        backtest_result_raw.at[index,
                               'total_potential_earn'] = total_potential_earn + current_invest_money
        backtest_result_raw.at[index,
                               'current_money'] = current_invest_money

        # TODO: post_strategy_execute
        backtest_result_raw.at[index, 'stock_bucket'] = _calc_stock_count(
            stock_bucket_dict)

        if index == base_index[-2]:
            if verbose:
                print('today coin list')
                for today_symbol in pick_stockdata_list:
                    print('{symbol_name} : {strategy_result}'.format(
                        symbol_name=today_symbol.symbol, strategy_result=strategy_result_dict[today_symbol.symbol].value[index]))
        # calculate profit and bucket
        # post strategy execute

    backtest_result_raw['total_potential_profit'] = (
        backtest_result_raw['total_potential_earn'] - init_invest_money) / init_invest_money

    if save_raw_csv_file:
        sell_df = pd.DataFrame(debug_raw_dict)
        sell_df.to_csv(save_raw_csv_file)
    return ResponseSuccess(BacktestResult(value=backtest_result_raw))
    # except Exception as e:
    #    exc_type, exc_obj, exc_tb = sys.exc_info()
    #    fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
    #    print(exc_type, fname, exc_tb.tb_lineno)
    #    return ResponseFailure(ResponseTypes.SYSTEM_ERROR, e)
