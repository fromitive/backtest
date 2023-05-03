import statistics
from itertools import product
from typing import List

from backtest.domains.backtest import Backtest
from backtest.domains.stockdata import StockData
from backtest.domains.strategy import Strategy
from backtest.module_compet.pandas import pd
from backtest.response import ResponseSuccess
from backtest.use_cases.backtest_execute import backtest_execute


def grid_search_optimize_one_strategy_parameter(strategy_function, target_stockdata_list: List[StockData],
                                                param_grid: dict, inverse: bool = False, verbose: bool = False) -> pd.DataFrame:
    result_dict = {key: [] for key in param_grid.keys()}
    result_dict['mean_total_profit'] = []
    result_dict['mean_min_potential_profit'] = []
    stock_len = len(target_stockdata_list)
    keys, values = zip(*param_grid.items())
    param_combinations_list = [
        dict(zip(keys, v)) for v in product(*values)]
    combinations_len = len(param_combinations_list)
    for idx, param_combination in enumerate(param_combinations_list, start=1):
        total_profit_list = []
        total_min_potential_profit_list = []
        for stock_idx, stockdata in enumerate(target_stockdata_list, start=1):
            if verbose:
                print("\rtest[{idx} / {total_test_len}] progress {stock_idx} / {total}".format(
                    idx=idx, total_test_len=combinations_len, stock_idx=stock_idx, total=stock_len), end='', flush=True)
            strategy = Strategy(name=strategy_function.__name__,
                                function=strategy_function, weight=1, inverse=inverse, options=param_combination)
            backtest = Backtest(
                strategy_list=[strategy], stockdata_list=[stockdata])
            response = backtest_execute(backtest)

            if isinstance(response, ResponseSuccess):
                backtest_result = response
                backtest_df = backtest_result.value.value
                total_profit = backtest_df['total_profit'].expanding(
                ).sum().iloc[-1]
                potential_min_profit = backtest_df['total_potential_profit']
                total_profit_list.append(total_profit)
                total_min_potential_profit_list.append(potential_min_profit)
        mean_total_profit = statistics.mean(total_profit_list)
        mean_min_potential_profit = statistics.mean(
            total_min_potential_profit_list)
        if verbose:
            print('test[{idx}]: {function_name} parmater_combination : {param_combination} inverse : {inverse} mean_total_profit : {mean_total_profit} mean_min_potential_profit: {mean_min_potential_profit}'.format(
                idx=idx, function_name=strategy_function.__name__, param_combination=param_combination, inverse=inverse, mean_total_profit=mean_total_profit, mean_min_potential_profit=mean_min_potential_profit))
        for k, v in param_combination.items():
            result_dict[k].append(v)
        result_dict['mean_min_potential_profit'].append(
            mean_total_profit)
        result_dict['mean_total_profit'].append(mean_min_potential_profit)
    result_df = pd.DataFrame(result_dict)
    return result_df
