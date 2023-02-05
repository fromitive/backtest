from backtest.domains.backtest import Backtest
from backtest.domains.backtest_result import BacktestResult
from backtest.domains.strategy import Strategy
from backtest.domains.strategy import StockData
from backtest.domains.strategy_result import StrategyResultColumnType
from backtest.use_cases.strategy_execute import strategy_execute
from backtest.response import ResponseFailure, ResponseSuccess, ResponseTypes

import pandas as pd


def backtest_execute(backtest: Backtest):
    try:
        for stockdata in backtest.stockdata_list:
            strategy_total_result = pd.DataFrame(index=pd.DatetimeIndex([]))
            for strategy in backtest.strategy_list:
                response = strategy_execute(strategy=strategy)
                if isinstance(response, ResponseFailure):
                    raise Exception('strategy_response error!')
                else:
                    strategy_result = response.value
                    if strategy_result.target == stockdata.symbol or strategy_result.target == 'ALL':
                        strategy_total_result = strategy_total_result.join(
                            strategy_result.value, how='outer')
            for column in strategy_total_result.columns:
                strategy_total_result[column] = strategy_total_result[column].fillna(
                    {i: (StrategyResultColumnType.KEEP, 0) for i in strategy_total_result.index})

        return ResponseSuccess(BacktestResult())
    except Exception as e:
        return ResponseFailure(ResponseTypes.SYSTEM_ERROR, e)
