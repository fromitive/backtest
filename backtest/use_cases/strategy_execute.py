from backtest.domains.strategy import Strategy, StrategyType
from backtest.domains.strategy_result import StrategyResult, StrategyResultColumnType
from backtest.response import ResponseFailure, ResponseSuccess, ResponseTypes
import pandas as pd


def _basic_function(strategy: Strategy):
    response = StrategyResult(value=pd.DataFrame(
        index=strategy.data.index, columns=[strategy.name]))
    response.value[strategy.name] = [(
        StrategyResultColumnType.KEEP, strategy.weight)] * len(strategy)
    return response


def strategy_execute(strategy: Strategy, strategy_fucntion=_basic_function):
    try:
        if strategy.type == StrategyType.no_type:
            raise Exception('type not defined..')
        response = strategy_fucntion(strategy=strategy)
        return ResponseSuccess(response)
    except Exception as e:
        return ResponseFailure(ResponseTypes.SYSTEM_ERROR, e)
