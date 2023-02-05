from backtest.domains.backtest import Backtest
from backtest.domains.backtest_result import BacktestResult
from backtest.response import ResponseFailure, ResponseSuccess, ResponseTypes


def backtest_execute(backtest: Backtest):
    try:
        return ResponseSuccess(BacktestResult())
    except Exception as e:
        return ResponseFailure(ResponseTypes.SYSTEM_ERROR, e)
