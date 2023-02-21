from backtest.domains.backtest import Backtest
from backtest.domains.backtest_result import BacktestResult
from backtest.domains.strategy import Strategy
from backtest.domains.strategy import StockData
from backtest.domains.strategy_result import StrategyResultColumnType
from backtest.use_cases.strategy_execute import strategy_execute
from backtest.response import ResponseFailure, ResponseSuccess, ResponseTypes

import pandas as pd


def _sum_strategy(series: pd.Series):
    total_result = {StrategyResultColumnType.KEEP: 0,
                    StrategyResultColumnType.SELL: 0,
                    StrategyResultColumnType.BUY: 0}
    for idx in series.index:
        type, weight = series[idx]
        total_result[type] += weight
    print(total_result)
    return max(total_result, key=total_result.get)


strategy_total_result.apply(lambda row: _sum_strategy(row), axis=1)

def backtest_execute(backtest: Backtest):
    try:
        bucket=pd.DataFrame(index=pd.DatetimeIndex([]))
        for stockdata in backtest.stockdata_list:
            strategy_total_result = pd.DataFrame(index=pd.DatetimeIndex([]))
            for strategy in backtest.strategy_list:
                response = strategy_execute(strategy=strategy)
                if isinstance(response, ResponseFailure):
                    raise Exception('strategy_response error!')
                else:
                    strategy_result = response.value
                    if strategy_result.value.columns[0] not in strategy_total_result.columns:
                        if strategy_result.target == stockdata.symbol or strategy_result.target == 'ALL':
                            strategy_total_result = strategy_total_result.join(
                                strategy_result.value, how='outer')
            #fill na with 
            for column in strategy_total_result.columns:
                strategy_total_result[column] = strategy_total_result[column].fillna(
                    {i: (StrategyResultColumnType.KEEP, 0) for i in strategy_total_result.index})
                
            total_result=strategy_total_result.apply(lambda row: _sum_strategy(row), axis=1)
        
            

        return ResponseSuccess(BacktestResult())
    except Exception as e:
        return ResponseFailure(ResponseTypes.SYSTEM_ERROR, e)


@pytest.fixture(scope='function')
def dict_backtest_result():
    return {'stock_bucket': [
        [('ETH', '2022-10-30', 0.0), ('BTC', '2022-10-30', 0.0)],

        [('BTC', '2022-10-30', -1.0), ('ETH', '2022-10-30', 0.0),
         ('ETH', '2022-10-31', 0.0)],

        [('XRP', '2022-11-01', 0.5)],
        [('TEST', '2022-12-05', 10.0)]
    ],
        'date': ['2022-10-30',
                 1388070000000,
                 '2022-02-09',
                 '2022-04-07'],
        'total_profit': [
            1.0,
            1.0,
            1.1,
            -2.1
    ]}
