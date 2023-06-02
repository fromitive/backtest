import pandas as pd
import pytest

from backtest.domains.backtest_result import BacktestResult


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
    ], }


@pytest.fixture(scope='function')
def dict_backtest_result_dataframe(dict_backtest_result):
    df = pd.DataFrame(dict_backtest_result,
                      columns=['stock_bucket', 'total_profit', 'date'])
    df.set_index('date', inplace=True)
    df.index = pd.to_datetime(df.index)
    df.index = df.index.strftime('%Y-%m-%d %H:%M:%S')
    return df


def test_init_backtest_result_without_parameters():
    backtest_result = BacktestResult()
    assert isinstance(backtest_result.value, pd.DataFrame)
    # assert isinstance(backtest_result.value.index, pd.DatetimeIndex)
    # assert list(backtest_result.value.columns) == ['stock_bucket',
    #                                               'total_profit',
    #                                               'total_stock_count',
    #                                               'stock_count']


def test_init_backtest_result_from_dict(dict_backtest_result):
    backtest_result = BacktestResult.from_dict(dict_backtest_result)
    assert isinstance(backtest_result.value, pd.DataFrame)
    assert list(backtest_result.value.columns) == ['stock_bucket',
                                                   'total_profit',
                                                   'total_potential_profit',
                                                   'total_stock_count',
                                                   'stock_count']
