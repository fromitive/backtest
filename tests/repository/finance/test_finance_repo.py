from unittest import mock
from pandas import Timestamp
from backtest.domains.stockdata import StockData
from backtest.repository.finance.finance_repo import FinanceRepo
import pandas as pd
import pytest



@pytest.fixture(scope='function')
def finance_data_dict():
    return {'Date': [Timestamp('2018-01-02 00:00:00'),
            Timestamp('2018-01-03 00:00:00'),
            Timestamp('2018-01-04 00:00:00'),
            Timestamp('2018-01-05 00:00:00'),
            Timestamp('2018-01-08 00:00:00')],
    'Open': [2683.72998, 2697.850098, 2719.310059, 2731.330078, 2742.669922],
    'High': [2695.889893, 2714.370117, 2729.290039, 2743.449951, 2748.51001],
    'Low': [2682.360107, 2697.77002, 2719.070068, 2727.919922, 2737.600098],
    'Close': [2695.810059, 2713.060059, 2723.98999, 2743.149902, 2747.709961],
    'Adj Close': [2695.810059, 2713.060059, 2723.98999, 2743.149902, 2747.709961],
    'Volume': [3397430000, 3544030000, 3697340000, 3239280000, 3246160000]}


@pytest.fixture(scope='function')
def finance_dataframe(finance_data_dict):
    finance_data_dict
    df = pd.DataFrame(finance_data_dict)
    df.set_index(df['Date']).drop(['Date'], axis=1)
    return df


@mock.patch('FinanceDataReader.DataReader')
def test_finance_repo(mock_fdr_result,finance_dataframe):
    mock_fdr_result.return_value = finance_dataframe
    finance_repo = FinanceRepo()
    response = finance_repo.get(filters={})
    assert isinstance(response, StockData)
    #assert isinstance(response.data.index, pd.DatetimeIndex)
    assert list(response.data.columns) == [
        'open', 'high', 'low', 'close', 'volume']
