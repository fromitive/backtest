import pytest
import pandas as pd
from backtest.repository.webrepo.crypto.upbit_repo import UpbitRepo
from backtest.domains.stockdata import StockData
from unittest import mock


@pytest.fixture(scope='function')
def mocked_requests_get(*args, **kwargs):
    class MockResponse:
        def __init__(self, json_data, status_code):
            self.json_data = json_data
            self.status_code = status_code

        def json(self):
            return self.json_data

    return MockResponse([
        {
            "market": "KRW-BTC",
            "candle_date_time_utc": "2018-04-18T00:00:00",
            "candle_date_time_kst": "2018-04-18T09:00:00",
            "opening_price": 8450000,
            "high_price": 8679000,
            "low_price": 8445000,
            "trade_price": 8626000,
            "timestamp": 1524046650532,
            "candle_acc_trade_price": 107184005903.68721,
            "candle_acc_trade_volume": 12505.93101659,
            "prev_closing_price": 8450000,
            "change_price": 176000,
            "change_rate": 0.0208284024
        }
    ], 200)


@mock.patch('requests.get')
def test_upbit_repo_without_paramemters(mock_response_get, mocked_requests_get):
    mock_response_get.return_value = mocked_requests_get
    upbit_repo = UpbitRepo()
    response = upbit_repo.get(filters={})
    assert isinstance(response, StockData)
    assert isinstance(response.data.index, pd.DatetimeIndex)
    assert list(response.data.columns) == [
        'open', 'high', 'low', 'close', 'volume']
