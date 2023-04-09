import pytest
import pandas as pd
from backtest.repository.webrepo.crypto.bithumb_repo import BithumbRepo
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

    return MockResponse({'data': [
        [
            1388070000000,
            "737000",
            "755000",
            "755000",
            "737000",
            "3.78"
        ],
        [
            1388156400000,
            "750000",
            "750000",
            "750000",
            "750000",
            "12"
        ]]}, 200)


@mock.patch('requests.get')
def test_bithumb_repo_without_paramemters(mock_response_get, mocked_requests_get):
    mock_response_get.return_value = mocked_requests_get
    bithumb_repo = BithumbRepo()
    response = bithumb_repo.get(filters={})
    assert isinstance(response, StockData)
    assert isinstance(response.data.index, pd.DatetimeIndex)
    assert list(response.data.columns) == [
        'open', 'high', 'low', 'close', 'volume']
