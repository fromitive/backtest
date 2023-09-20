from unittest import mock

import pytest

from backtest.domains.stockdata import StockData
from backtest.repository.webrepo.crypto.binance_repo import BinanceRepo


@pytest.fixture(scope="function")
def mocked_requests_get(*args, **kwargs):
    class MockResponse:
        def __init__(self, json_data, status_code):
            self.json_data = json_data
            self.status_code = status_code

        def json(self):
            return self.json_data

    return MockResponse(
        [
            [
                1673355600000,
                "17224.15000000",
                "17255.55000000",
                "17212.14000000",
                "17246.05000000",
                "8539.81370000",
                1673359199999,
                "147171813.20299680",
                229053,
                "4142.95566000",
                "71399389.49013960",
                "0",
            ]
        ],
        200,
    )


@mock.patch("requests.get")
def test_binance_repo_without_paramemters(mock_response_get, mocked_requests_get):
    mock_response_get.return_value = mocked_requests_get
    binance_repo = BinanceRepo()
    response = binance_repo.get(filters={})
    assert isinstance(response, StockData)
    # assert isinstance(response.data.index, pd.DatetimeIndex)
    assert list(response.data.columns) == ["open", "high", "low", "close", "volume"]
