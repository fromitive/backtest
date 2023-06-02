from unittest import mock

import pytest

from backtest.domains.selector_reference import SelectorReference
from backtest.module_compet.pandas import pd
from backtest.repository.webrepo.crypto.coingecko_repo import CoinGeckoRepo


@pytest.fixture(scope='function')
def mocked_requests_get(*args, **kwargs):
    class MockResponse:
        def __init__(self, json_data, status_code):
            self.json_data = json_data
            self.status_code = status_code

        def json(self):
            return self.json_data
    return MockResponse({"stats": [[1393459200000, 33946082.0],
                                   [1393545600000, 129181721.0],
                                   [1393632000000, 228225596.0],
                                   [1393718400000, 220007969.0],
                                   [1393804800000, 393424191.0],
                                   [1393891200000, 1026360933.0],
                                   [1393977600000, 455813514.0],
                                   [1394064000000, 208246241.0],
                                   [1394150400000, 209880515.0],
                                   [1394236800000, 212860847.0],
                                   [1394323200000, 269369988.0],
                                   [1394409600000, 304541792.0],
                                   [1394496000000, 291245544.0],
                                   [1394582400000, 291662324.0],
                                   [1394668800000, 266638660.0],
                                   [1394755200000, 233243247.0],
                                   [1394841600000, 218753323.0],
                                   [1394928000000, 160773510.0],
                                   [1395014400000, 94746422.0],
                                   [1395100800000, 110162461.0],
                                   [1395187200000, 168242035.0],
                                   [1395273600000, 157810478.0],
                                   [1395360000000, 154289177.0],
                                   [1395446400000, 155495515.0],
                                   [1395532800000, 153115018.0],
                                   [1395619200000, 120412325.0]]}, 200)


@mock.patch('requests.get')
def test_coingecko_repo_without_paramemters(mock_response_get, mocked_requests_get):
    mock_response_get.return_value = mocked_requests_get
    coingecko_repo = CoinGeckoRepo()
    response = coingecko_repo.get(filters={})
    assert isinstance(response, SelectorReference)
    # assert isinstance(
    #    response.data.index, pd.DatetimeIndex)
    assert response.symbol == 'BTC'
    assert list(response.data.columns) == [
        'marketcap']
