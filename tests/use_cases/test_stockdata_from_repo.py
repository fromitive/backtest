from unittest import mock

import pytest

from backtest.domains.stockdata import StockData
from backtest.request.stockdata_from_repo import \
    build_stock_data_from_repo_request
from backtest.use_cases.stockdata_from_repo import stockdata_from_repo


@pytest.fixture(scope='function')
def sample_empty_stockdata():
    return StockData()


def test_stockdata_from_repo(sample_empty_stockdata):
    repo = mock.Mock()
    repo.get.return_value = sample_empty_stockdata
    request = build_stock_data_from_repo_request()
    response = stockdata_from_repo(repo, request)
    repo.get.assert_called_with(filters=None)
    assert bool(response) is True
    assert response.value == sample_empty_stockdata


def test_stockdata_from_repo_with_invalid_request(sample_empty_stockdata):
    repo = mock.Mock()
    repo.get.return_value = sample_empty_stockdata
    request = None
    response = stockdata_from_repo(repo, request)
    assert bool(response) is False
    assert response.value['type'] == 'ParametersError'
