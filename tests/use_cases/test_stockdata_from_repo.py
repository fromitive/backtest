import pytest
from unittest import mock
from backtest.domains.stockdata import StockData
from backtest.use_cases.stockdata_from_repo import stockdata_from_repo
from backtest.request.stockdata_from_repo import build_stock_data_from_repo_request


@pytest.fixture(scope='function')
def sample_empty_stockdata():
    return StockData()


def test_stockdata_from_repo(sample_empty_stockdata):
    repo = mock.Mock()
    repo.get.return_value = sample_empty_stockdata
    request = build_stock_data_from_repo_request()
    response = stockdata_from_repo(repo, request)
    repo.get.assert_called_with(filters=None)
    assert bool(response) == True
    assert response.value == sample_empty_stockdata
