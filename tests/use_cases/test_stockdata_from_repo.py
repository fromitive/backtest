from datetime import datetime
from unittest import mock

import pytest

from backtest.domains.stockdata import StockData
from backtest.module_compet.pandas import pd
from backtest.request.stockdata_from_repo import build_stock_data_from_repo_request
from backtest.use_cases.stockdata_from_repo import stockdata_from_repo


@pytest.fixture(scope="function")
def sample_empty_stockdata_df():
    return pd.DataFrame(columns=["date", "open", "high", "low", "close", "volume"], index=pd.DatetimeIndex([]))


@pytest.fixture(scope="function")
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


@mock.patch("os.makedirs")
@mock.patch("pandas.read_csv")
@mock.patch("pandas.DataFrame.to_csv")
def test_stockdata_from_repo_with_cache_enable_if_exist(
    to_csv_result, read_csv_result, os_makedirs, sample_empty_stockdata_df
):
    os_makedirs.return_value = None
    to_csv_result.return_value = None
    read_csv_result.return_value = sample_empty_stockdata_df
    STOCKDATA_CSVREPO_DIR_PATH = "backtest/csvrepo/stockdata"
    str_today = datetime.strftime(datetime.now(), "%Y-%m-%d")
    repo = mock.Mock(spec_set=["__name__", "get"])
    repo.get.return_value = sample_empty_stockdata_df
    request = build_stock_data_from_repo_request(filters={"order__eq": "ETH", "from__eq": "2019-09-01"})
    response = stockdata_from_repo(repo, request, cache=True)

    read_csv_result.assert_called_with(
        "{}/{}_ETH_2019-09-01_{}_1d.csv".format(STOCKDATA_CSVREPO_DIR_PATH, type(repo).__name__, str_today)
    )

    assert bool(response) is True
    assert isinstance(response.value, StockData)


def test_stockdata_from_repo_with_invalid_request(sample_empty_stockdata):
    repo = mock.Mock()
    repo.get.return_value = sample_empty_stockdata
    request = None
    response = stockdata_from_repo(repo, request)
    assert bool(response) is False
    assert response.value["type"] == "ParametersError"
