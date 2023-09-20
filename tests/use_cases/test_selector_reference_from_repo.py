from datetime import datetime
from unittest import mock

import pytest

from backtest.domains.selector_reference import SelectorReference
from backtest.module_compet.pandas import pd
from backtest.request.selector_reference_from_repo import build_selector_reference_from_repo_request
from backtest.use_cases.selector_reference_from_repo import selector_reference_from_repo


@pytest.fixture(scope="function")
def sample_empty_selector_reference():
    return SelectorReference(symbol="TEST")


@pytest.fixture(scope="function")
def dict_selector_reference():
    return {"date": ["2022-10-11", "2022-11-11"], "dummy": [1.1, 2.2], "dummy2": ["11", "22"]}


@pytest.fixture(scope="function")
def dataframe_reference(dict_selector_reference):
    return pd.DataFrame(dict_selector_reference)


def test_selector_reference_from_repo(sample_empty_selector_reference):
    repo = mock.Mock(spec_set=["__name__", "get"])
    repo.get.return_value = sample_empty_selector_reference
    request = build_selector_reference_from_repo_request()
    response = selector_reference_from_repo(repo, request)
    repo.get.assert_called_with(filters=None)
    assert bool(response) is True
    assert response.value == sample_empty_selector_reference


@mock.patch("os.makedirs")
@mock.patch("pandas.read_csv")
@mock.patch("pandas.DataFrame.to_csv")
def test_selector_reference_from_repo_with_cache_enable_if_not_exist_cache(
    to_csv_result, read_csv_result, os_makedirs, sample_empty_selector_reference, dataframe_reference
):
    os_makedirs.return_value = None
    to_csv_result.return_value = None
    read_csv_result.side_effect = FileNotFoundError
    SELECTOR_REFERENCE_CSVREPO_DIR_PATH = "backtest/csvrepo/selector_reference"
    str_today = datetime.strftime(datetime.now(), "%Y-%m-%d")
    repo = mock.Mock(spec_set=["__name__", "get"])
    repo.get.return_value = sample_empty_selector_reference
    request = build_selector_reference_from_repo_request(filters={"symbol__eq": "ETH", "from__eq": "2019-09-01"})
    response = selector_reference_from_repo(repo, request, cache=True)

    repo.get.assert_called_with(filters={"symbol__eq": "ETH", "from__eq": "2019-09-01"})
    to_csv_result.assert_called_with(
        "{}/{}_ETH_2019-09-01_{}.csv".format(SELECTOR_REFERENCE_CSVREPO_DIR_PATH, type(repo).__name__, str_today)
    )
    os_makedirs.assert_called_with(SELECTOR_REFERENCE_CSVREPO_DIR_PATH, exist_ok=True)

    assert bool(response) is True
    assert isinstance(response.value, SelectorReference)


@mock.patch("os.makedirs")
@mock.patch("pandas.read_csv")
@mock.patch("pandas.DataFrame.to_csv")
def test_selector_reference_from_repo_with_cache_enable_if_exist_cache(
    to_csv_result, read_csv_result, os_makedirs, sample_empty_selector_reference, dataframe_reference
):
    os_makedirs.return_value = None
    to_csv_result.return_value = None
    read_csv_result.return_value = dataframe_reference
    SELECTOR_REFERENCE_CSVREPO_DIR_PATH = "backtest/csvrepo/selector_reference"
    str_today = datetime.strftime(datetime.now(), "%Y-%m-%d")
    repo = mock.Mock(spec_set=["__name__", "get"])
    repo.get.return_value = sample_empty_selector_reference
    request = build_selector_reference_from_repo_request(filters={"symbol__eq": "ETH", "from__eq": "2019-09-01"})
    response = selector_reference_from_repo(repo, request, cache=True)

    read_csv_result.assert_called_with(
        "{}/{}_ETH_2019-09-01_{}.csv".format(SELECTOR_REFERENCE_CSVREPO_DIR_PATH, type(repo).__name__, str_today)
    )

    assert bool(response) is True
    assert isinstance(response.value, SelectorReference)


def test_selector_reference_from_invalied_request_repo(sample_empty_selector_reference):
    repo = mock.Mock(spec_set=["__name__", "get"])
    repo.get.return_value = sample_empty_selector_reference
    request = None
    response = selector_reference_from_repo(repo, request=request)
    assert bool(response) is False
    assert response.value["type"] == "ParametersError"
