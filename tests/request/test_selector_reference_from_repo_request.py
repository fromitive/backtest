import pytest
from backtest.request.selector_reference_from_repo import build_selector_reference_from_repo_request


def test_build_selector_reference_from_repo_request_without_filters():
    request = build_selector_reference_from_repo_request()
    assert request.filters is None
    assert bool(request) is True


def test_build_selector_reference_from_repo_request_with_empty_filters():
    request = build_selector_reference_from_repo_request({})

    assert request.filters == {}
    assert bool(request) is True


def test_build_selector_reference_from_repo_request_with_invalid_filters_parameter():
    request = build_selector_reference_from_repo_request(filters=5)
    assert request.has_errors()
    assert request.errors[0]["parameter"] == "filters"
    assert bool(request) is False


def test_build_selector_reference_from_repo_request_with_incorrect_filter_keys():
    request = build_selector_reference_from_repo_request(filters={"a": 1})

    assert request.has_errors()
    assert request.errors[0]["parameter"] == "filters"
    assert bool(request) is False


@pytest.mark.parametrize("symbol__eq", ["BTC", "ETH"])
@pytest.mark.parametrize("from__eq", ["1990-01-01", "2010-01-20"])
def test_build_selector_reference_from_repo_request_accepted_filters(symbol__eq, from__eq):
    filters = {"symbol__eq": symbol__eq, "from__eq": from__eq, "to__eq": "2022-11-01"}
    request = build_selector_reference_from_repo_request(filters=filters)
    assert request.filters == filters
    assert bool(request) is True


@pytest.mark.parametrize("invalid_date", ["9999-99-99", "invalied_object", "11-22-33"])
def test_build_selector_reference_from_repo_request_rejected_filter_with_invalied_date(invalid_date):
    filters = {"from__eq": invalid_date, "to__eq": invalid_date}
    request = build_selector_reference_from_repo_request(filters=filters)
    assert request.has_errors()
    assert request.errors[0]["parameter"] == "filters"
    for error in request.errors:
        assert "YYYY-MM-DD" in error["message"]
    assert bool(request) is False


@pytest.mark.parametrize("future_date", ["9999-12-31"])
def test_build_stock_data_from_repo_request_accepted_filter_with_valied_future_date(future_date):
    filters = {"to__eq": future_date}
    request = build_selector_reference_from_repo_request(filters=filters)
    assert request.filters == filters
    assert bool(request) is True


@pytest.mark.parametrize("future_date", ["9999-12-31"])
def test_build_stock_data_from_repo_request_rejected_filter_with_valied_future_date(future_date):
    filters = {"from__eq": future_date}
    request = build_selector_reference_from_repo_request(filters=filters)
    assert request.has_errors()
    assert request.errors[0]["parameter"] == "filters"
    for error in request.errors:
        assert "future" in error["message"]
    assert bool(request) is False
