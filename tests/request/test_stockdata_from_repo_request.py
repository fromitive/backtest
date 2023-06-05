import pytest
from backtest.request.stockdata_from_repo import build_stock_data_from_repo_request


def test_build_stock_data_from_repo_request_without_filters():
    request = build_stock_data_from_repo_request()
    assert request.filters is None
    assert bool(request) is True


def test_build_stock_data_from_repo_request_with_empty_filters():
    request = build_stock_data_from_repo_request({})

    assert request.filters == {}
    assert bool(request) is True


def test_build_stock_data_from_repo_request_with_invalid_filters_parameter():
    request = build_stock_data_from_repo_request(filters=5)

    assert request.has_errors()
    assert request.errors[0]["parameter"] == "filters"
    assert bool(request) is False


def test_build_stock_data_from_repo_request_with_incorrect_filter_keys():
    request = build_stock_data_from_repo_request(filters={"a": 1})

    assert request.has_errors()
    assert request.errors[0]["parameter"] == "filters"
    assert bool(request) is False


@pytest.mark.parametrize("payment", ['KRW', 'USDT'])
@pytest.mark.parametrize("interval", ['24h', '30m'])
def test_build_stock_data_from_repo_request_accepted_filters(interval, payment):
    filters = {'order__eq': 'BTC', 'payment__eq': payment,
               'chart_interval__eq': interval, 'from__eq': '1990-01-01', 'to__eq': '1990-01-01', 'start_time__eq': '06:00', 'end_time__eq': '23:30'}
    request = build_stock_data_from_repo_request(filters=filters)
    assert request.filters == filters
    assert bool(request) is True


@pytest.mark.parametrize("payment", ['FAILED', 'TEST'])
def test_build_stock_data_from_repo_request_rejected_filters_with_wrong_payment(payment):
    filters = {'order__eq': 'BTC', 'payment__eq': payment,
               'chart_interval__eq': '24h'}
    request = build_stock_data_from_repo_request(filters=filters)
    assert request.has_errors()
    assert request.errors[0]["parameter"] == "filters"
    assert bool(request) is False


@pytest.mark.parametrize("chart_interval", ['invalied_internval'])
def test_build_stock_data_from_repo_request_rejected_filters_with_wrong_payment(chart_interval):
    filters = {'order__eq': 'BTC', 'payment__eq': ['KRW'],
               'chart_interval__eq': chart_interval}
    request = build_stock_data_from_repo_request(filters=filters)
    assert request.has_errors()
    assert request.errors[0]["parameter"] == "filters"
    assert bool(request) is False


@pytest.mark.parametrize("key", ["code__lt", "code__gt"])
def test_build_stock_data_from_repo_request_rejected_filters(key):
    filters = {key: 1}
    request = build_stock_data_from_repo_request(filters=filters)
    assert request.has_errors()
    assert request.errors[0]["parameter"] == "filters"
    assert bool(request) is False


@pytest.mark.parametrize("invalid_date", ['9999-99-99', 'invalied_object', '11-22-33'])
def test_build_stock_data_from_repo_request_rejected_filter_with_invalied_date(invalid_date):
    filters = {'chart_interval__eq': '24h',
               'from__eq': invalid_date, 'to__eq': invalid_date}
    request = build_stock_data_from_repo_request(filters=filters)
    assert request.has_errors()
    assert request.errors[0]["parameter"] == "filters"
    for error in request.errors:
        assert 'YYYY-MM-DD' in error['message']
    assert bool(request) is False


@pytest.mark.parametrize("future_date", ['9999-12-31'])
def test_build_stock_data_from_repo_request_accepted_filter_with_valied_future_date(future_date):
    filters = {'chart_interval__eq': '24h',
               'to__eq': future_date}
    request = build_stock_data_from_repo_request(filters=filters)
    assert request.filters == filters
    assert bool(request) is True


@pytest.mark.parametrize("future_date", ['9999-12-31'])
def test_build_stock_data_from_repo_request_rejected_filter_with_valied_future_date(future_date):
    filters = {'chart_interval__eq': '24h',
               'from__eq': future_date}
    request = build_stock_data_from_repo_request(filters=filters)
    assert request.has_errors()
    assert request.errors[0]["parameter"] == "filters"
    for error in request.errors:
        assert 'future' in error['message']
    assert bool(request) is False
