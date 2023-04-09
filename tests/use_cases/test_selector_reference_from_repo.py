import pytest
from unittest import mock
from backtest.domains.selector_reference import SelectorReference
from backtest.request.selector_reference_from_repo import build_selector_reference_from_repo_request
from backtest.use_cases.selector_reference_from_repo import selector_reference_from_repo
from backtest.response import ResponseFailure


@pytest.fixture(scope='function')
def sample_empty_selector_reference():
    return SelectorReference()


def test_stockdata_from_repo(sample_empty_selector_reference):
    repo = mock.Mock()
    repo.get.return_value = sample_empty_selector_reference
    request = build_selector_reference_from_repo_request()
    response = selector_reference_from_repo(repo, request)
    repo.get.assert_called_with(filters=None)
    assert bool(response) == True
    assert response.value == sample_empty_selector_reference


def test_stockdata_from_invalied_request_repo(sample_empty_selector_reference):
    repo = mock.Mock()
    repo.get.return_value = sample_empty_selector_reference
    request = None
    response = selector_reference_from_repo(repo, request=request)
    assert bool(response) == False
    assert response.value['type'] == 'ParametersError'
