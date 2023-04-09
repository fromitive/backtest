from backtest.request.selector_reference_from_repo import SelectorReferenceFromReponValidRequest
from backtest.request.selector_reference_from_repo import SelectorReferenceFromRepoInvalidRequest
from backtest.response import (
    ResponseFailure,
    ResponseSuccess,
    ResponseTypes,
    build_response_from_invalid_request
)


def selector_reference_from_repo(repo, request):
    if not request and not isinstance(request, SelectorReferenceFromReponValidRequest):
        invalid_request = SelectorReferenceFromRepoInvalidRequest()
        invalid_request.add_error(
            'param_error', 'this request not supported..')
        return build_response_from_invalid_request(invalid_request)
    try:
        selector_reference = repo.get(filters=request.filters)
        return ResponseSuccess(selector_reference)
    except Exception as e:
        return ResponseFailure(ResponseTypes.SYSTEM_ERROR, e)
