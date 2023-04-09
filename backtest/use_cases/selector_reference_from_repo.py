from backtest.response import (
    ResponseFailure,
    ResponseSuccess,
    ResponseTypes,
    build_response_from_invalid_request
)


def selector_reference_from_repo(repo, request):
    if not request:
        return build_response_from_invalid_request(request)
    try:
        selector_reference = repo.get(filters=request.filters)
        return ResponseSuccess(selector_reference)
    except Exception as e:
        return ResponseFailure(ResponseTypes.SYSTEM_ERROR, e)
