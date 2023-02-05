from backtest.response import (
    ResponseFailure,
    ResponseSuccess,
    ResponseTypes,
    build_response_from_invalid_request
)


def stockdata_from_repo(repo, request):
    if not request:
        return build_response_from_invalid_request(request)
    try:
        stockdata = repo.get(filters=request.filters)
        return ResponseSuccess(stockdata)
    except Exception as e:
        return ResponseFailure(ResponseTypes.SYSTEM_ERROR, e)
