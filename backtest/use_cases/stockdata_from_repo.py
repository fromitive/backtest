from backtest.request.stockdata_from_repo import StockDataFromRepoValidRequest, StockDataFromRepoInvalidRequest
from backtest.response import (
    ResponseFailure,
    ResponseSuccess,
    ResponseTypes,
    build_response_from_invalid_request
)


def stockdata_from_repo(repo, request):
    if not request and not isinstance(request, StockDataFromRepoValidRequest):
        invalid_request = StockDataFromRepoInvalidRequest()
        invalid_request.add_error(
            'param_error', 'this request not supported..')
        return build_response_from_invalid_request(invalid_request)
    try:
        stockdata = repo.get(filters=request.filters)
        return ResponseSuccess(stockdata)
    except Exception as e:
        return ResponseFailure(ResponseTypes.SYSTEM_ERROR, e)
