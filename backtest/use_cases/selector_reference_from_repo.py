from backtest.request.selector_reference_from_repo import (
    SelectorReferenceFromRepoInvalidRequest,
    SelectorReferenceFromReponValidRequest)
from backtest.response import (ResponseFailure, ResponseSuccess, ResponseTypes,
                               build_response_from_invalid_request)

SELECTOR_REFERENCE_CSV_REPO_PATH = "backtest/csvrepo/selector_reference/{repo_name}_{symbol}_{from_date}_{to_date}.csv"
SELECTOR_REFERENCE_CSV_REPO_DIR_PATH = "backtest/csvrepo/selector_reference"

def selector_reference_from_repo(repo, request, cache=False):
    str_today = datetime.strftime(datetime.now(), "%Y-%m-%d")
    repo_name = type(repo).__name__
    symbol = 'NOSYM'
    from_date = '1999-01-01'
    to_date = str_today

    if not request and not isinstance(request, SelectorReferenceFromReponValidRequest):
        invalid_request = SelectorReferenceFromRepoInvalidRequest()
        invalid_request.add_error(
            'param_error', 'this request not supported..')
        return build_response_from_invalid_request(invalid_request)
    elif request.filters:
        symbol = request.filters['symbol__eq'] if 'symbol__eq' in request.filters else 'NOSYM'
        from_date = request.filters['from__eq'] if 'from__eq' in request.filters else '1990-01-01'
        to_date = request.filters['to__eq'] if 'to__eq' in request.filters else str_today
    CSV_PATH = SELECTOR_REFERENCE_CSV_REPO_PATH.format(
        repo_name=repo_name, symbol=symbol, from_date=from_date, to_date=to_date)
    if cache:
        try:
            selector_reference = SelectorReference.from_csv(CSV_PATH)
        except FileNotFoundError:
            pass
    try:
        selector_reference = repo.get(filters=request.filters)
        if cache:
            os.makedirs(SELECTOR_REFERENCE_CSV_REPO_DIR_PATH, exist_ok=True)
            selector_reference.to_csv(CSV_PATH)
        return ResponseSuccess(selector_reference)
    except Exception as e:
        return ResponseFailure(ResponseTypes.SYSTEM_ERROR, e)
