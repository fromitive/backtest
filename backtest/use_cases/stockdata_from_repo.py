import os
from datetime import datetime

from backtest.domains.stockdata import StockData
from backtest.request.stockdata_from_repo import (
    StockDataFromRepoInvalidRequest, StockDataFromRepoValidRequest)
from backtest.response import (ResponseFailure, ResponseSuccess, ResponseTypes,
                               build_response_from_invalid_request)

STOCKDATA_CSV_REPO_PATH = "backtest/csvrepo/stockdata/{repo_name}_{order}_{from_date}_{to_date}_{chart_interval}.csv"
STOCKDATA_CSV_REPO_DIR_PATH = "backtest/csvrepo/stockdata"


def stockdata_from_repo(repo, request, cache=False):
    str_today = datetime.strftime(datetime.now(), "%Y-%m-%d")
    repo_name = type(repo).__name__
    order = 'NOSYM'
    from_date = '1999-01-01'
    to_date = str_today

    if not request and not isinstance(request, StockDataFromRepoValidRequest):
        invalid_request = StockDataFromRepoInvalidRequest()
        invalid_request.add_error(
            'param_error', 'this request not supported..')
        return build_response_from_invalid_request(invalid_request)
    elif request.filters:
        order = request.filters['order__eq'] if 'order__eq' in request.filters else 'NOSYM'
        from_date = request.filters['from__eq'] if 'from__eq' in request.filters else '1990-01-01'
        to_date = request.filters['to__eq'] if 'to__eq' in request.filters else str_today
        chart_interval = request.filters['chart_interval__eq'] if 'to__eq' in request.filters else '1d'

    CSV_PATH = STOCKDATA_CSV_REPO_PATH.format(
        repo_name=repo_name, order=order, from_date=from_date, to_date=to_date, chart_interval=chart_interval)

    if cache:
        try:
            stockdata = StockData.from_csv(CSV_PATH, symbol=order)
            return ResponseSuccess(stockdata)
        except FileNotFoundError:
            pass
    try:
        stockdata = repo.get(filters=request.filters)
        if cache:
            os.makedirs(STOCKDATA_CSV_REPO_DIR_PATH, exist_ok=True)
            stockdata.to_csv(CSV_PATH)
        return ResponseSuccess(stockdata)
    except Exception as e:
        return ResponseFailure(ResponseTypes.SYSTEM_ERROR, e)
