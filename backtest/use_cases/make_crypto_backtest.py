from datetime import datetime
from enum import Enum
from typing import List

import requests

from backtest.domains.selector_reference import SelectorReference
from backtest.domains.selector_result import (SelectorResult,
                                              SelectorResultColumnType)
from backtest.domains.stockdata import StockData
from backtest.module_compet.pandas import pd
from backtest.repository.webrepo.crypto.bithumb_repo import BithumbRepo
from backtest.repository.webrepo.crypto.coingecko_repo import CoinGeckoRepo
from backtest.repository.webrepo.crypto.upbit_repo import UpbitRepo
from backtest.request.selector_reference_from_repo import \
    build_selector_reference_from_repo_request
from backtest.request.stockdata_from_repo import \
    build_stock_data_from_repo_request
from backtest.response import ResponseFailure, ResponseSuccess, ResponseTypes
from backtest.use_cases.selector_reference_from_repo import \
    selector_reference_from_repo
from backtest.use_cases.standardize_selector_reference import \
    standardize_selector_reference
from backtest.use_cases.stockdata_from_repo import stockdata_from_repo

SELECTOR_REFERENCE_CSV_REPO_PATH = "backtest/csvrepo/selector_reference/{repo_name}_{symbol}_{from_date}_{to_date}.csv"
SELECTOR_REFERENCE_CSV_REPO_DIR_PATH = "backtest/csvrepo/selector_reference"

class MarketType(Enum):
    BITHUMB = 1
    UPBIT = 2
    BINANCE = 3


def get_bithumb_symbol():
    URL_BITUMB_SYMBOL_API = 'https://api.bithumb.com/public/ticker/ALL_KRW'
    res = requests.get(URL_BITUMB_SYMBOL_API).json().get('data')
    symbol_list = list(res.keys())[:-1]
    return symbol_list


def get_upbit_symbol():
    URL_UPBIT_SYMBOL_API = 'https://api.upbit.com/v1/market/all'
    res = requests.get(URL_UPBIT_SYMBOL_API).json()
    symbol_list = [symbol['market'][4:]
                   for symbol in res if symbol['market'].split('-')[0] == 'KRW']
    return symbol_list


def make_crypto_selector_reference_makretcap(market: MarketType, from_date: str = '', to_date: str = '', cache: bool = False) -> ResponseSuccess | ResponseFailure:
    str_today = datetime.strftime(datetime.now(), "%Y-%m-%d")
    str_symbol = ''
    if from_date == '':
        from_date = '1999-01-01'
    if to_date == '':
        to_date = str_today
    get_symbol_function = None
    if market == MarketType.BITHUMB:
        get_symbol_function = get_bithumb_symbol
        str_symbol = 'BITHUMB'
    elif market == MarketType.UPBIT:
        get_symbol_function = get_upbit_symbol
        str_symbol = 'UPBIT'
    else:
        return ResponseFailure(type_=ResponseTypes.SYSTEM_ERROR, message='market not supported value = {}'.format(market))

    CSV_PATH = SELECTOR_REFERENCE_CSV_REPO_PATH.format(
        repo_name='CoinGeckoRepo', symbol=str_symbol, from_date=from_date, to_date=to_date)

    if cache:
        try:
            selector_reference = SelectorReference.from_csv(
                CSV_PATH, symbol=str_symbol)
            return ResponseSuccess(selector_reference)
        except FileNotFoundError:
            pass

    symbol_list = get_symbol_function()
    # get selector_reference_list
    selector_reference_list = []
    for idx, symbol in enumerate(symbol_list, start=1):
        print('make selector reference {symbol} {idx}/{len_symbol_list}'.format(
            symbol=symbol, idx=idx, len_symbol_list=len(symbol_list)))
        request = build_selector_reference_from_repo_request(
            filters={'symbol__eq': symbol, 'from__eq': from_date, 'to__eq': to_date})
        response = selector_reference_from_repo(
            CoinGeckoRepo(), request=request, cache=cache)
        if isinstance(response, ResponseFailure):
            return ResponseFailure(type_=ResponseTypes.SYSTEM_ERROR, message='symbol : {} fail to load from coingecko repository'.format(symbol))
        else:  # ResponseSuccess
            selector_reference_list.append(response.value)
    standardize_selector_reference(
        selector_reference_list, to_date=to_date)
    selector_reference_data = pd.DataFrame(
        index=selector_reference_list[0].data.index, columns=symbol_list)
    for selector_reference in selector_reference_list:
        selector_reference_data[selector_reference.symbol] = selector_reference.data['marketcap']
    total_selector_reference = SelectorReference(
        symbol=str_symbol, data=selector_reference_data)
    if cache:
        total_selector_reference.to_csv(CSV_PATH)
    return ResponseSuccess(total_selector_reference)


def make_crypto_stockdata_list(market: MarketType, selector_result: SelectorResult, cache: bool = False) -> List[StockData] | ResponseFailure:
    from_date = datetime.strftime(selector_result.value.index[0], "%Y-%m-%d")
    to_date = datetime.strftime(selector_result.value.index[-1], "%Y-%m-%d")
    stockdata_list = []
    stockdata_repo = None
    if market == MarketType.BITHUMB:
        stockdata_repo = BithumbRepo
    elif market == MarketType.UPBIT:
        stockdata_repo = UpbitRepo
    else:
        return ResponseFailure(type_=ResponseTypes.SYSTEM_ERROR, message='market repo not supported value = {}'.format(market))

    selector_result_df = selector_result.value
    stockdata_symbol_list_df = selector_result_df.apply(
        lambda row: row.index[row == SelectorResultColumnType.SELECT].tolist(), axis=1)

    stockdata_symbol_bucket = set()
    for index in stockdata_symbol_list_df.index:
        stockdata_symbol_bucket.update(stockdata_symbol_list_df[index])
    symbol_len = len(stockdata_symbol_bucket)
    for idx, symbol in enumerate(stockdata_symbol_bucket, start=1):
        print('Get StockData[{symbol}] From {repo_name} {current}/{total}'.format(
            symbol=symbol, repo_name=stockdata_repo, current=idx, total=symbol_len))
        request = build_stock_data_from_repo_request(
            filters={'order__eq': symbol, 'from__eq': from_date, 'to__eq': to_date})
        response = stockdata_from_repo(
            stockdata_repo(), request=request, cache=cache)
        if isinstance(response, ResponseFailure):
            return ResponseFailure(ResponseTypes.SYSTEM_ERROR, "failed to load repository symbol value : {symbol} in {repo_name}".format(symbol=symbol, repo_name=stockdata_repo))
        else:
            stockdata_list.append(response.value)

    return stockdata_list
