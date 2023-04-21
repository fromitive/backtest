from datetime import datetime
from enum import Enum

import requests

from backtest.domains.selector_reference import SelectorReference
from backtest.module_compet.pandas import pd
from backtest.repository.webrepo.crypto.coingecko_repo import CoinGeckoRepo
from backtest.request.selector_reference_from_repo import \
    build_selector_reference_from_repo_request
from backtest.response import ResponseFailure, ResponseSuccess, ResponseTypes
from backtest.use_cases.selector_reference_from_repo import \
    selector_reference_from_repo
from backtest.use_cases.standardize_selector_reference import \
    standardize_selector_reference


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


def make_crypto_selector_reference_makretcap(market: MarketType, from_date: str = '', to_date: str = '') -> ResponseSuccess | ResponseFailure:
    str_today = datetime.strftime(datetime.now(), "%Y-%m-%d")
    if from_date == '':
        from_date = '1999-01-01'
    if to_date == '':
        to_date = str_today
    get_symbol_function = None
    if market == MarketType.BITHUMB:
        get_symbol_function = get_bithumb_symbol
    elif market == MarketType.UPBIT:
        get_symbol_function = get_upbit_symbol
    else:
        return ResponseFailure(type_=ResponseTypes.SYSTEM_ERROR, message='market not supported value = {}'.format(market))
    symbol_list = get_symbol_function()
    # get selector_reference_list
    selector_reference_list = []
    for symbol in symbol_list:
        request = build_selector_reference_from_repo_request(
            filters={'symbol__eq': symbol, 'from__eq': from_date, 'to__eq': to_date})
        response = selector_reference_from_repo(
            CoinGeckoRepo(), request=request, cache=True)
        if isinstance(response, ResponseFailure):
            return ResponseFailure(type_=ResponseTypes.SYSTEM_ERROR, message='symbol : {} fail to load from coingecko repository'.format(symbol))
        else:  # ResponseSuccess
            selector_reference_list.append(response.value)

    standardize_selector_reference(selector_reference_list)

    return ResponseSuccess(SelectorReference(symbol='BITHUMB', data=pd.DataFrame(columns=symbol_list)))
