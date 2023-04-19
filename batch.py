import requests

from backtest.util.selector_reference_util import get_coingecko_symbol_id


def batch_bithumb_symbol_to_geckco_id():
    URL_BITUMB_SYMBOL_API = 'https://api.bithumb.com/public/ticker/ALL_KRW'
    res = requests.get(URL_BITUMB_SYMBOL_API).json().get('data')

    symbol_list = list(res.keys())[:-1]
    symbol_list_len = len(symbol_list)

    for idx, symbol in enumerate(symbol_list):
        print("[+] GET bithumb symbol id : {} {}/{}".format(symbol,
              idx, symbol_list_len))
        get_coingecko_symbol_id(symbol)


def batch_upbit_symbol_to_geckco_id():
    URL_UPBIT_SYMBOL_API = 'https://api.upbit.com/v1/market/all'
    res = requests.get(URL_UPBIT_SYMBOL_API).json()
    symbol_list = [symbol['market'][4:]
                   for symbol in res if symbol['market'].split('-')[0] == 'KRW']
    symbol_list_len = len(symbol_list)

    for idx, symbol in enumerate(symbol_list):
        print("[+] GET upbit symbol id : {} {}/{}".format(symbol,
              idx, symbol_list_len))
        get_coingecko_symbol_id(symbol)


batch_upbit_symbol_to_geckco_id()
