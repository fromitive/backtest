from backtest.use_cases.make_crypto_backtest import (get_bithumb_symbol,
                                                     get_upbit_symbol)
from backtest.util.selector_reference_util import get_coingecko_symbol_id


def batch_bithumb_symbol_to_geckco_id():
    symbol_list = get_bithumb_symbol()
    symbol_list_len = len(symbol_list)

    for idx, symbol in enumerate(symbol_list, start=1):
        print("[+] GET bithumb symbol id : {} {}/{}".format(symbol,
              idx, symbol_list_len))
        get_coingecko_symbol_id(symbol)


def batch_upbit_symbol_to_geckco_id():
    symbol_list = get_upbit_symbol()
    symbol_list_len = len(symbol_list)

    for idx, symbol in enumerate(symbol_list, start=1):
        print("[+] GET upbit symbol id : {} {}/{}".format(symbol,
              idx, symbol_list_len))
        get_coingecko_symbol_id(symbol)


batch_bithumb_symbol_to_geckco_id()
batch_upbit_symbol_to_geckco_id()
