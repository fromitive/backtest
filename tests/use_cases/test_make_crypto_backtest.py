from datetime import datetime
from unittest import mock

import pytest

from backtest.domains.selector_reference import SelectorReference
from backtest.domains.stockdata import StockData
from backtest.response import ResponseSuccess
from backtest.use_cases.make_crypto_backtest import (
    MarketType, make_crypto_selector_reference_makretcap)


@pytest.fixture(scope='function')
def sample_selector_reference_list():
    btc_result = {'date': ['2020-01-01 00:00:00', '2020-01-02 00:00:00', '2020-01-03 00:00:00'],
                  'marketcap': [1.1, 2.2, 3.3]}
    eth_result = {'date': ['2020-01-02 00:00:00', '2020-01-03 00:00:00'],
                  'marketcap': [4.4, 5.5]}
    btc_sel_ref = SelectorReference.from_dict(btc_result, symbol='BTC')
    eth_sel_ref = SelectorReference.from_dict(eth_result, symbol='ETH')
    return [btc_sel_ref, eth_sel_ref]


@pytest.fixture(scope='function')
def stockdata_list(dict_stock_data_list):
    stockdata1 = StockData.from_dict(
        dict_data=dict_stock_data_list[0], symbol='BTC')
    stockdata2 = StockData.from_dict(
        dict_data=dict_stock_data_list[1], symbol='ETH')
    return [
        stockdata1, stockdata2
    ]


@mock.patch('backtest.use_cases.make_crypto_backtest.get_bithumb_symbol')
@mock.patch('backtest.repository.webrepo.crypto.coingecko_repo.CoinGeckoRepo.get')
def test_make_crypto_selector_marketcap(coingeckorepo, get_bithumb_symbol, sample_selector_reference_list):
    from_date = '2020-01-01'
    to_date = '2020-01-03'
    coingeckorepo.side_effect = [
        sample_selector_reference_list[0], sample_selector_reference_list[1]]
    get_bithumb_symbol.return_value = ['BTC', 'ETH']
    response = make_crypto_selector_reference_makretcap(
        market=MarketType.BITHUMB, from_date=from_date, to_date=to_date)
    assert isinstance(response, ResponseSuccess)
    assert isinstance(response.value, SelectorReference)
    assert list(response.value.data.columns) == ['BTC', 'ETH']  # 'BTC','ETH'
    assert response.value.symbol == 'BITHUMB'
    assert response.value.data.index[0] == '2020-01-01 00:00:00'
    assert response.value.data.index[-1] == '2020-01-03 00:00:00'


""" TODO
@mock.patch('backtest.use_cases.stockdata_from_repo.stockdata_from_repo')
@mock.patch('backtest.use_cases.make_crypto_backtest.get_bithumb_order')

def test_make_crypto_backtest_with_bithumb_repo(stockdata_from_repo, bithumb_order, stockdata_list):
    stockdata_from_repo.side_effect = [
        ResponseSuccess(stockdata_list[0]), ResponseSuccess(stockdata_list[1])]
    bithumb_order.return_value = ['BTC,ETH']

    selector = Selector(name='marketcap select', weight=10,
                        selector_function=marketcap_function,max_select_stock_num=1,reference={})
    strategy = Strategy(name='sma_self', function=sma_function,
                        weight=1, options={'rolling': 10})
    response = make_crypto_backtest(
        market=MarketType.BITHUMB, strategy_list=[strategy], selector_list=[])

    assert isinstance(response, ResponseSuccess)
    assert isinstance(response.value, BacktestResult)
"""
