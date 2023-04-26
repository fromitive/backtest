import sys

from backtest.domains.backtest import Backtest
from backtest.domains.selector import Selector
from backtest.domains.strategy import Strategy
from backtest.repository.finance.finance_repo import FinanceRepo
from backtest.repository.webrepo.crypto.bithumb_repo import BithumbRepo
from backtest.request.stockdata_from_repo import \
    build_stock_data_from_repo_request
from backtest.response import ResponseSuccess
from backtest.use_cases.backtest_execute import backtest_execute
from backtest.use_cases.make_crypto_backtest import (
    MarketType, get_bithumb_symbol, make_crypto_selector_reference_makretcap,
    make_crypto_stockdata_list)
from backtest.use_cases.selector_execute import (rank_marketcap_function,
                                                 selector_execute)
from backtest.use_cases.stockdata_from_repo import stockdata_from_repo
from backtest.use_cases.strategy_execute import (greed_fear_index_function,
                                                 rsi_big_stock_function,
                                                 rsi_function,
                                                 sma_big_stock_function,
                                                 sma_function,
                                                 sma_multi_big_stock_function)
from backtest.util.stockdata_util import get_greed_fear_index

top_rate = float(sys.argv[1])
select_pick = int(sys.argv[2])

print('top_rate {}, select_pick {}'.format(top_rate, select_pick))
request2 = build_stock_data_from_repo_request(
    filters={'order__eq': 'BTC', 'from__eq': '2017-01-01'})
response2 = stockdata_from_repo(BithumbRepo(), request=request2, cache=True)

request4 = build_stock_data_from_repo_request(
    filters={'order__eq': 'US500', 'from__eq': '2017-01-01'})
response4 = stockdata_from_repo(FinanceRepo(), request=request4, cache=True)


stockdata2 = response2.value
stockdata4 = response4.value


strategy1 = Strategy(name='snp_big_stock', function=sma_big_stock_function,
                     weight=3, options={'big_stock': stockdata4, 'rolling': 100})
strategy2 = Strategy(name='sma_big_stock', function=sma_big_stock_function,
                     weight=2, options={'big_stock': stockdata2, 'rolling': 90})
strategy3 = Strategy(name='sma_self', function=sma_function,
                     weight=1, options={'rolling': 10})
greed_fear_df = get_greed_fear_index()
strategy4 = Strategy(name='Greed_Fear_Index', function=greed_fear_index_function,
                     weight=1, options={'greed_fear_index_data': greed_fear_df, 'index_fear': 20, 'index_greed': 60})
strategy5 = Strategy(name='rsi_function', function=rsi_function,
                     weight=0.5, options={'period': 15, 'overbought_level': 30, 'oversold_level': 70})
strategy6 = Strategy(name='rsi_big_stock_function', function=rsi_big_stock_function,
                     weight=0.5, options={'big_stock': stockdata4, 'period': 15, 'overbought_level': 30, 'oversold_level': 70})
strategy7 = Strategy(name='sma_multi_big_stock', function=sma_multi_big_stock_function,
                     weight=1, options={'big_stock': stockdata4, 'rolling_list': [100, 15]})
strategy8 = Strategy(name='sma_multi_big_stock', function=sma_multi_big_stock_function,
                     weight=1, options={'big_stock': stockdata2, 'rolling_list': [100, 15]})


response = make_crypto_selector_reference_makretcap(
    market=MarketType.BITHUMB, from_date='2017-01-01', cache=True)
if isinstance(response, ResponseSuccess):
    selector_reference = response.value
    symbol_list = get_bithumb_symbol()
    selector1 = Selector(name='basic function', weight=10, selector_function=rank_marketcap_function,
                         reference=response.value, options={'top_rate': top_rate, 'select_pick': select_pick})
    print('selector_execute')
    response = selector_execute(
        selector_list=[selector1], symbol_list=symbol_list, from_date='2017-01-01')
    selector_result = response.value
    print('make crypto_stockdata_list')
    stockdata_list = make_crypto_stockdata_list(
        market=MarketType.BITHUMB, selector_result=selector_result, cache=True)


backtest = Backtest(strategy_list=[strategy1, strategy2, strategy3, strategy4, strategy5, strategy6, strategy7, strategy8],
                    stockdata_list=stockdata_list, selector_result=selector_result)
backtest_result = backtest_execute(backtest).value
backtest_result.value
df = backtest_result.value['total_profit'].expanding().sum()
df.to_csv('expanding_sum_{}_{}.csv'.format(top_rate, select_pick))
backtest_result.value.to_csv('sample_{}_{}.csv'.format(top_rate, select_pick))
