from backtest.repository.webrepo.bithumb_repo import BithumbRepo
from backtest.repository.webrepo.upbit_repo import UpbitRepo
from backtest.repository.webrepo.binance_repo import BinanceRepo
from backtest.use_cases.stockdata_from_repo import stockdata_from_repo
from backtest.request.stockdata_from_repo import build_stock_data_from_repo_request
from backtest.domains.strategy import Strategy
from backtest.use_cases.strategy_execute import basic_function
from backtest.domains.backtest import Backtest
from backtest.use_cases.strategy_execute import strategy_execute
from backtest.use_cases.backtest_execute import backtest_execute


request = build_stock_data_from_repo_request(
    filters={'order__eq': 'btc', 'from__eq': '2018-01-01'})
response = stockdata_from_repo(BithumbRepo(), request=request)
stockdata = response.value
strategy = Strategy(name='basic_strategy', function=basic_function)
backtest = Backtest(strategy_list=[strategy], stockdata_list=[stockdata])
backtest_result = backtest_execute(backtest)
