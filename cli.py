from backtest.repository.webrepo.bithumb_repo import BithumbRepo
from backtest.repository.webrepo.upbit_repo import UpbitRepo
from backtest.repository.webrepo.binance_repo import BinanceRepo
from backtest.use_cases.stockdata_from_repo import stockdata_from_repo
from backtest.request.stockdata_from_repo import build_stock_data_from_repo_request
from backtest.domains.strategy import Strategy, StrategyType
from backtest.use_cases.strategy_execute import strategy_execute

request = build_stock_data_from_repo_request(filters={'order__eq': 'btc'})
response = stockdata_from_repo(BithumbRepo(), request=request)
stockdata = response.value
strategy = Strategy(name='basic_strategy',
                    type=StrategyType.with_stockdata, data=stockdata.data)

strategy_result = strategy_execute(strategy=strategy)
