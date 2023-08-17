from backtest.use_cases.backtest_execute import backtest_execute
from backtest.use_cases.strategy_execute import ema_local_min_max_trandline_function, ema_swing_low_function
from backtest.domains.strategy_result import StrategyResultColumnType
from backtest.request.stockdata_from_repo import build_stock_data_from_repo_request
from backtest.use_cases.stockdata_from_repo import stockdata_from_repo
from backtest.repository.webrepo.crypto.upbit_repo import UpbitRepo
from backtest.domains.backtest_plot_package import BacktestPlotPackage
from backtest.domains.backtest import Backtest
from backtest.domains.strategy import Strategy
from datetime import datetime, timedelta
from trade import UpbitTrade, TradeStrategyPackage
from typing import List

import threading
import time

def build_stockdata(symbol: str, from_date: str, cache: bool = False):
    request = build_stock_data_from_repo_request(
        filters={'order__eq': symbol, 'from__eq': from_date, 'chart_interval__eq': '30m'})
    response = stockdata_from_repo(UpbitRepo(), request=request, cache=cache)
    stockdata = response.value
    return stockdata


def is_tradeable(symbol: str, verbose: bool = False, trade_strategy_list: List[TradeStrategyPackage] = []):
    from_date = datetime.now() - timedelta(days=28)
    from_date_str = from_date.strftime("%Y-%m-%d")
    stockdata = build_stockdata(symbol=symbol, from_date=from_date_str, cache=False)
    strategy_list = []
    for trade_strategy in trade_strategy_list:
        function_name = trade_strategy.strategy_function_name
        strategy_function = trade_strategy.strategy_function
        strategy = Strategy(name=function_name, function=strategy_function, weight=100, options=trade_strategy.strategy_options)
        strategy_list.append(strategy)
    backtest = Backtest(strategy_list=strategy_list, stockdata_list=[stockdata])
    plot_package = BacktestPlotPackage()
    backtest_execute(
        backtest, verbose=False, plot_package=plot_package).value
    
    dummy_extra_data = dict()
    dummy_extra_function = None
    for trade_strategy, package_data in zip(trade_strategy_list, plot_package.package_data_bucket[symbol]):
        function_name = trade_strategy.strategy_function_name
        strategy_result = package_data[function_name][function_name]
    
        extra_data = dict()
        for column in trade_strategy.extra_column:
            extra_data[column] = float(package_data[function_name][column].iloc[-1])
        dummy_extra_data = extra_data
        dummy_extra_function = trade_strategy.extra_function
        
        for index, values in zip(strategy_result.iloc[-2:].index.values, strategy_result.iloc[-2:].values):
            if verbose:
                print('[{time}] [{symbol}] --> (strategy_function : {function_name}) {latest_result}'.format(time=index, symbol=symbol, function_name=function_name, latest_result=values[0]))
            if values[0] == StrategyResultColumnType.BUY:
                return True, extra_data, trade_strategy.extra_function
    
    return False, dummy_extra_data, dummy_extra_function


def is_tradeable_check(symbol: str, verbose: bool = False, trade_strategy_list: List[TradeStrategyPackage] = [], delay: int = 600):
    tradeable, extra_data, extra_function = is_tradeable(symbol, verbose=verbose, trade_strategy_list=trade_strategy_list)
    if tradeable:
        print('buy pending... recheck')
        time.sleep(delay)
        tradeable, extra_data, extra_function = is_tradeable(symbol, verbose=verbose, trade_strategy_list=trade_strategy_list)
    return tradeable, extra_data, extra_function


def execute_trade(symbol: str, types: str, **kwargs) -> bool:
    global GLOBAL_TRADE_OBJECT
    global GLOBAL_MAX_TRY_COUNT
    global GLOBAL_VERBOSE
    global GLOBAL_SET
    global GLOBAL_DEFAULT_SELL_LOSS
    global GLOBAL_DEFAULT_SELL_PROFIT
    try_count = 0
    is_trade = False
    order_id = ''
    coin_price = 0.0
    while try_count < GLOBAL_MAX_TRY_COUNT:
        if is_trade is False:
            if types == 'BUY':
                buy_rate = kwargs['buy_rate'] 
                my_krw = float(GLOBAL_TRADE_OBJECT.get_krw()) * buy_rate
                trade_fee = float(GLOBAL_TRADE_OBJECT.get_trade_fee(symbol=symbol))
                coin_price = float(GLOBAL_TRADE_OBJECT.get_coin_price(symbol=symbol))
                buy_unit = my_krw / coin_price * (1 - trade_fee)
                order_result = GLOBAL_TRADE_OBJECT.trade(symbol=symbol, units="{:.4f}".format(buy_unit), type='BUY', price=coin_price, payment_currency="KRW")
            elif types == 'SELL':
                sell_unit = kwargs['sell_unit']
                coin_price = float(GLOBAL_TRADE_OBJECT.get_coin_price(symbol=symbol))
                order_result = GLOBAL_TRADE_OBJECT.trade(symbol=symbol, units=sell_unit, type='SELL', price=coin_price, payment_currency="KRW")
            if order_result and '-' in order_result:
                is_trade = True
                order_id = order_result
            else:
                print('[ERROR] {}'.format(order_id))
                
        if order_id and is_trade:
            time.sleep(15)
            order_status = GLOBAL_TRADE_OBJECT.get_order_status(order_id=order_id)
            order_type = GLOBAL_TRADE_OBJECT.get_order_type(order_id=order_id)
            
            # Order Cancel <-- todo!
            if GLOBAL_VERBOSE:
                print("[INFO] [{types} - {symbol}] Order Status : {order_status}, try_count : {try_count}".format(types=types, symbol=symbol, order_status=order_status, try_count=try_count))
                
            if not order_status:
                GLOBAL_TRADE_OBJECT.order_cancel(order_id=order_id, symbol=symbol, order_type=order_type)
                if GLOBAL_TRADE_OBJECT.get_cancel_status(order_id):
                    order_id = ''
                    is_trade = False
            else:
                if types == 'BUY':
                    buy_dict = {'flag': False, 'symbol': '', 'order_id': '', 'sell_profit': GLOBAL_DEFAULT_SELL_PROFIT, 'sell_loss': GLOBAL_DEFAULT_SELL_LOSS, 'default_sell_loss': GLOBAL_DEFAULT_SELL_LOSS, 'default_sell_profit': GLOBAL_DEFAULT_SELL_PROFIT}
                    buy_dict['symbol'] = symbol
                    buy_dict['order_id'] = order_id
                    buy_dict['flag'] = True
                    if 'extra_data' in kwargs.keys() and 'extra_function' in kwargs.keys():
                        extra_data = kwargs['extra_data']
                        extra_function = kwargs['extra_function']
                        result = extra_function(coin_price, extra_data, GLOBAL_DEFAULT_SELL_LOSS, GLOBAL_DEFAULT_SELL_PROFIT)
                        buy_dict['sell_loss'] = result['sell_loss']
                        buy_dict['sell_profit'] = result['sell_profit']
                        sell_routine = threading.Thread(target=sell_thread, args=(buy_dict, ))
                        sell_routine.start()
                    GLOBAL_SET.add(symbol)
                else:
                    GLOBAL_SET.remove(symbol)
                return True
        try_count += 1
    return False


def is_market_timing(minute_list: List[int], minute: int, second: int):
    if minute in minute_list:
        if second >= 0:
            return True
    return False


def buy_thread(buy_rate: float = 0.5, trade_strategy_list: List[TradeStrategyPackage] = []):
    global GLOBAL_VERBOSE
    global GLOBAL_TRADE_STOCK_COUNT
    global GLOBAL_TRADE_STOCK_MINIMAL_RANK
    global GLOBAL_TRADE_OBJECT
    global GLOBAL_SET
    global GLOBAL_MAX_BUY_COUNT
    
    print('BUY_THREAD START')
    while True:
        try:
            current_time = GLOBAL_TRADE_OBJECT.get_current_time()
            minute = current_time.minute
            second = current_time.second
            if is_market_timing([0, 30], minute, second):
                if GLOBAL_VERBOSE:
                    print('[CURRENT-TIME] {}'.format(current_time))
                symbols = GLOBAL_TRADE_OBJECT.get_top_symbol_list(GLOBAL_TRADE_STOCK_MINIMAL_RANK, 'acc_trade_price_24h') 
                symbols = symbols[::-1][:GLOBAL_TRADE_STOCK_COUNT]
                for symbol in symbols:
                    tradeable, extra_data, extra_function = is_tradeable_check(symbol=symbol, verbose=GLOBAL_VERBOSE, trade_strategy_list=trade_strategy_list, delay=600)
                    if tradeable:
                        if GLOBAL_VERBOSE:
                            print('BUY SYMBOL : {}'.format(symbol))
                        if symbol not in GLOBAL_SET or len(GLOBAL_SET) < GLOBAL_MAX_BUY_COUNT:
                            lock.acquire()  # Acquire the lock
                            execute_trade(symbol, 'BUY', buy_rate=buy_rate, extra_data=extra_data, extra_function=extra_function)
                            lock.release()  # Release the lock
        except Exception as e:
            print('[CRITICAL] EXCEPTION!! {}'.format(e))
        time.sleep(1)


def sell_thread(buy_dict):
    global GLOBAL_VERBOSE
    global GLOBAL_TRADE_OBJECT
    global GLOBAL_LEFT_HOUR
    current_time = GLOBAL_TRADE_OBJECT.get_current_time()
    print('[{current_time}] {symbol} / sell_profit : {sell_profit} / sell_loss : {sell_loss} - SELL_THREAD START'.format(current_time=current_time, symbol=buy_dict['symbol'], sell_profit=buy_dict['sell_profit'], sell_loss=buy_dict['sell_loss']))
    while True:
        try:
            order_detail = GLOBAL_TRADE_OBJECT.get_order_detail(order_id=buy_dict['order_id'])
            symbol_balance = GLOBAL_TRADE_OBJECT.get_balance(symbol=buy_dict['symbol'])
            order_unit = ""
            if isinstance(symbol_balance, float):
                order_unit = "{:.4f}".format(symbol_balance)
            else:
                raise ValueError("API Server has Error!")
            if order_unit == "0.0000":
                print('[{current_time}] {symbol} - ALREADY SELLED'.format(current_time=current_time, symbol=buy_dict['symbol']))
                GLOBAL_SET.remove(buy_dict['symbol'])
                break
            order_price = float(order_detail['price'])
            current_price = float(GLOBAL_TRADE_OBJECT.get_coin_price(symbol=buy_dict['symbol']))
            profit_rate = (current_price - order_price) / order_price
            
            left_hour = (datetime.now() - current_time).seconds // 3600
            if left_hour >= GLOBAL_LEFT_HOUR and profit_rate > 0.0:
                lock.acquire()
                print("[{symbol}] {left_hour} HOUR LEFT. SELL ANYWAY".format(symbol= buy_dict['symbol'], left_hour=GLOBAL_LEFT_HOUR))
                execute_trade(buy_dict['symbol'], 'SELL', sell_unit=order_unit)
                lock.release()
            
            # reset sell-loss https://www.youtube.com/watch?v=q-Jk-qD_EEI
            if profit_rate / 0.001 > 0:
                if profit_rate >= buy_dict['sell_profit']:
                    buy_dict['sell_loss'] = buy_dict['sell_profit'] * 0.8
                    buy_dict['sell_profit'] = buy_dict['sell_profit'] * 1.2
                else:
                    buy_dict['sell_loss'] = (profit_rate / 2.0) - 0.0005  # reset sell_loss
                print("[SELL-THREAD ({symbol}]) RESET SELL-LOSS : {sell_loss}".format(symbol=buy_dict['symbol'], sell_loss=buy_dict['sell_loss']))
                
            if profit_rate >= buy_dict['sell_profit'] or profit_rate <= buy_dict['sell_loss']:
                lock.acquire()
                if execute_trade(buy_dict['symbol'], 'SELL', sell_unit=order_unit):
                    lock.release()
                    break
                lock.release()
        except Exception as e:
            print('[CRITICAL] EXCEPTION!! {}'.format(e))
        time.sleep(1)
    print('[{current_time}] {symbol} - SELL_THREAD END'.format(current_time=current_time, symbol=buy_dict['symbol']))


api_key = ''
secret = ''

with open('api-key.txt', 'r') as f:
    line = f.readlines()
    api_key = str(line[0]).strip()
    secret = str(line[1]).strip()
default_sell_profit = 0.020
default_sell_loss = -0.015
GLOBAL_TRADE_OBJECT = UpbitTrade(access_key=api_key, secret=secret)
GLOBAL_DEFAULT_SELL_LOSS = -0.015
GLOBAL_DEFAULT_SELL_PROFIT = 0.025
GLOBAL_SET = set()
GLOBAL_VERBOSE = True
GLOBAL_TRADE_STOCK_MINIMAL_RANK = 100
GLOBAL_TRADE_STOCK_COUNT = 40
GLOBAL_MAX_TRY_COUNT = 30
GLOBAL_MAX_BUY_COUNT = 3
GLOBAL_LEFT_HOUR = 18
buy_rate = 0.5
lock = threading.Lock()  # Create a lock object

# Create the threads
trade_strategy_list = []
extra_column = ['sell_loss', 'sell_profit']


def _calc_sell_loss_profit(order_price, extra_data, sell_loss, sell_profit):
    result = {'sell_loss': sell_loss, 'sell_profit': sell_profit}
    result['sell_loss'] = extra_data['sell_loss']
    result['sell_profit'] = extra_data['sell_profit']
    return result


package1 = TradeStrategyPackage(strategy_function=ema_local_min_max_trandline_function,
                                strategy_function_name='ema_local_min_max_trandline_function',
                                extra_function=_calc_sell_loss_profit,
                                extra_column=extra_column)

package2 = TradeStrategyPackage(strategy_function=ema_swing_low_function,
                                strategy_function_name='ema_swing_low_function',
                                extra_function=_calc_sell_loss_profit,
                                extra_column=extra_column)

package3 = TradeStrategyPackage(strategy_function=ema_swing_low_function,
                                strategy_function_name='ema_swing_low_function2',
                                strategy_options={'ema_period':20, 'rate_threshold':0.01},
                                extra_function=_calc_sell_loss_profit,
                                extra_column=extra_column
                                )

trade_strategy_list.append(package1)
trade_strategy_list.append(package2)
trade_strategy_list.append(package3)

# Create the threads
buy_routine = threading.Thread(target=buy_thread, args=(buy_rate, trade_strategy_list))

# Start the threads
buy_routine.start()

# Wait for the threads to finish (this will never happen in this case)
buy_routine.join()