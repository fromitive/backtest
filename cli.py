import pandas as pd
import numpy as np
import vectorbt as vbt

from backtest.use_cases.backtest_execute import backtest_execute
from backtest.use_cases.strategy_execute import stocastic_rsi_ema_mix_function
from backtest.domains.strategy_result import StrategyResultColumnType
from backtest.request.stockdata_from_repo import build_stock_data_from_repo_request
from backtest.use_cases.stockdata_from_repo import stockdata_from_repo
from backtest.repository.webrepo.crypto.upbit_repo import UpbitRepo
from backtest.domains.backtest_plot_package import BacktestPlotPackage
from backtest.domains.backtest import Backtest
from backtest.domains.strategy import Strategy
from datetime import datetime, timedelta
from trade import UpbitTrade

import requests
import threading
import time

def build_stockdata(symbol: str, from_date: str):
    request = build_stock_data_from_repo_request(
        filters={'order__eq': symbol, 'from__eq': from_date, 'chart_interval__eq': '30m'})
    response = stockdata_from_repo(UpbitRepo(), request=request, cache=False)
    stockdata = response.value

    request2 = build_stock_data_from_repo_request(
        filters={'order__eq': symbol, 'from__eq': from_date})
    response2 = stockdata_from_repo(UpbitRepo(), request=request2, cache=False)
    stockdata2 = response2.value
    return (stockdata, stockdata2)


def calculate_heikin_ashi(df, open, high, low, close):
    ha_close = (df[open] + df[high] + df[low] + df[close]) / 4
    ha_open = (df[open].shift(1) + df[close].shift(1)) / 2
    ha_high = df[[high, open, close]].max(axis=1)
    ha_low = df[[low, open, close]].min(axis=1)

    df['HA_Close'] = ha_close
    df['HA_Open'] = ha_open
    df['HA_High'] = ha_high
    df['HA_Low'] = ha_low
    df['Movement'] = np.where(df['HA_Close'] > df['HA_Open'], 'Up', 'Down')
    return df


def get_trade_asset_and_time() -> (pd.DataFrame,datetime):
    url = "https://api.bithumb.com/public/ticker/ALL_KRW"

    headers = {"accept": "application/json"}

    response = requests.get(url, headers=headers, timeout=5)

    if response.status_code == 200:
        json_data = response.json().get('data')
        timestamp = int(json_data['date']) / 1000
        current_time_obj = datetime.fromtimestamp(timestamp)
        
        del json_data['date']
        df = pd.DataFrame.from_dict(json_data, orient='index')
        df = df.astype(float)
        return df, current_time_obj
    else:
        return False, False


def is_tradeable(symbol: str, verbose: bool = False):
    from_date = datetime.now() - timedelta(days=14)
    from_date_str = from_date.strftime("%Y-%m-%d")
    stockdata, stockdata2 = build_stockdata(symbol=symbol, from_date=from_date_str)
    calculate_heikin_ashi(stockdata2.data, 'open', 'high', 'low', 'close')

    strategy = Strategy(name='stocastic_rsi_ema_mix_function', function=stocastic_rsi_ema_mix_function, weight=100,  options={'heikin_ashi':{stockdata.symbol: stockdata2},'sell_profit':0.02, 'sell_lose': -0.015, 'compare_movement':2})
    backtest = Backtest(strategy_list=[strategy], stockdata_list=[stockdata])
    plot_package = BacktestPlotPackage()
    backtest_execute(
        backtest, verbose=False, plot_package=plot_package).value
    strategy_result = plot_package.package_data_bucket[symbol][0]['stocastic_rsi_ema_mix_function']['stocastic_rsi_ema_mix_function']
    latest_result = strategy_result.iloc[-1][0]
    if verbose:
        print('[{time}] [{symbol} - {volume}] --> {latest_result}'.format(time=strategy_result.index[-1], symbol=symbol, volume=stockdata.data.volume.iloc[-1], latest_result=latest_result))
    if latest_result == StrategyResultColumnType.BUY:
        return True


def execute_trade(symbol: str, types: str, **kwargs) -> bool:
    global GLOBAL_TRADE_OBJECT
    global GLOBAL_BUY_DICT
    global GLOBAL_MAX_TRY_COUNT
    global GLOBAL_VERBOSE
    try_count = 0
    is_trade = False
    order_id = ''
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
            time.sleep(10)
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
                    GLOBAL_BUY_DICT['symbol'] = symbol
                    GLOBAL_BUY_DICT['order_id'] = order_id
                    GLOBAL_BUY_DICT['flag'] = True
                else:
                    GLOBAL_BUY_DICT['symbol'] = ''
                    GLOBAL_BUY_DICT['order_id'] = ''
                    GLOBAL_BUY_DICT['flag'] = False
                return True
        try_count += 1
    return False


def buy_thread(buy_rate: float = 0.5):
    global GLOBAL_BUY_DICT
    global GLOBAL_VERBOSE
    global GLOBAL_TRADE_STOCK_COUNT
    global GLOBAL_TRADE_OBJECT
    
    print('BUY_THREAD START')
    while True:
        if not GLOBAL_BUY_DICT['flag']:  
            try:
                current_time = GLOBAL_TRADE_OBJECT.get_current_time()
                minute = current_time.minute
                second = current_time.second

                if GLOBAL_VERBOSE:
                    print('[CURRENT-TIME] {}'.format(current_time))
                if minute == 29 or minute == 59:
                    if second >= 50:
                        symbols = GLOBAL_TRADE_OBJECT.get_top_symbol_list(GLOBAL_TRADE_STOCK_COUNT, 'acc_trade_price_24h')
                        for symbol in symbols:
                            if is_tradeable(symbol, verbose=GLOBAL_VERBOSE):
                                if GLOBAL_VERBOSE:
                                    print('BUY SYMBOL : {}'.format(symbol))
                                lock.acquire()  # Acquire the lock
                                execute_trade(symbol, 'BUY', buy_rate=buy_rate)
                                lock.release()  # Release the lock
                                break
            except Exception as e:
                print('[CRITICAL] EXCEPTION!! {}'.format(e))
        time.sleep(1)


def sell_thread(sell_profit, sell_lose):
    global GLOBAL_BUY_DICT
    global GLOBAL_VERBOSE
    global GLOBAL_TRADE_OBJECT
    
    print('SELL_THREAD START')
    while True:
        if GLOBAL_BUY_DICT['flag']:
            try:
                order_detail = GLOBAL_TRADE_OBJECT.get_order_detail(order_id=GLOBAL_BUY_DICT['order_id'])
                order_unit = order_detail['volume']
                order_price = float(order_detail['price'])
                current_price = float(GLOBAL_TRADE_OBJECT.get_coin_price(symbol=GLOBAL_BUY_DICT['symbol']))
                profit_rate = (current_price - order_price) / order_price
                if GLOBAL_VERBOSE:
                    print('[SELL_THREAD] [{symbol}] ORDER_PRICE : {order_price} -> CURRENT_PRICE : {current_price} / profit_rate : {profit_rate}'.format(symbol=GLOBAL_BUY_DICT['symbol'], order_price=order_price, current_price=current_price, profit_rate=profit_rate))
                if profit_rate >= sell_profit or profit_rate <= sell_lose:
                    lock.acquire()  # Acquire the lock
                    execute_trade(GLOBAL_BUY_DICT['symbol'], 'SELL', sell_unit=order_unit)
                    lock.release()
            except Exception as e:
                print('[CRITICAL] EXCEPTION!! {}'.format(e))
        time.sleep(1)


api_key = ''
secret = ''

with open('api-key.txt', 'r') as f:
    line = f.readlines()
    api_key = str(line[0]).strip()
    secret = str(line[1]).strip()

GLOBAL_TRADE_OBJECT = UpbitTrade(access_key=api_key, secret=secret)
GLOBAL_BUY_DICT = {'flag': False, 'symbol': '', 'order_id': ''}
GLOBAL_VERBOSE = True
GLOBAL_TRADE_STOCK_COUNT = 10
GLOBAL_MAX_TRY_COUNT = 30
buy_rate = 0.5
sell_profit = 0.025
sell_lose = -0.015
lock = threading.Lock()  # Create a lock object

# Create the threads

buy_routine = threading.Thread(target=buy_thread, args=(buy_rate,))
sell_routine = threading.Thread(target=sell_thread, args=(sell_profit, sell_lose))

# Start the threads
buy_routine.start()
sell_routine.start()

# Wait for the threads to finish (this will never happen in this case)
buy_routine.join()
sell_routine.join()