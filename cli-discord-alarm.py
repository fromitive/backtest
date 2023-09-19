from backtest.use_cases.backtest_execute import backtest_execute
from backtest.use_cases.strategy_execute import finally_fib, swing_search
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
from discord_webhook import DiscordWebhook


def build_stockdata(symbol: str, from_date: str, cache: bool = False, big_stock: bool = False):
    request = build_stock_data_from_repo_request(
        filters={'order__eq': symbol, 'from__eq': from_date})
    response = stockdata_from_repo(UpbitRepo(), request=request, cache=cache)
    stockdata = response.value

    big_stockdata = None
    if big_stock:
        big_stock_from_date = datetime.strptime(from_date, '%Y-%m-%d') - timedelta(days=900)
        big_request = build_stock_data_from_repo_request(
            filters={'order__eq': symbol, 'from__eq': big_stock_from_date.strftime('%Y-%m-%d')})
        big_response = stockdata_from_repo(UpbitRepo(), request=big_request, cache=cache)

        big_stockdata = big_response.value

    return stockdata, big_stockdata


def is_tradeable(symbol: str, verbose: bool = False, trade_strategy_list: List[TradeStrategyPackage] = []) -> (bool, dict, TradeStrategyPackage):
    from_date = datetime.now() - timedelta(days=900)
    from_date_str = from_date.strftime("%Y-%m-%d")
    stockdata, big_stockdata = build_stockdata(symbol=symbol, from_date=from_date_str, cache=False, big_stock=False)
    strategy_list = []

    for trade_strategy in trade_strategy_list:
        if 'big_stockdata' in trade_strategy.strategy_options.keys():
            trade_strategy.strategy_options['big_stockdata'] = big_stockdata

        function_name = trade_strategy.strategy_function_name
        strategy_function = trade_strategy.strategy_function
        strategy = Strategy(name=function_name, function=strategy_function, weight=100, options=trade_strategy.strategy_options)
        strategy_list.append(strategy)
    backtest = Backtest(strategy_list=strategy_list, stockdata_list=[stockdata])
    plot_package = BacktestPlotPackage()
    backtest_execute(
        backtest, verbose=False, plot_package=plot_package).value

    dummy_extra_data = dict()
    for trade_strategy, package_data in zip(trade_strategy_list, plot_package.package_data_bucket[symbol]):
        function_name = trade_strategy.strategy_function_name
        strategy_result = package_data[function_name][function_name]
        min_observe_time_idx = trade_strategy.min_observe_time_idx
        max_observe_time_idx = trade_strategy.max_observe_time_idx
        min_minute = (abs(min_observe_time_idx) - 1) * 30 if min_observe_time_idx else 30
        max_minute = abs(min_observe_time_idx) * 30 if min_observe_time_idx else 60
        extra_data = dict()
        for column in trade_strategy.extra_column:
            extra_data[column] = float(package_data[function_name][column].iloc[-1])
        dummy_extra_data = extra_data

        for index, values in zip(strategy_result.iloc[min_observe_time_idx:max_observe_time_idx].index.values, strategy_result.iloc[min_observe_time_idx:max_observe_time_idx].values):
            current_time = datetime.now()
            index_time = datetime.strptime(index, "%Y-%m-%d %H:%M:%S")
            time_diff = current_time - index_time
            if values[0] == StrategyResultColumnType.BUY:
                print('[{time}] [{symbol}] --> (strategy_function : {function_name}) {latest_result}'.format(time=index, symbol=symbol, function_name=function_name, latest_result=values[0]))
                return True, extra_data, trade_strategy

    return False, dummy_extra_data, trade_strategy


def is_market_timing(minute: int, second: int, hour: int, trade_strategy_list: List[TradeStrategyPackage]) -> List[TradeStrategyPackage]:
    result = []
    for trade_strategy in trade_strategy_list:
        if minute in trade_strategy.market_timing:
            if second >= 0:
                result.append(trade_strategy)
    return result


def discord_message(symbol, current_time, discord_type: str, trade_strategy_package: TradeStrategyPackage, **kwargs):
    strategy_name = trade_strategy_package.strategy_function_name
    message_function = trade_strategy_package.message_function
    message_function(symbol, current_time, discord_type, strategy_name, **kwargs)


def discord_fib_message(symbol, current_time, discord_type, strategy_name, **kwargs):
    with open(discord_type, 'r') as f:
        lines = f.readlines()
        artifacts = "-----artifacts-----\n"
        current_price = kwargs['extra_data']['close']
        local_max_price = kwargs['extra_data']['local_max']
        fib_columns = []
        close_fib_list = []
        if 'extra_data' in kwargs.keys():
            for artifact in kwargs['extra_data']:
                artifacts += "{} : {:.2f}\n".format(artifact, kwargs['extra_data'][artifact])
                if 'fib_sig' in artifact:
                    close_fib_list.append((abs(current_price - kwargs['extra_data'][artifact]), artifact))
            if close_fib_list:
                fib_columns = [item[1] for item in close_fib_list]
                close_fib, close_fib_rate = close_fib_list[0]
                for item in close_fib_list:
                    before_fib = close_fib
                    close_fib = min(close_fib, item[0])
                    if before_fib != close_fib:
                        close_fib_rate = item[1]
                # get buy, sell price
                buy_price = kwargs['extra_data'][close_fib_rate]
                
                sell_price = kwargs['extra_data'][close_fib_rate]
                sell_fib_rate = close_fib_rate
                sell_fib_index = fib_columns.index(close_fib_rate) + 1
                
                if sell_fib_index == len(fib_columns):
                    sell_fib_rate = 'local_max'
                    sell_price = local_max_price
                else:
                    sell_fib_rate = fib_columns[sell_fib_index]
                    sell_price = kwargs['extra_data'][sell_fib_rate]

            artifacts += "🔎  current price {} closed to {:.2f}({})\n".format(current_price, close_fib, close_fib_rate)
            artifacts += "🎯  target price\n"
            artifacts += "- buy : {:.2f} ({})\n".format(buy_price, close_fib_rate)
            artifacts += "- sell : {:.2f} ({})\n".format(sell_price, sell_fib_rate)
        artifacts += "-----artifacts end-----\n\n"
            
        content = '[{current_time}] [{strategy_name}] BUY *{symbol}*\n\nartifects\n{artifacts}'.format(current_time=current_time, strategy_name=strategy_name, symbol=symbol, artifacts=artifacts)
        webhook = DiscordWebhook(url=lines[0].strip(), content=content)
        webhook.execute()


def discord_swing_message(symbol, current_time, discord_type, strategy_name, **kwargs):
    with open(discord_type, 'r') as f:
        lines = f.readlines()
        artifacts = "-----artifacts-----\n"
        if 'extra_data' in kwargs.keys():
            for artifact in kwargs['extra_data']:
                artifacts += "{} : {:.2f}\n".format(artifact, kwargs['extra_data'][artifact])
        artifacts += "-----artifacts end-----\n\n"

        content = '[{current_time}] [{strategy_name}] BUY *{symbol}*\n\nartifects\n{artifacts}'.format(current_time=current_time, strategy_name=strategy_name, symbol=symbol, artifacts=artifacts)
        webhook = DiscordWebhook(url=lines[0].strip(), content=content)
        webhook.execute()


def buy_thread(trade_strategy_list: List[TradeStrategyPackage] = []):
    global GLOBAL_VERBOSE
    global GLOBAL_TRADE_STOCK_COUNT
    global GLOBAL_TRADE_STOCK_MINIMAL_RANK
    global GLOBAL_TRADE_OBJECT
    global GLOBAL_SET
    global GLOBAL_MAX_BUY_COUNT
    sent_symbol_set = set()
    print('BUY_THREAD START')
    while True:
        try:
            current_time = GLOBAL_TRADE_OBJECT.get_current_time()
            minute = current_time.minute
            second = current_time.second
            hour = current_time.hour
            market_timing_trade_strategy_list = is_market_timing(minute, second, hour, trade_strategy_list)
            if hour == 9:
                sent_symbol_set = set()
                print('[DEBUG] reset symbol set')
            if market_timing_trade_strategy_list:
                if GLOBAL_VERBOSE:
                    print('[CURRENT-TIME] {}'.format(current_time))
                symbols = GLOBAL_TRADE_OBJECT.get_top_symbol_list(GLOBAL_TRADE_STOCK_MINIMAL_RANK, 'acc_trade_price_24h')
                symbols = symbols[:40]
                # symbols = symbols[50:60] + symbols[60:70] + symbols[80:90] + symbols[100:110]
                if 'BTT' in symbols:
                    symbols.remove('BTT')

                for symbol in symbols:
                    tradeable, extra_data, trade_strategy_package = is_tradeable(symbol=symbol, verbose=GLOBAL_VERBOSE, trade_strategy_list=market_timing_trade_strategy_list)
                    if tradeable and symbol not in sent_symbol_set:
                        trade_routine = threading.Thread(target=discord_message, args=(symbol, current_time, 'discord_url.txt', trade_strategy_package), kwargs={'extra_data': extra_data})
                        trade_routine.start()
                        sent_symbol_set.add(symbol)

                    if hour in [5, 9, 18] and minute == 0 and symbol not in sent_symbol_set:
                        print('[DEBUG] current symbol set')
                        trade_routine = threading.Thread(target=discord_message, args=(symbol, current_time, 'discord_check.txt', trade_strategy_package), kwargs={'extra_data': extra_data})
                        trade_routine.start()
                        sent_symbol_set.add(symbol)
        except Exception as e:
            print('[BUY-THREAD CRITICAL] EXCEPTION!! {}'.format(e))
            if lock.locked():
                lock.release()
        time.sleep(1)


api_key = ''
secret = ''
GLOBAL_VERBOSE = True
lock = threading.Lock()  # Create a lock object
with open('api-key.txt', 'r') as f:
    line = f.readlines()
    api_key = str(line[0]).strip()
    secret = str(line[1]).strip()
    

GLOBAL_TRADE_OBJECT = UpbitTrade(access_key=api_key, secret=secret)
GLOBAL_TRADE_STOCK_MINIMAL_RANK = 100
# Create the threads
trade_strategy_list = []

fib_signal_list = [0.786, 0.618, 0.5, 0.382, 0.236]
extra_column_fib = ['local_min', 'local_max', 'close', 'ema_200', 'volume_level_rate', 'volume_obstacle']
extra_column_swing = ['close', 'ema_200', 'ema_400', 'ema_5', 'Senkou_span_B', 'sell_loss', 'sell_profit']
for fib_sig in fib_signal_list:
    extra_column_fib.append('fib_sig_{}'.format(fib_sig))
basic_extra_column = ['sell_loss', 'sell_profit', 'close', 'open']


def _calc_basic_sell_loss_profit(order_price, extra_data, sell_loss, sell_profit):
    result = {'sell_loss': sell_loss, 'sell_profit': sell_profit}
    result['sell_loss'] = extra_data['sell_loss']
    result['sell_profit'] = extra_data['sell_profit']
    return result


package1 = TradeStrategyPackage(strategy_function=finally_fib,
                                strategy_function_name='finally_fib',
                                strategy_options={'fib_signal_list': fib_signal_list, 'vp_range': 250},
                                extra_function=_calc_basic_sell_loss_profit,
                                extra_column=extra_column_fib,
                                buy_bid_ask='bid',
                                market_timing=[0, 5, 10, 15, 20, 25, 30, 35, 40, 45, 50, 55],
                                sell_prior=4,
                                min_observe_time_idx=-1,
                                max_observe_time_idx=None,
                                message_function=discord_fib_message,
                                )

package2 = TradeStrategyPackage(strategy_function=swing_search,
                                strategy_function_name='swing_search',
                                strategy_options={},
                                extra_function=_calc_basic_sell_loss_profit,
                                extra_column=extra_column_swing,
                                buy_bid_ask='bid',
                                market_timing=[0, 5, 10, 15, 20, 25, 30, 35, 40, 45, 50, 55],
                                sell_prior=4,
                                min_observe_time_idx=-1,
                                max_observe_time_idx=None,
                                message_function=discord_swing_message,
                                )


trade_strategy_list.append(package1)
trade_strategy_list.append(package2)

# Create the threads
buy_routine = threading.Thread(target=buy_thread, args=(trade_strategy_list, ))

# Start the threads
buy_routine.start()

# Wait for the threads to finish (this will never happen in this case)
buy_routine.join()
