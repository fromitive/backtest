from backtest.use_cases.backtest_execute import backtest_execute
from backtest.use_cases.strategy_execute import candle_stick_pattern, ema_swing_low_function
from backtest.domains.strategy_result import StrategyResultColumnType
from backtest.request.stockdata_from_repo import build_stock_data_from_repo_request
from backtest.use_cases.stockdata_from_repo import stockdata_from_repo
from backtest.repository.webrepo.crypto.upbit_repo import UpbitRepo
from backtest.domains.backtest_plot_package import BacktestPlotPackage
from backtest.domains.backtest import Backtest
from backtest.domains.strategy import Strategy
from datetime import datetime, timedelta
from trade import UpbitTrade, TradeStrategyPackage

import threading
import time
from typing import List


def build_stockdata(symbol: str, from_date: str, cache: bool = False, big_stock: bool = False):
    request = build_stock_data_from_repo_request(
        filters={"order__eq": symbol, "from__eq": from_date, "chart_interval__eq": "30m"}
    )
    response = stockdata_from_repo(UpbitRepo(), request=request, cache=cache)
    stockdata = response.value

    big_stockdata = None
    if big_stock:
        big_stock_from_date = datetime.strptime(from_date, "%Y-%m-%d") - timedelta(days=900)
        big_request = build_stock_data_from_repo_request(
            filters={"order__eq": symbol, "from__eq": big_stock_from_date.strftime("%Y-%m-%d")}
        )
        big_response = stockdata_from_repo(UpbitRepo(), request=big_request, cache=cache)

        big_stockdata = big_response.value

    return stockdata, big_stockdata


def is_tradeable(
    symbol: str, verbose: bool = False, trade_strategy_list: List[TradeStrategyPackage] = []
) -> (bool, dict, TradeStrategyPackage):
    from_date = datetime.now() - timedelta(days=28)
    from_date_str = from_date.strftime("%Y-%m-%d")
    stockdata, big_stockdata = build_stockdata(symbol=symbol, from_date=from_date_str, cache=False, big_stock=True)
    strategy_list = []

    for trade_strategy in trade_strategy_list:
        if "big_stockdata" in trade_strategy.strategy_options.keys():
            trade_strategy.strategy_options["big_stockdata"] = big_stockdata

        function_name = trade_strategy.strategy_function_name
        strategy_function = trade_strategy.strategy_function
        strategy = Strategy(
            name=function_name, function=strategy_function, weight=100, options=trade_strategy.strategy_options
        )
        strategy_list.append(strategy)
    backtest = Backtest(strategy_list=strategy_list, stockdata_list=[stockdata])
    plot_package = BacktestPlotPackage()
    backtest_execute(backtest, verbose=False, plot_package=plot_package).value

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

        for index, values in zip(
            strategy_result.iloc[min_observe_time_idx:max_observe_time_idx].index.values,
            strategy_result.iloc[min_observe_time_idx:max_observe_time_idx].values,
        ):
            current_time = datetime.now()
            index_time = datetime.strptime(index, "%Y-%m-%d %H:%M:%S")
            time_diff = current_time - index_time
            time_minutes = time_diff.seconds // 60
            if time_minutes >= min_minute and time_minutes < max_minute:
                if verbose and time_diff.seconds:
                    print(
                        "[{time}] [{symbol}] --> (strategy_function : {function_name}) {latest_result}".format(
                            time=index, symbol=symbol, function_name=function_name, latest_result=values[0]
                        )
                    )

                if values[0] == StrategyResultColumnType.BUY:
                    return True, extra_data, trade_strategy

    return False, dummy_extra_data, trade_strategy


def is_tradeable_check(
    symbol: str, verbose: bool = False, trade_strategy_list: List[TradeStrategyPackage] = [], delay: int = 600
):
    tradeable, extra_data, extra_function = is_tradeable(
        symbol, verbose=verbose, trade_strategy_list=trade_strategy_list
    )
    if tradeable:
        print("buy pending... recheck")
        time.sleep(delay)
        tradeable, extra_data, extra_function = is_tradeable(
            symbol, verbose=verbose, trade_strategy_list=trade_strategy_list
        )
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
    order_id = ""
    cancel_check = False
    coin_price = 0.0
    while try_count < GLOBAL_MAX_TRY_COUNT:
        if is_trade is False:
            if types == "BUY":
                trade_strategy_package = kwargs["trade_strategy_package"]
                order_result = "8497594a-a9fc-40e1-8304-45a986b46a86"
                coin_price = float(
                    GLOBAL_TRADE_OBJECT.get_coin_price(symbol=symbol, bid_ask=trade_strategy_package.buy_bid_ask)
                )
            elif types == "SELL":
                order_result = "8497594a-a9fc-40e1-8304-45a986b46a86"
            if order_result and "-" in order_result:
                is_trade = True
                order_id = order_result
            else:
                print("[ERROR] {}".format(order_id))

        if order_id and is_trade:
            time.sleep(1)
            if cancel_check:
                order_status = True
            else:
                order_status = False

            # Order Cancel <-- todo!
            if GLOBAL_VERBOSE:
                print(
                    "[INFO] [{types} - {symbol}] Order Status : {order_status}, try_count : {try_count}".format(
                        types=types, symbol=symbol, order_status=order_status, try_count=try_count
                    )
                )

            if not order_status:
                get_cancel_status = True
                if get_cancel_status:
                    order_id = ""
                    is_trade = False
                    cancel_check = True
            else:
                if types == "BUY":
                    buy_dict = {
                        "flag": False,
                        "symbol": "",
                        "order_id": "",
                        "sell_profit": 0.025,
                        "sell_loss": -0.015,
                        "default_sell_loss": -0.015,
                        "default_sell_profit": 0.025,
                    }
                    buy_dict["symbol"] = symbol
                    buy_dict["order_id"] = order_id
                    buy_dict["flag"] = True
                    if "extra_data" in kwargs.keys():
                        extra_data = kwargs["extra_data"]
                        trade_strategy_package = kwargs["trade_strategy_package"]
                        result = trade_strategy_package.extra_function(coin_price, extra_data, -0.015, 0.025)
                        buy_dict["sell_loss"] = result["sell_loss"]
                        buy_dict["sell_profit"] = result["sell_profit"]
                        buy_dict["sell_prior"] = trade_strategy_package.sell_prior
                        sell_routine = threading.Thread(target=sell_thread, args=(buy_dict,))
                        sell_routine.start()
                    lock.acquire()
                    GLOBAL_SET.add(symbol)
                    lock.release()
                else:
                    lock.acquire()
                    GLOBAL_SET.remove(symbol)
                    lock.release()
                return True
        try_count += 1
    if lock.locked():
        lock.release()
    return False


def is_market_timing(
    minute: int, second: int, hour: int, trade_strategy_list: List[TradeStrategyPackage]
) -> List[TradeStrategyPackage]:
    result = []
    for trade_strategy in trade_strategy_list:
        if minute in trade_strategy.market_timing:
            if second >= 0:
                result.append(trade_strategy)
    return result


def buy_thread(buy_rate: float = 0.5, trade_strategy_list: List[TradeStrategyPackage] = []):
    global GLOBAL_BUY_DICT
    global GLOBAL_VERBOSE
    global GLOBAL_TRADE_STOCK_COUNT
    global GLOBAL_TRADE_OBJECT
    global GLOBAL_MAX_BUY_COUNT
    global GLOBAL_SET

    print("BUY_THREAD START")
    while True:
        if not GLOBAL_BUY_DICT["flag"]:
            try:
                current_time = GLOBAL_TRADE_OBJECT.get_current_time()
                if GLOBAL_VERBOSE:
                    print("[CURRENT-TIME] {}".format(current_time))
                    symbols = GLOBAL_TRADE_OBJECT.get_top_symbol_list(GLOBAL_TRADE_STOCK_COUNT, "acc_trade_price_24h")
                    symbols = symbols[:60]
                    for symbol in symbols:
                        tradeable, extra_data, trade_strategy_package = is_tradeable_check(
                            symbol, verbose=GLOBAL_VERBOSE, trade_strategy_list=trade_strategy_list, delay=10
                        )
                        if tradeable:
                            if GLOBAL_VERBOSE:
                                print("BUY SYMBOL : {}".format(symbol))
                        lock.acquire()
                        if symbol not in GLOBAL_SET and len(GLOBAL_SET) < GLOBAL_MAX_BUY_COUNT:
                            lock.release()
                            trade_routine = threading.Thread(
                                target=execute_trade,
                                args=(symbol, "BUY"),
                                kwargs={
                                    "buy_rate": buy_rate,
                                    "extra_data": extra_data,
                                    "trade_strategy_package": trade_strategy_package,
                                },
                            )
                            trade_routine.start()
                        if lock.locked():
                            lock.release()

            except Exception as e:
                print("[CRITICAL] EXCEPTION!! {}".format(e))
                if lock.locked():
                    lock.release()
        time.sleep(1)


def sell_thread(buy_dict):
    global GLOBAL_VERBOSE
    global GLOBAL_TRADE_OBJECT
    global GLOBAL_SET
    current_time = GLOBAL_TRADE_OBJECT.get_current_time()
    print(
        "[{current_time}] {symbol} / sell_profit : {sell_profit} / sell_loss : {sell_loss} - SELL_THREAD START".format(
            current_time=current_time,
            symbol=buy_dict["symbol"],
            sell_profit=buy_dict["sell_profit"],
            sell_loss=buy_dict["sell_loss"],
        )
    )
    sell_count = 0
    order_price_entry = ["621.0", "620.0", "619.0", "623.0", "620.0"]

    while True:
        try:
            symbol_balance = GLOBAL_TRADE_OBJECT.get_balance(symbol=buy_dict["symbol"])
            order_unit = ""
            if isinstance(symbol_balance, float):
                order_unit = "{:.4f}".format(symbol_balance)
            else:
                raise ValueError("API Server has Error!")
            order_unit = "0.0001"
            if order_unit == "0.0000":
                print(
                    "[{current_time}] {symbol} - ALREADY SELLED".format(
                        current_time=current_time, symbol=buy_dict["symbol"]
                    )
                )
                lock.acquire()
                GLOBAL_SET.remove(buy_dict["symbol"])
                lock.release()
                break

            order_price = float(order_price_entry[sell_count])
            sell_count = (sell_count + 1) % len(order_price_entry)
            current_price = float(GLOBAL_TRADE_OBJECT.get_coin_price(symbol=buy_dict["symbol"]))
            profit_rate = (current_price - order_price) / order_price
            if GLOBAL_VERBOSE:
                print(
                    "[SELL_THREAD] [{symbol}] ORDER_PRICE : {order_price} -> CURRENT_PRICE : {current_price} / profit_rate : {profit_rate} / sell_loss: {sell_loss}".format(
                        symbol=buy_dict["symbol"],
                        order_price=order_price,
                        current_price=current_price,
                        profit_rate=profit_rate,
                        sell_loss=buy_dict["sell_loss"],
                    )
                )

            if profit_rate / 0.001 > 0:
                new_sell_loss = 0.0
                if profit_rate >= buy_dict["sell_profit"]:
                    new_sell_loss = buy_dict["sell_profit"] * 0.8
                    buy_dict["sell_profit"] = buy_dict["sell_profit"] * 1.2
                else:
                    loss_price = GLOBAL_TRADE_OBJECT.get_coin_price(buy_dict["symbol"], prior=buy_dict["sell_prior"])
                    new_sell_loss = (loss_price - order_price) / order_price
                    new_sell_loss = max(new_sell_loss, 0.0005)

                if new_sell_loss != buy_dict["sell_loss"]:
                    before_sell_loss = buy_dict["sell_loss"]
                    buy_dict["sell_loss"] = max(buy_dict["sell_loss"], new_sell_loss)
                    if buy_dict["sell_loss"] > before_sell_loss:
                        print(
                            "[SELL-THREAD ({symbol}]) RESET SELL-LOSS : {sell_loss}".format(
                                symbol=buy_dict["symbol"], sell_loss=buy_dict["sell_loss"]
                            )
                        )

            left_hour = (datetime.now() - current_time).seconds // 3600
            if left_hour >= 18 and profit_rate > 0.0:
                print("[{symbol}] 18 HOUR LEFT. SELL ANYWAY".format(symbol=buy_dict["symbol"]))
                execute_trade(buy_dict["symbol"], "SELL", sell_unit=order_unit)

            if profit_rate <= buy_dict["sell_loss"]:
                if execute_trade(buy_dict["symbol"], "SELL", sell_unit=order_unit):
                    break

        except Exception as e:
            print("[CRITICAL] EXCEPTION!! {}".format(e))
        time.sleep(1)
    if lock.locked():
        lock.release()
    print("[{current_time}] {symbol} - SELL_THREAD END".format(current_time=current_time, symbol=buy_dict["symbol"]))


api_key = ""
secret = ""

with open("api-key.txt", "r") as f:
    line = f.readlines()
    api_key = str(line[0]).strip()
    secret = str(line[1]).strip()

default_sell_profit = 0.020
default_sell_loss = -0.015
GLOBAL_TRADE_OBJECT = UpbitTrade(access_key=api_key, secret=secret)
GLOBAL_BUY_DICT = {
    "flag": False,
    "symbol": "",
    "order_id": "",
    "sell_profit": default_sell_profit,
    "sell_loss": default_sell_loss,
    "default_sell_loss": default_sell_loss,
    "default_sell_profit": default_sell_profit,
}
GLOBAL_VERBOSE = True
GLOBAL_TRADE_STOCK_COUNT = 40
GLOBAL_MAX_TRY_COUNT = 30
GLOBAL_MAX_BUY_COUNT = 3
GLOBAL_SET = set()
buy_rate = 0.5
lock = threading.Lock()  # Create a lock object

trade_strategy_list = []

# set TradeStrategyPackage

extra_column = ["sell_loss", "sell_profit", "min_price", "close"]
basic_extra_column = ["sell_loss", "sell_profit", "close", "open"]


def _calc_sell_loss_profit(order_price, extra_data, sell_loss, sell_profit):
    result = {"sell_loss": sell_loss, "sell_profit": sell_profit}
    result["sell_loss"] = (extra_data["min_price"] - order_price) / order_price
    sell_diff_rate = (order_price - extra_data["close"]) / extra_data["close"]
    result["sell_profit"] = abs(result["sell_loss"]) * 2.0 - sell_diff_rate
    return result


def _calc_basic_sell_loss_profit(order_price, extra_data, sell_loss, sell_profit):
    print(order_price, extra_data["open"])
    result = {"sell_loss": sell_loss, "sell_profit": sell_profit}
    result["sell_loss"] = -0.0225
    result["sell_profit"] = 0.01

    return result


package1 = TradeStrategyPackage(
    strategy_function=ema_swing_low_function,
    strategy_options={"ema_period": 20, "period": 48, "big_stockdata": None},
    strategy_function_name="ema_swing_low_function_short1",
    extra_function=_calc_sell_loss_profit,
    market_timing=[0, 30],
    extra_column=extra_column,
)

package2 = TradeStrategyPackage(
    strategy_function=ema_swing_low_function,
    strategy_function_name="ema_swing_low_function",
    strategy_options={"big_stockdata": None},
    extra_function=_calc_sell_loss_profit,
    market_timing=[0, 30],
    extra_column=extra_column,
)

package3 = TradeStrategyPackage(
    strategy_function=ema_swing_low_function,
    strategy_function_name="ema_swing_low_function2",
    strategy_options={"ema_period": 20, "rate_threshold": 0.01, "big_stockdata": None},
    extra_function=_calc_sell_loss_profit,
    market_timing=[0, 30],
    extra_column=extra_column,
)

package4 = TradeStrategyPackage(
    strategy_function=candle_stick_pattern,
    strategy_function_name="candle_stick_pattern",
    extra_function=_calc_basic_sell_loss_profit,
    extra_column=basic_extra_column,
    min_observe_time_idx=-1,
    max_observe_time_idx=None,
    buy_bid_ask="ask",
    market_timing=[0, 10, 20, 30, 40, 50],
    sell_prior=3,
)

trade_strategy_list.append(package4)
# trade_strategy_list.append(package1)
# trade_strategy_list.append(package2)
# trade_strategy_list.append(package3)

# Create the threads
buy_routine = threading.Thread(target=buy_thread, args=(buy_rate, trade_strategy_list))

# Start the threads
buy_routine.start()

# Wait for the threads to finish (this will never happen in this case)
buy_routine.join()
