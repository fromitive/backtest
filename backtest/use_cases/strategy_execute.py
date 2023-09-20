from typing import List

from backtest.domains.stockdata import StockData
from backtest.domains.strategy import Strategy, StrategyExecuteFlagType
from backtest.domains.backtest_plot_package import BacktestPlotPackage
from backtest.domains.strategy_result import StrategyResult, StrategyResultColumnType
from backtest.module_compet.pandas import pd
from backtest.response import ResponseFailure, ResponseSuccess, ResponseTypes
from ta.momentum import RSIIndicator
from backtest.util.custom_indicator import trendilo, twin_range_filter
from scipy.stats import linregress
import copy
import numpy as np
from backtest.module_compet.talib import talib
from scipy.signal import argrelextrema


def basic_function(data: StockData, weight: int, name: str):
    response = pd.DataFrame(index=data.data.index, columns=[name])
    response[name] = [(StrategyResultColumnType.KEEP, weight)] * len(data)
    return response


def _calc_max_min_df(df: pd.DataFrame):
    df["min_price"] = df["close"].iloc[0]
    df["max_price"] = df["close"].iloc[1]
    min_price = df["close"].iloc[0]
    max_price = df["close"].iloc[1]
    for pos in range(2, len(df)):
        current_price = df["close"].iat[pos]
        if min(min_price, current_price) != min_price:
            min_price = current_price
            max_price = current_price
        else:
            max_price = max(max_price, current_price)
        df["min_price"].iat[pos] = min_price
        df["max_price"].iat[pos] = max_price
    return df


def swing_search(data: StockData, weight: int, name: str, support_df: pd.DataFrame = None):
    df = data.data

    if support_df is not None:
        df["volume_rank"] = support_df[data.symbol]

    # calc_signal1
    df["ema_100"] = talib.EMA(df["close"], timeperiod=100)
    df["ema_200"] = talib.EMA(df["close"], timeperiod=200)
    df["ema_400"] = talib.EMA(df["close"], timeperiod=400)
    df["ema_reverse_align_signal"] = df.apply(
        lambda r: r["ema_100"] < r["ema_200"] and r["ema_200"] < r["ema_400"], axis=1
    )

    # calc_signal2
    df["ema_5"] = talib.EMA(df["close"], timeperiod=5)
    df["ema_golden_closs_signal"] = df.apply(lambda r: r["close"] > r["ema_5"], axis=1)

    # calc_signal3
    def _rolling_signal(r):
        return r.any()

    df["up_raw_signal"] = (df["close"] >= df["ema_200"] * 0.98) & (df["close"] <= df["ema_200"] * 1.02)
    df["up_signal"] = df["up_raw_signal"].rolling(window=5).apply(_rolling_signal)

    # calc_signal4
    upper, middle, lower = talib.BBANDS(df["close"], 40, 2.0, 2.0)
    df["bb_upper"] = upper
    df["bb_raw_signal"] = df["close"] > df["bb_upper"]
    df["bb_signal"] = df["bb_raw_signal"].rolling(window=10).apply(_rolling_signal)

    # calc_siganal5
    df["High_52"] = df["high"].rolling(window=52).max()
    df["Low_52"] = df["low"].rolling(window=52).min()
    df["Senkou_span_B"] = ((df["High_52"] + df["Low_52"]) / 2).shift(26)
    df["ichimoku_signal"] = df["close"] > df["Senkou_span_B"]

    df["sell_loss"] = -0.05
    df["sell_profit"] = 0.2

    def _build_result(r: pd.Series):
        if (
            r["ema_reverse_align_signal"]
            and r["ema_golden_closs_signal"]
            and r["up_signal"] > 0.0
            and r["bb_signal"] > 0.0
            and r["ichimoku_signal"]
        ):
            return (StrategyResultColumnType.BUY, weight)

        return (StrategyResultColumnType.KEEP, 0)

    df[name] = df.apply(lambda r: _build_result(r), axis=1)

    buy_buffer = {"count": -1, "idx": ""}
    for count, idx in enumerate(df.index):
        strategy, _ = df.at[idx, name]
        # buy signal meet
        if strategy == StrategyResultColumnType.BUY:
            if buy_buffer["count"] < 0 and buy_buffer["idx"] == "":
                buy_buffer["count"] = count
                buy_buffer["idx"] = idx
            else:
                df.at[idx, name] = (StrategyResultColumnType.KEEP, 0)

        if buy_buffer["count"] >= 0 and buy_buffer["idx"] != "":
            buy_buffer_idx = buy_buffer["idx"]
            sell_loss = df.at[buy_buffer_idx, "sell_loss"]
            sell_profit = df.at[buy_buffer_idx, "sell_profit"]
            profit_rate = (df.at[idx, "high"] - df.at[buy_buffer_idx, "close"]) / df.at[
                buy_buffer_idx, "close"
            ]  # calc profit_rate
            if profit_rate >= sell_profit or profit_rate <= sell_loss or profit_rate > 0.10:
                if support_df is None:
                    pass
                    # print("{symbol},{buy_date},{sell_date},{profit_rate},{volume_level_rate},volume_obstacle:{volume_obstacle}".format(symbol=data.symbol, buy_date=buy_buffer_idx, sell_date=idx, profit_rate=profit_rate, volume_level_rate=df.at[buy_buffer_idx, 'volume_level_rate'], volume_obstacle=df.at[buy_buffer_idx, 'volume_obstacle']))
                else:
                    pass
                    # print("{symbol},{buy_date},{buy_rank},{sell_date},{sell_rank},{profit_rate},{volume_level_rate},volume_obstacle:{volume_obstacle}".format(symbol=data.symbol, buy_date=buy_buffer_idx, buy_rank=df.at[buy_buffer_idx, 'volume_rank'], sell_date=idx, sell_rank=df.at[idx, 'volume_rank'], profit_rate=profit_rate, volume_level_rate=df.at[buy_buffer_idx, 'volume_level_rate'], volume_obstacle=df.at[buy_buffer_idx, 'volume_obstacle']))
                if idx != buy_buffer_idx:  # for alarm
                    df.at[idx, name] = (StrategyResultColumnType.SELL, weight)
                buy_buffer = {"count": -1, "idx": ""}  # init buy_buffer
    result_column = [name, "close", "ema_200", "ema_400", "ema_5", "Senkou_span_B", "sell_loss", "sell_profit"]

    return df[result_column]


def finally_fib(
    data: StockData,
    weight: int,
    name: str,
    window: int = 300,
    fib_signal_list: List[int] = [0.786, 0.618, 0.5, 0.382],
    vp_range: int = 100,
    support_df: pd.DataFrame = None,
):
    df = data.data

    if support_df is not None:
        df["volume_rank"] = support_df[data.symbol]

    df["ema_200"] = talib.EMA(df["close"], timeperiod=200)
    df["local_min"] = df["low"].rolling(window=window).min()
    df["local_min_signal"] = df.apply(lambda r: r["local_min"] == r["low"], axis=1)
    df["local_max"] = df["local_min"]

    max_value = df["high"].iat[0]
    for idx in range(window, len(df)):
        if df["local_min_signal"].iat[idx]:
            max_value = df["high"].iat[idx]
        max_value = max(max_value, df["high"].iat[idx])
        df["local_max"].iat[idx] = max_value

    for fib_sig in fib_signal_list:
        df["fib_sig_{}".format(fib_sig)] = df["local_max"] * (1 - fib_sig) + df["local_min"] * fib_sig

    # calculate buy signal
    df["buy_signal"] = False
    df["sell_loss_price"] = df["local_min"]
    for idx in range(window, len(df)):
        if (
            df["ema_200"].iat[idx] < df["high"].iat[idx]
            and df["close"].iat[idx] > df["open"].iat[idx]
            and df["high"].iat[idx] < df["local_max"].iat[idx]
        ):
            for fib_sig in fib_signal_list:
                if (
                    df["low"].iat[idx] < df["fib_sig_{}".format(fib_sig)].iat[idx]
                    and df["close"].iat[idx] > df["fib_sig_{}".format(fib_sig)].iat[idx]
                ):
                    df["buy_signal"].iat[idx] = True
                    if fib_sig != fib_signal_list[0]:
                        df["sell_loss_price"].iat[idx] = df[
                            "fib_sig_{}".format(fib_signal_list[fib_signal_list.index(fib_sig) - 1])
                        ].iat[idx]
                    else:
                        df["sell_loss_price"].iat[idx] = df["local_min"].iat[idx]
                    break

    # calcluate volume profile
    df["volume_level_rate"] = 0.0
    df["volume_obstacle"] = False

    close_buffer = []

    def get_rank(volume, current_index):
        ser = pd.Series(volume)
        ranks = ser.rank().tolist()
        return ranks[current_index]

    for idx in range(window, len(df)):
        if df["buy_signal"].iat[idx]:
            pass
        if df["local_min_signal"].iat[idx]:
            # init close_buffer
            close_buffer = []
        local_min = df["local_min"].iat[idx]
        local_max = df["local_max"].iat[idx]
        volume = df["volume"].iat[idx]
        close = df["close"].iat[idx]
        close_buffer.append((close, volume))
        volume_profile_dict = {round(level, 1): 0.0 for level in np.linspace(local_min, local_max, vp_range)}
        volume_keys = list(volume_profile_dict.keys())
        current_level = 0
        for c, v in close_buffer:
            volume_level_index = 0
            for i in range(len(volume_keys) - 1):
                if c >= volume_keys[i] and c <= volume_keys[i + 1]:
                    volume_profile_dict[volume_keys[i]] += v
                    volume_level_index = i
            if c == close_buffer[-1][0]:
                current_level = volume_level_index
        lower_price_volume = [volume_profile_dict[key] for key in volume_keys[: current_level + 1]]
        upper_price_volume = [volume_profile_dict[key] for key in volume_keys[current_level + 1 :]]
        entire_price_volume = [volume_profile_dict[key] for key in volume_keys]
        df["volume_level_rate"].iat[idx] = get_rank(entire_price_volume, current_level) / vp_range * 100.0
        df["volume_obstacle"].iat[idx] = sum(lower_price_volume) < sum(upper_price_volume)

    df["sell_loss"] = (df["sell_loss_price"] - df["close"]) / df["close"] * 0.125
    df["sell_profit"] = df["sell_loss"].abs() * 2.0

    def _build_result(r: pd.Series):
        if r["buy_signal"]:
            return (StrategyResultColumnType.BUY, weight)

        return (StrategyResultColumnType.KEEP, 0)

    df[name] = df.apply(lambda r: _build_result(r), axis=1)

    buy_buffer = {"count": -1, "idx": ""}
    for count, idx in enumerate(df.index):
        strategy, _ = df.at[idx, name]
        # buy signal meet
        if strategy == StrategyResultColumnType.BUY:
            if buy_buffer["count"] < 0 and buy_buffer["idx"] == "":
                buy_buffer["count"] = count
                buy_buffer["idx"] = idx
            else:
                df.at[idx, name] = (StrategyResultColumnType.KEEP, 0)

        if buy_buffer["count"] >= 0 and buy_buffer["idx"] != "":
            buy_buffer_idx = buy_buffer["idx"]
            sell_loss = df.at[buy_buffer_idx, "sell_loss"]
            sell_profit = df.at[buy_buffer_idx, "sell_profit"]
            profit_rate = (df.at[idx, "high"] - df.at[buy_buffer_idx, "close"]) / df.at[
                buy_buffer_idx, "close"
            ]  # calc profit_rate
            if profit_rate >= sell_profit or profit_rate <= sell_loss or profit_rate > 0.10:
                if support_df is None:
                    pass
                    # print("{symbol},{buy_date},{sell_date},{profit_rate},{volume_level_rate},volume_obstacle:{volume_obstacle}".format(symbol=data.symbol, buy_date=buy_buffer_idx, sell_date=idx, profit_rate=profit_rate, volume_level_rate=df.at[buy_buffer_idx, 'volume_level_rate'], volume_obstacle=df.at[buy_buffer_idx, 'volume_obstacle']))
                else:
                    pass
                    # print("{symbol},{buy_date},{buy_rank},{sell_date},{sell_rank},{profit_rate},{volume_level_rate},volume_obstacle:{volume_obstacle}".format(symbol=data.symbol, buy_date=buy_buffer_idx, buy_rank=df.at[buy_buffer_idx, 'volume_rank'], sell_date=idx, sell_rank=df.at[idx, 'volume_rank'], profit_rate=profit_rate, volume_level_rate=df.at[buy_buffer_idx, 'volume_level_rate'], volume_obstacle=df.at[buy_buffer_idx, 'volume_obstacle']))
                if idx != buy_buffer_idx:  # for alarm
                    df.at[idx, name] = (StrategyResultColumnType.SELL, weight)
                buy_buffer = {"count": -1, "idx": ""}  # init buy_buffer
    result_column = [name, "local_min", "local_max", "close", "ema_200", "volume_level_rate", "volume_obstacle"]

    for fib_sig in fib_signal_list:
        result_column.append("fib_sig_{}".format(fib_sig))

    return df[result_column]


def candle_stick_pattern(data: StockData, weight: int, name: str, big_stockdata: StockData = None, rolling: int = 72):
    df = data.data

    def _calc_volume_signal(r):
        before = r[: len(r) - 1]
        return np.sum(before) < r[-1]

    df["volume_signal"] = df["volume"].rolling(window=rolling).apply(_calc_volume_signal)
    df["ema_20"] = talib.EMA(df["close"], timeperiod=20)
    df["ema_50"] = talib.EMA(df["close"], timeperiod=50)
    df["ema_100"] = talib.EMA(df["close"], timeperiod=100)
    df["ema_200"] = talib.EMA(df["close"], timeperiod=200)

    def _calc_big_stock_buy_signal(r):
        return r.all()

    def _calc_buy_signal(r: pd.Series):
        if r["volume_signal"] > 0 and r["close"] > r["open"]:
            for i in [20, 50, 100, 200]:
                if r["close"] < r["ema_{}".format(i)]:
                    return False
            return True
        return False

    df["buy_signal"] = df.apply(lambda r: _calc_buy_signal(r), axis=1)
    df["second_volume"] = df[df["buy_signal"] is True]["volume"]
    df["second_volume"] = df["second_volume"].fillna(method="ffill")
    df["second_signal"] = df.apply(
        lambda r: (r["volume"] / r["second_volume"]) > 0.5 and r["open"] < r["close"], axis=1
    )

    df["buy_signal_shift_1"] = df["buy_signal"].shift(1)
    df["close_shift_1"] = df["close"].shift(1)
    df["high_shift_minus_1"] = df["high"].shift(-1)
    if big_stockdata is not None:
        df["real_result"] = df.apply(
            lambda r: r["high_shift_minus_1"] > r["close"] and r["buy_signal"] and r["big_stock_buy_signal"] > 0.0,
            axis=1,
        )
    else:
        df["real_result"] = df.apply(lambda r: r["high_shift_minus_1"] > r["close"] and r["buy_signal"], axis=1)
    df["sell_loss"] = -0.025
    df["sell_profit"] = 0.01

    # debug
    # real_result = df[df['real_result'] is True].index
    # if len(real_result) > 0:
    #    for item in real_result[::-1]:
    #        print('[DEBUG] {symbol} - success_index {index}'.format(symbol=data.symbol, index=item))
    #   print('total success : {} / {}'.format(len(real_result), len(df[df['buy_signal'] is True])))

    # second_signal = df[df['second_signal'] is True].index
    # if len(second_signal):
    #    for item in second_signal[::-1]:
    #        print('[DEBUG] {symbol} - second_signal! {index}'.format(symbol=data.symbol, index=item))

    def _build_result(r: pd.Series):
        if big_stockdata:
            if (r["buy_signal"] or r["second_signal"]) and r["big_stock_buy_signal"] > 0.0:
                return (StrategyResultColumnType.BUY, weight)
            elif r["buy_signal_shift_1"]:
                return (StrategyResultColumnType.SELL, weight)
        else:
            if r["buy_signal"] or r["second_signal"]:
                return (StrategyResultColumnType.BUY, weight)
            elif r["buy_signal_shift_1"]:
                return (StrategyResultColumnType.SELL, weight)
        return (StrategyResultColumnType.KEEP, 0)

    df[name] = df.apply(lambda r: _build_result(r), axis=1)

    return df[[name, "sell_loss", "sell_profit", "close", "open"]]


def ema_swing_another_function(
    data: StockData,
    weight: int,
    name: str,
    watch_ema: int = 50,
    target_ema: int = 100,
    support_df: pd.DataFrame = None,
    big_stockdata: StockData = None,
):
    df = data.data
    df["ema_20"] = talib.EMA(df["close"], timeperiod=20)
    df["ema_50"] = talib.EMA(df["close"], timeperiod=50)
    df["ema_100"] = talib.EMA(df["close"], timeperiod=100)
    df["ema_200"] = talib.EMA(df["close"], timeperiod=200)

    df["reverse_array"] = df.apply(lambda r: r["ema_200"] > r["ema_100"] > r["ema_50"] > r["ema_20"], axis=1)
    df["buy_signal"] = False
    signal1 = False
    for idx in range(20, len(df)):
        if df["reverse_array"].iat[idx]:
            signal1 = True
        else:
            if signal1:
                if df["ema_{}".format(watch_ema)].iat[idx] > df["ema_{}".format(target_ema)].iat[idx]:
                    df["buy_signal"].iat[idx] = True
                    signal1 = False

    df["buy_signal_shift_1"] = df["buy_signal"].shift(1)

    def _build_result(r: pd.Series):
        if big_stockdata is not None:
            if r["buy_signal_shift_1"] is True and r["big_stock_buy_signal"] > 0:
                return (StrategyResultColumnType.BUY, weight)
            else:
                return (StrategyResultColumnType.KEEP, 0)
        else:
            if r["buy_signal_shift_1"] is True:
                return (StrategyResultColumnType.BUY, weight)
            else:
                return (StrategyResultColumnType.KEEP, 0)

    df[name] = df.apply(lambda r: _build_result(r), axis=1)

    df["min_price"] = df["low"].rolling(window=20).min()
    df["sell_loss"] = (df["min_price"] - df["close"]) / df["close"]
    df["sell_profit"] = df["sell_loss"].abs() * 2.0
    buy_buffer = {"count": -1, "idx": ""}
    for count, idx in enumerate(df.index):
        strategy, _ = df.at[idx, name]
        # buy signal meet
        if strategy == StrategyResultColumnType.BUY:
            if buy_buffer["count"] < 0 and buy_buffer["idx"] == "":
                buy_buffer["count"] = count
                buy_buffer["idx"] = idx
            else:
                df.at[idx, name] = (StrategyResultColumnType.KEEP, 0)

        if buy_buffer["count"] >= 0 and buy_buffer["idx"] != "":
            buy_buffer_idx = buy_buffer["idx"]
            sell_loss = df.at[buy_buffer_idx, "sell_loss"]
            sell_profit = df.at[buy_buffer_idx, "sell_profit"]
            profit_rate = (df.at[idx, "high"] - df.at[buy_buffer_idx, "close"]) / df.at[
                buy_buffer_idx, "close"
            ]  # calc profit_rate
            if profit_rate >= sell_profit or profit_rate <= sell_loss:
                if support_df is None:
                    print(
                        "{symbol},{buy_date},{sell_date},{profit_rate}".format(
                            symbol=data.symbol, buy_date=buy_buffer_idx, sell_date=idx, profit_rate=profit_rate
                        )
                    )
                else:
                    print(
                        "{symbol},{buy_date},{buy_rank},{sell_date},{sell_rank},{profit_rate}".format(
                            symbol=data.symbol,
                            buy_date=buy_buffer_idx,
                            buy_rank=df.at[buy_buffer_idx, "volume_rank"],
                            sell_date=idx,
                            sell_rank=df.at[idx, "volume_rank"],
                            profit_rate=profit_rate,
                        )
                    )
                df.at[idx, name] = (StrategyResultColumnType.SELL, weight)
                buy_buffer = {"count": -1, "idx": ""}  # init buy_buffer
    return df[[name]]


def ema_swing_low_function(
    data: StockData,
    weight: int,
    name: str,
    ema_period: int = 50,
    rate_threshold: float = 0.01,
    support_df: pd.DataFrame = None,
    period: int = 100,
    big_stockdata: StockData = None,
):
    df = data.data
    # df['volume_rank'] = support_stock_df[data.symbol]

    if support_df is not None:
        df["volume_rank"] = support_df[data.symbol]

    df["ema"] = talib.EMA(df["close"], timeperiod=ema_period)

    local_minima_indices = argrelextrema(df["ema"].values, np.less, order=period)
    local_maxima_indices = argrelextrema(df["ema"].values, np.greater, order=period)
    df["local_minima"] = False
    df["local_maxima"] = False
    df.loc[df.index[local_minima_indices], "local_minima"] = True
    df.loc[df.index[local_maxima_indices], "local_maxima"] = True

    signal1 = False
    df["buy_signal"] = False

    def _calc_another_local_minima(r):
        item = r[:-1]
        return len(item[item is True]) > 0

    df["another_local_minima"] = df["local_minima"].rolling(period * 2).apply(_calc_another_local_minima)
    for idx in range(period, len(df)):
        if df["local_maxima"].iat[idx]:
            signal1 = True
            maxima_close = df["close"].iat[idx]
        if signal1:
            if df["local_minima"].iat[idx]:
                minima_close = df["close"].iat[idx]
                rate = abs((maxima_close - minima_close) / maxima_close)
                if rate > rate_threshold:
                    if df["another_local_minima"].iat[idx]:
                        df["buy_signal"].iat[idx] = True
                        signal1 = False

    df["buy_signal_shift_1"] = df["buy_signal"].shift(1)

    def _calc_big_stock_buy_signal(r):
        return r.all()

    if big_stockdata is not None:
        big_stock_df = big_stockdata.data
        big_stock_df["big_ema"] = talib.EMA(big_stock_df["close"], timeperiod=20)
        big_stock_df["big_stock_buy_signal"] = big_stock_df.apply(lambda r: r["close"] > r["big_ema"], axis=1).shift(1)
        big_stock_df["big_stock_buy_signal"] = (
            big_stock_df["big_stock_buy_signal"].rolling(window=5).apply(_calc_big_stock_buy_signal)
        )

        if big_stockdata.unit == "D" and data.unit == "M":
            big_stock_df.index = pd.to_datetime(big_stock_df.index)
            big_stock_df = big_stock_df.resample("T").fillna(method="ffill")
            big_stock_df = big_stock_df.reindex(data.data.index).fillna(method="ffill")
        df["big_stock_buy_signal"] = big_stock_df["big_stock_buy_signal"]

    def _build_result(r: pd.Series):
        if big_stockdata is not None:
            if r["buy_signal_shift_1"] is True and r["big_stock_buy_signal"] > 0:
                return (StrategyResultColumnType.BUY, weight)
            else:
                return (StrategyResultColumnType.KEEP, 0)
        else:
            if r["buy_signal_shift_1"] is True:
                return (StrategyResultColumnType.BUY, weight)
            else:
                return (StrategyResultColumnType.KEEP, 0)

    df[name] = df.apply(lambda r: _build_result(r), axis=1)

    buy_buffer = {"count": -1, "idx": ""}
    # debug
    if len(df[df["buy_signal_shift_1"] is True].index) > 0:
        if big_stockdata is not None:
            index_data = df[(df["buy_signal_shift_1"] is True) & (df["big_stock_buy_signal"] is True)].index
            if len(index_data) > 0:
                print("[DEBUG] {symbol} - last index {index}".format(symbol=data.symbol, index=index_data[-1]))
        else:
            print(
                "[DEBUG] {symbol} - last index {index}".format(
                    symbol=data.symbol, index=df[df["buy_signal_shift_1"] is True].index[-1]
                )
            )

    df["min_price"] = df["low"].rolling(window=20).min()
    df["sell_loss"] = (df["min_price"] - df["close"]) / df["close"]
    df["sell_profit"] = df["sell_loss"].abs() * 2.0
    for count, idx in enumerate(df.index):
        strategy, _ = df.at[idx, name]
        # buy signal meet
        if strategy == StrategyResultColumnType.BUY:
            if buy_buffer["count"] < 0 and buy_buffer["idx"] == "":
                buy_buffer["count"] = count
                buy_buffer["idx"] = idx

        if buy_buffer["count"] >= 0 and buy_buffer["idx"] != "":
            buy_buffer_idx = buy_buffer["idx"]
            sell_loss = df.at[buy_buffer_idx, "sell_loss"]
            sell_profit = df.at[buy_buffer_idx, "sell_profit"]
            profit_rate = (df.at[idx, "high"] - df.at[buy_buffer_idx, "close"]) / df.at[
                buy_buffer_idx, "close"
            ]  # calc profit_rate
            if profit_rate >= sell_profit or profit_rate <= sell_loss:
                if support_df is None:
                    print(
                        "{symbol},{buy_date},{sell_date},{profit_rate}".format(
                            symbol=data.symbol, buy_date=buy_buffer_idx, sell_date=idx, profit_rate=profit_rate
                        )
                    )
                else:
                    print(
                        "{symbol},{buy_date},{buy_rank},{sell_date},{sell_rank},{profit_rate}".format(
                            symbol=data.symbol,
                            buy_date=buy_buffer_idx,
                            buy_rank=df.at[buy_buffer_idx, "volume_rank"],
                            sell_date=idx,
                            sell_rank=df.at[idx, "volume_rank"],
                            profit_rate=profit_rate,
                        )
                    )
                df.at[idx, name] = (StrategyResultColumnType.SELL, weight)
                buy_buffer = {"count": -1, "idx": ""}  # init buy_buffer

    return df[[name, "sell_loss", "sell_profit", "min_price", "close"]]


def ema_local_min_max_trandline_function(
    data: StockData,
    weight: int,
    name: str,
    support_df: pd.DataFrame = None,
    shift: int = 5,
    big_stock: StockData = None,
):
    df = data.data
    if support_df is not None:
        df["volume_rank"] = support_df[data.symbol]
    # df['volume_rank'] = support_stock_df[data.symbol]
    df["ema_200"] = talib.EMA(df["close"], timeperiod=200)
    if big_stock is not None:
        big_stock_df = big_stock.data
        big_stock_df["ema_long"] = talib.EMA(df["close"], timeperiod=200)
        big_stock_df["ema_short"] = talib.EMA(df["close"], timeperiod=20)

        def calc_lin(y):
            x = np.arange(len(y)) + 1
            slope, intercept, r_value, p_value, std_err = linregress(x=x[:-1], y=y[:-1])
            return slope

        big_stock_df["ema_slope_long"] = big_stock_df["ema_long"].rolling(window=3).apply(calc_lin)
        big_stock_df["ema_slope_short"] = big_stock_df["ema_short"].rolling(window=3).apply(calc_lin)

        if big_stock.unit == "D" and data.unit == "M":
            big_stock_df.index = pd.to_datetime(big_stock_df.index)
            big_stock_df = big_stock_df.resample("T").fillna(method="ffill")
            big_stock_df = big_stock_df.reindex(data.data.index).fillna(method="ffill")
        df["ema_slope_long"] = big_stock_df["ema_slope_long"]
        df["ema_slope_short"] = big_stock_df["ema_slope_short"]
        df["ema_big_long"] = big_stock_df["ema_long"]
        df["ema_big_short"] = big_stock_df["ema_short"]

    period = 100
    local_minima_indices = argrelextrema(df["ema_200"].values, np.less, order=period)
    local_maxima_indices = argrelextrema(df["ema_200"].values, np.greater, order=period)
    df["local_minima"] = False
    df["local_maxima"] = False
    df.loc[df.index[local_minima_indices], "local_minima"] = True
    df.loc[df.index[local_maxima_indices], "local_maxima"] = True

    df["local_max_value"] = None
    df["local_min_value"] = None
    df.loc[df.index[local_minima_indices], "local_min_value"] = df.loc[df.index[local_minima_indices], "ema_200"]
    df.loc[df.index[local_maxima_indices], "local_max_value"] = df.loc[df.index[local_maxima_indices], "ema_200"]
    df["local_max_value"] = df["local_max_value"].ffill()
    df["local_min_value"] = df["local_min_value"].ffill()

    signal1 = False
    ema_200 = df[df["ema_200"] != np.nan]["ema_200"].values[-1]
    rate_threshold = 0.01
    df["buy_signal"] = False
    for idx in range(len(df)):
        if df["local_maxima"].iat[idx]:
            signal1 = True
            ema_200 = df["ema_200"].iat[idx]
        if signal1:
            if df["local_minima"].iat[idx]:
                local_minima_ema = df["ema_200"].iat[idx]
                rate = (ema_200 - local_minima_ema) / ema_200
                if rate > rate_threshold:
                    df["buy_signal"].iat[idx] = True
                    signal1 = False

    df["buy_signal_shift"] = df["buy_signal"].shift(shift)
    df["ema_200_shift"] = df["ema_200"].shift(shift)
    df["ema_slope"] = (df["ema_200"] - df["ema_200_shift"]) / shift

    df["ema_slope_change"] = df["ema_slope"] - df["ema_slope"].shift(1)
    df["ema_slope_change_ma_short"] = df["ema_slope_change"].rolling(window=shift).mean()
    df["ema_slope_change_ma_long"] = df["ema_slope_change"].rolling(window=shift * 5).mean()

    def _build_result(r: pd.Series):
        if r["buy_signal_shift"] is True and r["ema_slope_short"] > 0 and r["ema_slope_long"] > 0:
            return (StrategyResultColumnType.BUY, weight)
        else:
            return (StrategyResultColumnType.KEEP, 0)

    df[name] = df.apply(lambda r: _build_result(r), axis=1)

    buy_buffer = {"count": -1, "idx": ""}
    # debug
    if len(df[df["buy_signal_shift"] is True].index) > 0:
        print(
            "[DEBUG] {symbol} - last index {index}".format(
                symbol=data.symbol, index=df[df["buy_signal_shift"] is True].index[-1]
            )
        )

    df["min_price"] = df["low"].rolling(window=20).min()
    df["sell_loss"] = (df["min_price"] - df["close"]) / df["close"]
    df["sell_profit"] = df["sell_loss"].abs() * 2.0

    for count, idx in enumerate(df.index):
        strategy, _ = df.at[idx, name]
        # buy signal meet
        if strategy == StrategyResultColumnType.BUY:
            if buy_buffer["count"] < 0 and buy_buffer["idx"] == "":
                buy_buffer["count"] = count
                buy_buffer["idx"] = idx

        if buy_buffer["count"] >= 0 and buy_buffer["idx"] != "":
            buy_buffer_idx = buy_buffer["idx"]
            sell_loss = df.at[buy_buffer_idx, "sell_loss"]
            sell_profit = df.at[buy_buffer_idx, "sell_profit"]
            profit_rate = (df.at[idx, "high"] - df.at[buy_buffer_idx, "close"]) / df.at[
                buy_buffer_idx, "close"
            ]  # calc profit_rate
            if profit_rate >= sell_profit or profit_rate <= sell_loss:
                if support_df is None:
                    print(
                        "{symbol},{buy_date},{sell_date},{profit_rate},{ema_slope_change},{ema_slope_change_ma_short},{ema_slope_change_ma_long}".format(
                            symbol=data.symbol,
                            buy_date=buy_buffer_idx,
                            sell_date=idx,
                            profit_rate=profit_rate,
                            ema_slope_change=df.at[buy_buffer_idx, "ema_slope_change"],
                            ema_slope_change_ma_short=df.at[buy_buffer_idx, "ema_slope_change_ma_short"],
                            ema_slope_change_ma_long=df.at[buy_buffer_idx, "ema_slope_change_ma_long"],
                        )
                    )
                else:
                    print(
                        "{symbol},{buy_date},{buy_rank},{sell_date},{sell_rank},{profit_rate},{ema_slope_change}".format(
                            symbol=data.symbol,
                            buy_date=buy_buffer_idx,
                            buy_rank=df.at[buy_buffer_idx, "volume_rank"],
                            sell_date=idx,
                            sell_rank=df.at[idx, "volume_rank"],
                            profit_rate=profit_rate,
                            ema_slope_change=df.at[buy_buffer_idx, "ema_slope_change"],
                        )
                    )
                df.at[idx, name] = (StrategyResultColumnType.SELL, weight)
                buy_buffer = {"count": -1, "idx": ""}  # init buy_buffer

    return df[[name, "sell_loss", "sell_profit", "min_price", "close"]]


def trendilo_mix_function(data: StockData, weight: int, name: str, big_stock: StockData):
    df = data.data
    big_df = big_stock.data
    big_df[["avpch", "rms"]] = trendilo(big_df)
    big_df["trading_time"] = big_df.apply(lambda r: r["avpch"] > r["rms"], axis=1)

    if big_stock.unit == "D" and data.unit == "M":
        big_df.index = pd.to_datetime(big_df.index)
        big_df = big_df.resample("T").fillna(method="ffill")
        big_df = big_df.reindex(data.data.index).fillna(method="ffill")
    df["trading_time"] = big_df["trading_time"].shift(1)
    df[["avpch", "rms"]] = trendilo(df)
    df[["long", "short"]] = twin_range_filter(df)

    df["trendilo_signal"] = False
    trendilo_signal1 = False
    for idx in range(1, len(df)):
        # signal1: positive rms not occur
        positive_rms_signal = df["rms"].iat[idx]
        negative_rms_signal = -df["rms"].iat[idx]
        current_avpch = df["avpch"].iat[idx]
        if positive_rms_signal < current_avpch:
            trendilo_signal1 = False
        elif negative_rms_signal > current_avpch:
            trendilo_signal1 = True
        if trendilo_signal1:  # trendilo_signal1 is True
            if current_avpch > 0.0:
                df["trendilo_signal"].iat[idx] = True

    # Compute Volume Oscillator
    short_term = 5
    long_term = 20
    short_mavg = df["volume"].rolling(window=short_term).mean()
    long_mavg = df["volume"].rolling(window=long_term).mean()

    def custom_function(x):
        print(type(x))

    # Compute Volume Oscillator
    df["vol_osc"] = (short_mavg - long_mavg) / long_mavg * 100

    df["vol_osc_trading_time"] = df.apply(lambda r: r["vol_osc"] > 0.0, axis=1)

    df["ema_20"] = talib.EMA(df["close"], timeperiod=20)
    df["ema_200"] = talib.EMA(df["close"], timeperiod=200)
    df["ema_200_trading_time"] = df.apply(lambda r: r["ema_20"] > r["ema_200"], axis=1)

    df["buy_signal"] = df.apply(
        lambda r: r["ema_200_trading_time"] and r["vol_osc_trading_time"] and r["long"] and r["trendilo_signal"], axis=1
    )

    pass


def pivot_strategy(
    data: StockData, weight: int, name: str, big_stock: StockData, pivot_days: int = 7, local_minima_gap: int = 3
):
    df = data.data
    big_df = big_stock.data

    # caclulate pivot point
    big_df["n_days_high"] = big_df["high"].rolling(pivot_days).max()
    big_df["n_days_low"] = big_df["low"].rolling(pivot_days).min()
    big_df["n_days_close"] = big_df["close"].rolling(pivot_days).mean()

    big_df["pivot_point"] = big_df.apply(lambda r: (r["n_days_high"] + r["n_days_low"] + r["n_days_close"]) / 3, axis=1)
    big_df["support1"] = big_df.apply(lambda r: r["pivot_point"] * 2 - r["n_days_high"], axis=1)
    big_df["resistance1"] = big_df.apply(lambda r: r["pivot_point"] * 2 - r["n_days_low"], axis=1)
    big_df["support2"] = big_df.apply(lambda r: r["pivot_point"] - (r["resistance1"] - r["support1"]), axis=1)
    big_df["resistance2"] = big_df.apply(lambda r: r["pivot_point"] - (r["support1"] - r["resistance1"]), axis=1)

    big_df["big_ema_200"] = talib.EMA(big_df["close"], timeperiod=200)
    big_df["big_ema_200_shift_1"] = big_df["big_ema_200"].shift(1)
    big_df["close_shift_1"] = big_df["close"].shift(1)

    def _calc_trading(r: pd.Series):
        if r["close_shift_1"] > r["big_ema_200_shift_1"]:
            return True
        return False

    big_df["trading_time"] = big_df.apply(lambda r: _calc_trading(r), axis=1)

    # sync 30minutes, daily
    if big_stock.unit == "D" and data.unit == "M":
        big_df.index = pd.to_datetime(big_df.index)
        big_df = big_df.resample("T").fillna(method="ffill")
        big_df = big_df.reindex(data.data.index).fillna(method="ffill")

    df["trading_time"] = big_df["trading_time"]
    df["pivot_point"] = big_df["pivot_point"]
    df["support1"] = big_df["support1"]
    df["support2"] = big_df["support2"]
    df["resistance1"] = big_df["resistance1"]
    df["resistance2"] = big_df["resistance2"]

    # calculate area
    def _calc_area(r: pd.Series, column_name: str) -> int:
        if r[column_name] < r["resistance2"] and r[column_name] >= r["resistance1"]:
            return 4
        elif r[column_name] < r["resistance1"] and r[column_name] >= r["pivot_point"]:
            return 3
        elif r[column_name] < r["pivot_point"] and r[column_name] >= r["support1"]:
            return 2
        elif r[column_name] < r["support1"] and r[column_name] >= r["support2"]:
            return 1
        else:
            return 0

    df["close_area"] = df.apply(lambda r: _calc_area(r, "close"), axis=1).astype(int)
    df["low_area"] = df.apply(lambda r: _calc_area(r, "low"), axis=1).astype(int)
    df["high_area"] = df.apply(lambda r: _calc_area(r, "high"), axis=1).astype(int)

    # clacluate area movement
    def _calc_area_movement(r: pd.Series) -> str:
        if r["close_area"] - r["low_area"] > 0.0:
            return "UP"
        elif r["close_area"] - r["high_area"] < 0.0:
            return "DOWN"
        else:
            return "NOT_MOVE"

    df["area_movement"] = df.apply(lambda r: _calc_area_movement(r), axis=1)

    # cacluate local minima data
    df["low_min"] = df["low"].rolling(local_minima_gap).min()
    df["close_min"] = df["close"].rolling(local_minima_gap).min()

    df["is_low_min"] = df.apply(lambda r: r["low_min"] == r["low"], axis=1)
    df["is_close_min"] = df.apply(lambda r: r["close_min"] == r["close_min"], axis=1)
    df["totally_local_minima"] = df.apply(lambda r: r["is_low_min"] and r["is_close_min"], axis=1)
    df["previous_local_minima"] = df["totally_local_minima"].shift(1)
    df["sell_loss"] = -0.015

    def _calc_take_profit(r: pd.Series):
        if r["close_area"] >= 4.0:
            return (r["resistance2"] - r["close"]) / r["close"]
        elif r["close_area"] >= 3.0:
            return (r["resistance1"] - r["close"]) / r["close"]
        elif r["close_area"] >= 2.0:
            return (r["pivot_point"] - r["close"]) / r["close"]
        else:
            return (r["support1"] - r["close"]) / r["close"]

    df["take_profit"] = df.apply(lambda r: min(0.15, _calc_take_profit(r)), axis=1)
    # calc buy_sell signal
    signal1 = (False, df.index[0])  # find 'UP' signal and there price
    df["buy_signal"] = False

    for idx in range(1, len(df)):
        if df["pivot_point"].iat[idx] != df["pivot_point"].iat[idx - 1]:
            signal1 = (False, df.index[idx])
        else:
            if df["area_movement"].iat[idx] == "DOWN":
                signal1 = (False, df.index[idx])
            elif df["area_movement"].iat[idx] == "UP":
                signal1 = (True, df.index[idx])

            if signal1[0]:
                if (
                    df.index[idx] != signal1[1]
                    and df["previous_local_minima"].iat[idx]
                    and df["close"].iat[idx] > df["close"].iat[idx - 1]
                ):
                    if df["area_movement"].iat[idx] == "NOT_MOVE" or df["area_movement"].iat[idx] == "UP":
                        df["buy_signal"].iat[idx] = True
                        sell_loss_rate = (df.at[signal1[1], "low"] - df["close"].iat[idx]) / df["close"].iat[idx]
                        df["sell_loss"].iat[idx] = sell_loss_rate

    df["ema_200"] = talib.EMA(df["close"], timeperiod=200)
    df["ema_20"] = talib.EMA(df["close"], timeperiod=20)
    df["ema_signal"] = df.apply(lambda r: r["ema_200"] < r["close"] and r["ema_200"] < r["ema_20"], axis=1)

    def _build_result(r: pd.Series):
        if r["buy_signal"] and r["ema_signal"] and r["trading_time"]:
            return (StrategyResultColumnType.BUY, weight)
        else:
            return (StrategyResultColumnType.KEEP, 0)

    df[name] = df.apply(lambda r: _build_result(r), axis=1)

    buy_buffer = {"count": -1, "idx": ""}

    for count, idx in enumerate(df.index):
        strategy, _ = df.at[idx, name]
        # buy signal meet
        if strategy == StrategyResultColumnType.BUY:
            if buy_buffer["count"] < 0 and buy_buffer["idx"] == "":
                buy_buffer["count"] = count
                buy_buffer["idx"] = idx

        if buy_buffer["count"] >= 0 and buy_buffer["idx"] != "":
            buy_buffer_idx = buy_buffer["idx"]
            sell_profit = df.at[buy_buffer_idx, "take_profit"]
            sell_loss = df.at[buy_buffer_idx, "sell_loss"]
            profit_rate = (df.at[idx, "high"] - df.at[buy_buffer_idx, "close"]) / df.at[
                buy_buffer_idx, "close"
            ]  # calc profit_rate
            if profit_rate >= sell_profit or profit_rate <= sell_loss:
                df.at[idx, name] = (StrategyResultColumnType.SELL, weight)
                buy_buffer = {"count": -1, "idx": ""}  # init buy_buffer

    return df[[name, "take_profit", "sell_loss", "close"]]


def close_fibonacci(data: StockData, weight: int, name: str, big_stock: StockData):
    df = data.data
    _calc_max_min_df(df)


def ema_fibonacci(data: StockData, weight: int, name: str, big_stock: StockData, sell_profit: float = 1.0):
    df = data.data
    big_df = big_stock.data
    big_df["big_ema_200"] = talib.EMA(big_df["close"], timeperiod=200)
    big_df["big_ema_200_shift_1"] = big_df["big_ema_200"].shift(1)
    big_df["close_shift_1"] = big_df["close"].shift(1)

    def _calc_trading(r: pd.Series):
        if r["close_shift_1"] > r["big_ema_200_shift_1"]:
            return True
        return False

    big_df["trading_time"] = big_df.apply(lambda r: _calc_trading(r), axis=1)

    if big_stock.unit == "D" and data.unit == "M":
        big_df.index = pd.to_datetime(big_df.index)
        big_df = big_df.resample("T").fillna(method="ffill")
        big_df = big_df.reindex(data.data.index).fillna(method="ffill")

    df["trading_time"] = big_df["trading_time"]

    df["max_rolling"] = df["high"].rolling(10).max()

    def _calc_not_maxima(r: pd.Series):
        if r.max_rolling == r.high:
            return False
        return True

    df["not_max"] = df.apply(lambda r: _calc_not_maxima(r), axis=1)

    df["ema_20"] = talib.EMA(df["close"], timeperiod=20)
    df["ema_50"] = talib.EMA(df["close"], timeperiod=50)
    df["ema_100"] = talib.EMA(df["close"], timeperiod=100)
    df["ema_200"] = talib.EMA(df["close"], timeperiod=200)

    for i in [20, 50, 100, 200]:
        df["ema_{}_shift1".format(i)] = df["ema_{}".format(i)].shift(1)

    df["ema_up_fib_bound1"] = df["ema_20"] * 0.786 + df["ema_200"] * (1 - 0.786)
    df["ema_up_fib_bound2"] = df["ema_20"] * 0.618 + df["ema_200"] * (1 - 0.618)
    df["ema_up_fib_bound3"] = df["ema_20"] * 0.5 + df["ema_200"] * (1 - 0.5)
    df["ema_up_fib_bound4"] = df["ema_20"] * 0.372 + df["ema_200"] * (1 - 0.372)
    df["ema_up_fib_bound5"] = df["ema_20"] * 0.236 + df["ema_200"] * (1 - 0.236)
    df["RSI"] = talib.RSI(df["close"], timeperiod=14)
    df["fastk"], df["fastd"] = talib.STOCH(
        df["RSI"], df["RSI"], df["RSI"], fastk_period=14, slowk_period=3, slowk_matype=0, slowd_period=3, slowd_matype=0
    )
    df["fastk_shift1"] = df["fastk"].shift(1)
    df["fastd_shift1"] = df["fastd"].shift(1)

    df["volume_ema"] = talib.EMA(df["volume"], timeperiod=200)

    def _calc_volume_ema(r: pd.Series):
        if r["volume_ema"] <= r["volume"]:
            return True
        return False

    df["volume_signal"] = df.apply(lambda r: _calc_volume_ema(r), axis=1)

    def _calc_rsi_signal(r: pd.Series):
        if r.fastk_shift1 < 20.0 and r.fastd_shift1 < 20.0 and r.fastk >= 20.0:
            if r.fastk > r.fastd and r.fastk > r.fastk_shift1 and r.fastd > r.fastd_shift1:
                return True
        return False

    df["rsi_signal"] = df.apply(lambda r: _calc_rsi_signal(r), axis=1)

    def _up_signal(r: pd.Series):
        if r.open < r.close:
            return True
        return False

    df["up_signal"] = df.apply(lambda r: _up_signal(r), axis=1)

    def _calc_fib_rate(r: pd.Series):
        boundary_count = 5
        boundary_bucket = {i: 0 for i in range(1, boundary_count + 1)}
        for value in [20, 50, 100, 200]:
            if r["ema_{}".format(value)] - r["ema_{}_shift1".format(value)] <= 0:
                return False
        if (r["ema_20"] - r["ema_200"]) / r["ema_20"] < 0.01:
            return False
        if r["ema_50"] > r["ema_100"] and r["ema_100"] > r["ema_200"]:
            for value in [50, 100, 200]:
                for i in range(1, boundary_count):
                    if (
                        r["ema_up_fib_bound{bound_count}".format(bound_count=i)] <= r["ema_{value}".format(value=value)]
                        and r["ema_{value}".format(value=value)]
                        >= r["ema_up_fib_bound{bound_count}".format(bound_count=i + 1)]
                    ):
                        boundary_bucket[i] += 1
            for i in boundary_bucket:
                if boundary_bucket[i] >= 2:
                    return False
            return True
        return False

    df["fib_rate"] = df.apply(lambda r: _calc_fib_rate(r), axis=1)

    def _build_result(r: pd.Series):
        if r["rsi_signal"] and r["fib_rate"] and r["trading_time"] and r["not_max"] and r["up_signal"]:
            return (StrategyResultColumnType.BUY, weight)
        return (StrategyResultColumnType.KEEP, 0)

    df[name] = df.apply(lambda r: _build_result(r), axis=1)

    # add sell strategy
    buy_buffer = {"count": -1, "idx": ""}

    for count, idx in enumerate(df.index):
        strategy, _ = df.at[idx, name]
        # buy signal meet
        if strategy == StrategyResultColumnType.BUY:
            if buy_buffer["count"] < 0 and buy_buffer["idx"] == "":
                buy_buffer["count"] = count
                buy_buffer["idx"] = idx

        if buy_buffer["count"] >= 0 and buy_buffer["idx"] != "":
            buy_buffer_idx = buy_buffer["idx"]
            sell_loss = (df.at[buy_buffer_idx, "low"] - df.at[buy_buffer_idx, "close"]) / df.at[buy_buffer_idx, "close"]
            for ema in [20, 50, 100, 200]:
                if df.at[buy_buffer_idx, "ema_{}".format(ema)] < df.at[buy_buffer_idx, "low"]:
                    sell_loss = (df.at[buy_buffer_idx, "ema_{}".format(ema)] - df.at[buy_buffer_idx, "close"]) / df.at[
                        buy_buffer_idx, "close"
                    ]
                    break
            sell_profit = min(abs(sell_loss) * 1.5, 0.02)
            profit_rate = (df.at[idx, "high"] - df.at[buy_buffer_idx, "close"]) / df.at[
                buy_buffer_idx, "close"
            ]  # calc profit_rate
            if profit_rate >= sell_profit or profit_rate <= sell_loss:
                df.at[idx, name] = (StrategyResultColumnType.SELL, weight)
                buy_buffer = {"count": -1, "idx": ""}  # init buy_buffer

    return df[[name, "ema_20", "ema_50", "ema_100", "ema_200", "low"]]


def stocastic_rsi_ema_mix_function(
    data: StockData,
    weight: int,
    name: str,
    timeperiod: int = 200,
    rsi_period: int = 14,
    fastk_period=3,
    fastd_period=3,
    fastd_matype=0,
    buy_rate: float = 20.0,
    sell_profit: float = 0.0225,
    sell_loss: float = -0.015,
    heikin_ashi: dict = {},
    compare_movement: int = 3,
):
    def _calculate_heikin_ashi(df, open, high, low, close):
        ha_close = (df[open] + df[high] + df[low] + df[close]) / 4
        ha_open = (df[open].shift(1) + df[close].shift(1)) / 2
        ha_high = df[[high, open, close]].max(axis=1)
        ha_low = df[[low, open, close]].min(axis=1)

        df["ha_close"] = ha_close
        df["ha_open"] = ha_open
        df["ha_high"] = ha_high
        df["ha_low"] = ha_low
        df["mov"] = np.where(df["ha_close"] > df["ha_open"], "Up", "Down")
        return df

    df = data.data
    df = _calculate_heikin_ashi(df, "open", "high", "low", "close")
    # argrelextrema will find the indices of relative minimums of a 1-D array
    local_minima_indices = argrelextrema(df["low"].values, np.less)

    # Initialize a new column with False values
    df["last_local_min_index"] = None

    # Set the last_local_min_index value to the index for local minima
    df.loc[df.index[local_minima_indices], "last_local_min_index"] = df.index[local_minima_indices]
    df["last_local_min_index"] = df["last_local_min_index"].ffill()
    df["last_local_min_index"].fillna(df.index[0], inplace=True)

    df["ema"] = talib.EMA(df["close"], timeperiod=timeperiod)
    df["RSI"] = talib.RSI(df["close"], timeperiod=rsi_period)
    df["RSI_shift_1"] = df["RSI"].shift(1)
    df["fastk"], df["fastd"] = talib.STOCH(
        df["RSI"], df["RSI"], df["RSI"], fastk_period=14, slowk_period=3, slowk_matype=0, slowd_period=3, slowd_matype=0
    )
    df["before_fastk"] = df["fastk"].shift(1)
    df["before_fastd"] = df["fastd"].shift(1)
    df["local_min_fastk"] = df["fastk"].loc[df["last_local_min_index"]].values
    df["local_min_fastd"] = df["fastd"].loc[df["last_local_min_index"]].values
    df["ha_close_shift_1"] = df["ha_close"].shift(1)
    df["ha_open_shift_1"] = df["ha_open"].shift(1)
    df["ha_low_shift_1"] = df["ha_low"].shift(1)

    def _buy_signal(r: pd.Series):
        if r.close > r.ema:  # buy condition 1
            # buy condition 2
            if r.RSI >= 50.0 and r.RSI_shift_1:
                # buy condition3
                if (
                    r.fastk > r.fastd
                    and r.fastk > buy_rate
                    and r.fastk < 80.0
                    and r.fastd > buy_rate
                    and r.fastd < 80.0
                    and r.fastk > r.before_fastk
                    and r.fastd > r.before_fastd
                ):
                    if r.before_fastk - r.before_fastd < r.fastk - r.fastd:
                        # buy_condition 4
                        before_candle_length = r.ha_close_shift_1 - r.ha_open_shift_1
                        candle_length = r.ha_close - r.ha_open
                        if (
                            r.ha_open == r.ha_low
                            and r.ha_open_shift_1 == r.ha_low_shift_1
                            and candle_length > 0
                            and before_candle_length > 0
                            and candle_length > before_candle_length
                        ):
                            return (StrategyResultColumnType.BUY, weight)
        return (StrategyResultColumnType.KEEP, 0)

    df["result"] = df.apply(lambda r: _buy_signal(r), axis=1)

    # add sell strategy
    buy_buffer = {"count": -1, "idx": ""}

    for count, idx in enumerate(df.index):
        strategy, _ = df.at[idx, "result"]
        # buy signal meet
        if strategy == StrategyResultColumnType.BUY:
            if buy_buffer["count"] < 0 and buy_buffer["idx"] == "":
                buy_buffer["count"] = count
                buy_buffer["idx"] = idx
            else:  # buy_buffer already exist
                if abs(count - buy_buffer["count"]) < 7:
                    df.at[idx, "result"] = (StrategyResultColumnType.KEEP, 0)
                else:
                    df.at[idx, "result"] = (StrategyResultColumnType.SELL, weight)
                    if idx != df.index[-1]:
                        df["result"].iat[count + 1] = (StrategyResultColumnType.BUY, weight)
                    buy_buffer = {"count": -1, "idx": ""}  # init buy_buffer

        if buy_buffer["count"] >= 0 and buy_buffer["idx"] != "":
            buy_buffer_idx = buy_buffer["idx"]
            last_local_min_index = df["last_local_min_index"].loc[buy_buffer_idx]
            local_min_loss = (df.at[last_local_min_index, "low"] - df.at[buy_buffer_idx, "close"]) / df.at[
                buy_buffer_idx, "close"
            ]
            local_min_profit = abs(local_min_loss) * 1.5
            sell_profit = sell_profit if sell_profit <= local_min_profit else local_min_profit
            sell_loss = sell_loss if sell_loss >= local_min_loss else local_min_loss
            profit_rate = (df.at[idx, "high"] - df.at[buy_buffer_idx, "close"]) / df.at[
                buy_buffer_idx, "close"
            ]  # calc profit_rate
            if profit_rate >= sell_profit or profit_rate <= sell_loss:
                df.at[idx, "result"] = (StrategyResultColumnType.SELL, weight)
                buy_buffer = {"count": -1, "idx": ""}  # init buy_buffer

    df[name] = df["result"]
    return df[[name, "ema", "fastk", "fastd"]]


def min_max_function(
    data: StockData,
    weight: int,
    name: str,
    avg_rolling: int = 7,
    avg_vol_rate: float = 2.0,
    high_low_diff_rate: float = 0.10,
):
    response = pd.DataFrame(index=data.data.index, columns=[name])
    temp_df = data.data.copy()
    temp_df["avg_vol"] = temp_df["volume"].rolling(avg_rolling).mean()

    def _min_max_function(r):
        current_avg_vol_rate = r.volume / r.avg_vol
        current_high_low_rate = r.high - r.low
        if current_avg_vol_rate >= avg_vol_rate and current_high_low_rate >= high_low_diff_rate:
            return (StrategyResultColumnType.BUY, weight)
        else:
            return (StrategyResultColumnType.KEEP, 0)

    temp_df["result"] = temp_df.apply(lambda r: _min_max_function(r), axis=1)

    response[name] = temp_df["result"]
    return response[["avg_vol", name]]


def _dataframe_sma(df: pd.DataFrame, weight: int, rolling=100):
    df["sma"] = df["close"].rolling(rolling).mean().fillna(0)
    df["smashift"] = df["sma"].shift(1).fillna(0)

    def _sma_internal(r):
        if (r.smashift - r.sma) > 0.0:
            return (StrategyResultColumnType.SELL, weight)
        elif (r.smashift - r.sma) == 0.0:
            return (StrategyResultColumnType.KEEP, weight)
        else:
            return (StrategyResultColumnType.BUY, weight)

    df["result"] = df.apply(lambda r: _sma_internal(r), axis=1)
    return df[["sma", "smashift", "result"]]


def _dataframe_sma_multi(df: pd.DataFrame, weight: int, rolling_list: List[int]):
    for rolling in rolling_list:
        df["sma_{}".format(rolling)] = df["close"].rolling(rolling).mean().fillna(0)

    def _sma_internal(r):
        if all([(r.close - r["sma_{}".format(rolling)]) > 0.0 for rolling in rolling_list]):
            return (StrategyResultColumnType.SELL, weight)
        elif all([(r.close - r["sma_{}".format(rolling)]) < 0.0 for rolling in rolling_list]):
            return (StrategyResultColumnType.BUY, weight)
        else:
            return (StrategyResultColumnType.KEEP, weight)

    df["result"] = df.apply(lambda r: _sma_internal(r), axis=1)
    column_list = ["sma_{}".format(rolling) for rolling in rolling_list]
    column_list.append("result")
    return df[column_list]


def sma_function(data: StockData, weight: int, name: str, rolling=100):
    response = pd.DataFrame(index=data.data.index, columns=[name])
    """
    strategyfunction here
    """
    response = _dataframe_sma(data.data, weight, rolling)
    response = response.rename({"result": name}, axis=1)
    return response


def buy_rate_function(data: StockData, weight: int, name: str, buy_rolling: int = 30, buy_rate: float = 0.5):
    temp_df = data.data.copy()
    temp_df["buy_rolling"] = temp_df["high"].rolling(buy_rolling).max()
    """
    strategyfunction here
    """

    def _buy_rate(r: pd.Series):
        if pd.isna(r.buy_rolling) or r.buy_rolling == 0.0:
            return (StrategyResultColumnType.KEEP, 0)
        else:
            current_buy_rate = r.close / r.buy_rolling
            if current_buy_rate <= buy_rate:
                return (StrategyResultColumnType.BUY, weight)
        return (StrategyResultColumnType.KEEP, 0)

    temp_df[name] = temp_df.apply(lambda r: _buy_rate(r), axis=1)
    return temp_df[[name]]


def sell_rate_function(
    data: StockData, weight: int, name: str, sell_rolling: int = 30, sell_rate: float = 0.5, base: str = "top"
):
    temp_df = data.data.copy()
    if base.lower() == "bottom":
        temp_df["sell_rolling"] = temp_df["low"].rolling(sell_rolling).min()
    else:
        temp_df["sell_rolling"] = temp_df["high"].rolling(sell_rolling).max()
    """
    strategyfunction here
    """

    def _sell_rate(r: pd.Series):
        if pd.isna(r.sell_rolling) or r.sell_rolling == 0.0:
            return (StrategyResultColumnType.KEEP, 0)
        else:
            current_sell_rate = r.close / r.sell_rolling
            if current_sell_rate >= sell_rate:
                return (StrategyResultColumnType.SELL, weight)
        return (StrategyResultColumnType.KEEP, 0)

    temp_df[name] = temp_df.apply(lambda r: _sell_rate(r), axis=1)
    return temp_df[[name]]


def sell_rate_custom_function(
    data: StockData, weight: int, name: str, sell_rolling: int = 30, sell_rate: float = 0.5, keep_weight: float = -1
):
    temp_df = data.data.copy()
    temp_df["sell_rolling"] = temp_df["low"].rolling(sell_rolling).min()
    """
    strategyfunction here
    """

    def _sell_rate(r: pd.Series):
        if pd.isna(r.sell_rolling) or r.sell_rolling == 0.0:
            return (StrategyResultColumnType.KEEP, 0)
        else:
            maximum_price = r["high"]
            sell_rolling = r["sell_rolling"]
            if sell_rolling * sell_rate <= maximum_price:
                return (StrategyResultColumnType.SELL, weight)
        if keep_weight < 0:
            return (StrategyResultColumnType.KEEP, weight)
        else:
            return (StrategyResultColumnType.KEEP, keep_weight)

    temp_df[name] = temp_df.apply(lambda r: _sell_rate(r), axis=1)
    return temp_df[[name]]


def buy_rate_custom_function(
    data: StockData, weight: int, name: str, buy_rolling: int = 30, buy_rate: float = 0.5, keep_weight: float = -1
):
    temp_df = data.data.copy()
    temp_df["buy_rolling"] = temp_df["high"].rolling(buy_rolling).max()
    """
    strategyfunction here
    """

    def _buy_rate(r: pd.Series):
        if pd.isna(r.buy_rolling) or r.buy_rolling == 0.0:
            return (StrategyResultColumnType.KEEP, 0)
        else:
            minimum_price = r["low"]
            buy_rolling = r["buy_rolling"]
            if buy_rolling * buy_rate >= minimum_price:
                return (StrategyResultColumnType.BUY, weight)
        # keep weight calculate
        if keep_weight < 0:
            return (StrategyResultColumnType.KEEP, weight)
        else:
            return (StrategyResultColumnType.KEEP, keep_weight)

    temp_df[name] = temp_df.apply(lambda r: _buy_rate(r), axis=1)
    return temp_df[[name]]


def sma_big_stock_function(data: StockData, weight: int, name: str, big_stock: StockData, rolling=100):
    response = pd.DataFrame(index=data.data.index, columns=[name])
    """
    strategyfunction here
    """
    response = _dataframe_sma(big_stock.data, weight, rolling)
    if big_stock.unit == "D" and data.unit == "M":
        response.index = pd.to_datetime(response.index)
        response = response.resample("T").fillna(method="bfill")
        response = response.reindex(data.data.index, method="ffill")

    response = response.rename({"result": name}, axis=1)
    return response


def sma_multi_function(data: StockData, weight: int, name: str, rolling_list: List[int] = [15, 100]):
    response = pd.DataFrame(index=data.data.index, columns=[name])
    """
    strategyfunction here
    """
    response = _dataframe_sma_multi(data.data, weight, rolling_list)
    response = response.rename({"result": name}, axis=1)
    return response


def sma_multi_big_stock_function(
    data: StockData, weight: int, name: str, big_stock: StockData, rolling_list: List[int] = [15, 100]
):
    response = pd.DataFrame(index=data.data.index, columns=[name])
    """
    strategyfunction here
    """
    response = _dataframe_sma_multi(big_stock.data, weight, rolling_list)
    if big_stock.unit == "D" and data.unit == "M":
        response.index = pd.to_datetime(response.index)
        response = response.resample("T").fillna(method="bfill")
        response = response.reindex(data.data.index)

    response = response.rename({"result": name}, axis=1)
    return response


def _calculate_rsi(data, period):
    rsi = RSIIndicator(close=data.data["close"], window=period)

    return rsi.rsi()


def rsi_function(
    data: StockData, weight: int, name: str, period: int, sell_score: int, buy_score: int, keep_weight: int = -1
):
    response = pd.DataFrame(index=data.data.index, columns=["rsi", name])
    response["rsi"] = _calculate_rsi(data, period)

    def _rsi_function(r):
        if r <= buy_score:
            return (StrategyResultColumnType.BUY, weight)
        elif r >= sell_score:
            return (StrategyResultColumnType.SELL, weight)
        else:
            if keep_weight == -1:
                return (StrategyResultColumnType.KEEP, weight)
            else:
                return (StrategyResultColumnType.KEEP, keep_weight)

    response[name] = response.apply(lambda r: _rsi_function(r["rsi"]), axis=1)
    return response[["rsi", name]]


def rsi_sma_diff_function(
    data: StockData,
    weight: int,
    name: str,
    rsi_period: int,
    sma_period: int,
    buy_rsi: float = 20.0,
    sell_rsi: float = 70.0,
    keep_weight: int = -1,
):
    response = pd.DataFrame(index=data.data.index, columns=["rsi", "sma", name])
    response["rsi"] = _calculate_rsi(data, rsi_period)
    response["sma"] = response["rsi"].rolling(sma_period).mean().fillna(0)
    response["sma_rsi_diff"] = response["sma"] - response["rsi"]
    response["sma_rsi_diff_only_plus"] = response["sma_rsi_diff"].apply(lambda r: 0.0 if r < 0 else r)
    response["sma_rsi_diff_diff_rsi"] = response["rsi"] - response["sma_rsi_diff_only_plus"]
    response["sma_diff_raw_data"] = response["sma_rsi_diff_only_plus"] - response["sma_rsi_diff_diff_rsi"]
    # sma_rsi_diff_only_plus - smi_rsi_diff_diff_rsi

    def _rsi_sma_diff_function(r):
        if r["sma_diff_raw_data"] > 0 and r["rsi"] <= buy_rsi:
            return (StrategyResultColumnType.BUY, weight)
        elif (r["rsi"] - (r["sma_diff_raw_data"] * (-1.0))) < 0.01 and r["rsi"] >= sell_rsi:
            return (StrategyResultColumnType.SELL, weight)
        else:
            return (StrategyResultColumnType.KEEP, 0)

    response[name] = response.apply(lambda r: _rsi_sma_diff_function(r), axis=1)
    return response[[name]]


def rsi_big_stock_function(
    data: StockData,
    weight: int,
    name: str,
    big_stock: StockData,
    period: int,
    sell_score: int,
    buy_score: int,
    keep_weight: int = -1,
):
    response = pd.DataFrame(index=data.data.index, columns=[name])
    if big_stock.unit == "D" and data.unit == "M":
        tmp_big_stock = big_stock.data.copy()
        tmp_big_stock.index = pd.to_datetime(tmp_big_stock.index)
        tmp_big_stock = tmp_big_stock.resample("T").interpolate()
        tmp_big_stock = tmp_big_stock.reindex(data.data.index, method="ffill")
        data.data["rsi"] = _calculate_rsi(tmp_big_stock.data["close"], period)
    else:
        data.data["rsi"] = _calculate_rsi(big_stock.data["close"], period)

    def _rsi_function(r):
        if r <= buy_score:
            return (StrategyResultColumnType.BUY, weight)
        elif r >= sell_score:
            return (StrategyResultColumnType.SELL, weight)
        else:
            if keep_weight == -1:
                return (StrategyResultColumnType.KEEP, weight)
            else:
                return (StrategyResultColumnType.KEEP, keep_weight)

    response[name] = data.data.apply(lambda r: _rsi_function(r["rsi"]), axis=1)
    return response[["rsi", name]]


def greed_fear_index_function(
    data: StockData, weight: int, name: str, greed_fear_index_data: pd.DataFrame, index_fear: int, index_greed: int
):
    temp_data = greed_fear_index_data.copy()
    if data.unit == "M":
        temp_data.index = pd.to_datetime(temp_data.index)
        temp_data = temp_data.resample("T").fillna(method="bfill")
        temp_data = temp_data.reindex(data.data.index, method="ffill")

    response = pd.DataFrame(index=data.data.index, columns=[name])
    raw_result = data.data.join(temp_data, how="inner")

    def _greed_fear_index(r):
        if (r["value"]) <= index_fear:  # extreme greed
            return (StrategyResultColumnType.BUY, weight)
        elif r["value"] >= index_greed:
            return (StrategyResultColumnType.SELL, weight)
        else:
            return (StrategyResultColumnType.KEEP, weight)

    response[name] = raw_result.apply(lambda r: _greed_fear_index(r), axis=1)
    return response


def _inner_strategy_execute(strategy: Strategy, data: StockData):
    try:
        if not strategy.function:
            strategy.function = basic_function
        response = strategy.function(data=data, weight=strategy.weight, name=strategy.name, **strategy.options)
        return ResponseSuccess(response)
    except Exception as e:
        return ResponseFailure(ResponseTypes.SYSTEM_ERROR, e)


def _sum_strategy(series: pd.Series, stockdata: StockData, weight_score_function):
    total_result = {StrategyResultColumnType.KEEP: 0, StrategyResultColumnType.SELL: 0, StrategyResultColumnType.BUY: 0}
    for idx in series.index:
        type, weight = series[idx]
        if stockdata.data["volume"][series.name] == 0.0:
            total_result[StrategyResultColumnType.KEEP] += weight
        else:
            total_result[type] += weight
    score_value = sorted(total_result.values(), reverse=True)
    strategy_rate = weight_score_function(first=score_value[0], second=score_value[1], third=score_value[2])
    return [max(total_result, key=total_result.get), strategy_rate]


def _inverse_strategy(row: pd.Series, name: str):
    if not isinstance(row[name], tuple):
        return (StrategyResultColumnType.KEEP, 0)
    col_type, weight = row[name]
    if col_type == StrategyResultColumnType.BUY:
        return (StrategyResultColumnType.SELL, weight)
    elif col_type == StrategyResultColumnType.SELL:
        return (StrategyResultColumnType.BUY, weight)
    else:
        return (StrategyResultColumnType.KEEP, weight)


def _sellonly_strategy(row: pd.Series, name: str):
    if not isinstance(row[name], tuple):
        return (StrategyResultColumnType.KEEP, 0)
    col_type, weight = row[name]
    if col_type == StrategyResultColumnType.BUY:
        return (StrategyResultColumnType.KEEP, 0)
    else:
        return (col_type, weight)


def _buyonly_strategy(row: pd.Series, name: str):
    if not isinstance(row[name], tuple):
        return (StrategyResultColumnType.KEEP, 0)
    col_type, weight = row[name]

    if col_type == StrategyResultColumnType.SELL:
        return (StrategyResultColumnType.KEEP, 0)
    else:
        return (col_type, weight)


def _basic_weight_score_function(first: int, second: int, third: int):
    return ((first + 1) * 2) / (1 + second + third)


def strategy_execute(
    strategy_list: List[Strategy],
    stockdata: StockData,
    save_strategy_result: bool = False,
    weight_score_function=_basic_weight_score_function,
    plot_package: BacktestPlotPackage = None,
):
    strategy_total_result = pd.DataFrame(index=stockdata.data.index)
    strategy_bucket = set()
    strategy_name_list = set([strategy.name for strategy in strategy_list])
    strategy_dict = {bucket_item: 1 for bucket_item in strategy_name_list}

    # create copied data for temporary used (calculate strategy result)
    copied_data = copy.deepcopy(stockdata)
    for strategy in strategy_list:
        if strategy.weight == 0:  # skip if strategy weight is zero
            continue
        if strategy.name in strategy_bucket:
            strategy_dict[strategy.name] += 1
            strategy.name = "{}_{}".format(strategy.name, strategy_dict[strategy.name])
        else:
            strategy_bucket.add(strategy.name)

        response = _inner_strategy_execute(strategy=strategy, data=copied_data)
        if isinstance(response, ResponseFailure):
            return ResponseFailure(ResponseTypes.SYSTEM_ERROR, "strategy function error occured!")
        else:
            strategy_result = response.value
            if strategy.inverse:
                strategy_result[strategy.name] = strategy_result.apply(
                    lambda row: _inverse_strategy(row, strategy.name), axis=1
                )
            if strategy.flag == StrategyExecuteFlagType.SELLONLY:
                strategy_result[strategy.name] = strategy_result.apply(
                    lambda row: _sellonly_strategy(row, strategy.name), axis=1
                )
            elif strategy.flag == StrategyExecuteFlagType.BUYONLY:
                strategy_result[strategy.name] = strategy_result.apply(
                    lambda row: _buyonly_strategy(row, strategy.name), axis=1
                )
            if plot_package:
                if stockdata.symbol not in plot_package.package_data_bucket.keys():
                    plot_package.package_data_bucket[stockdata.symbol] = []
                plot_package.package_data_bucket[stockdata.symbol].append({strategy.name: strategy_result})
            if len(stockdata) >= len(strategy_result):
                strategy_total_result = strategy_total_result.join(
                    strategy_result[[strategy.name]], how="left", rsuffix="{}_".format(strategy.name)
                )
            else:
                strategy_total_result = strategy_total_result.join(
                    strategy_result[[strategy.name]], how="inner", rsuffix="{}_".format(strategy.name)
                )
    # delete temporary data
    del copied_data
    # fill na with
    for column in strategy_total_result.columns:
        strategy_total_result[column] = strategy_total_result[column].fillna(
            {i: (StrategyResultColumnType.KEEP, 0) for i in strategy_total_result.index}
        )
    if save_strategy_result:
        strategy_total_result.to_csv("{}_strategy_total_result.csv".format(stockdata.symbol))
    return ResponseSuccess(
        StrategyResult(
            strategy_total_result.apply(lambda row: _sum_strategy(row, stockdata, weight_score_function), axis=1)
        )
    )
