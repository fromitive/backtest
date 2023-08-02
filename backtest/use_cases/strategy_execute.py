from typing import List

from backtest.domains.stockdata import StockData
from backtest.domains.strategy import Strategy, StrategyExecuteFlagType
from backtest.domains.backtest_plot_package import BacktestPlotPackage
from backtest.domains.strategy_result import (StrategyResult,
                                              StrategyResultColumnType)
from backtest.module_compet.pandas import pd
from backtest.response import ResponseFailure, ResponseSuccess, ResponseTypes
from ta.momentum import RSIIndicator
import copy
import numpy as np
import talib
from scipy.signal import argrelextrema

def basic_function(data: StockData, weight: int, name: str):
    response = pd.DataFrame(
        index=data.data.index, columns=[name])
    response[name] = [(
        StrategyResultColumnType.KEEP, weight)] * len(data)
    return response

def ema_fibonacci(data: StockData, weight: int, name: str, big_stock: StockData, sell_profit: float = 1.0):
    df = data.data
    big_df = big_stock.data
    big_df['big_ema_200'] = talib.EMA(big_df['close'], timeperiod=200)
    big_df['big_ema_200_shift_1'] = big_df['big_ema_200'].shift(1)
    big_df['close_shift_1'] = big_df['close'].shift(1)
    
    def _calc_trading(r:pd.Series):
        if r['close_shift_1'] > r['big_ema_200_shift_1']:
            return True
        return False
    
    big_df['trading_time'] = big_df.apply(lambda r:_calc_trading(r), axis=1)
    
    if big_stock.unit == 'D' and data.unit == 'M':
        big_df.index = pd.to_datetime(big_df.index)
        big_df = big_df.resample('T').fillna(method='bfill')
        big_df = big_df.reindex(data.data.index)
        
    df['trading_time'] = big_df['trading_time']
    
    df['max_rolling'] = df['high'].rolling(10).max()
    
    def _calc_not_maxima(r: pd.Series):
        if r.max_rolling == r.high:
            return False
        return True
    df['not_max'] = df.apply(lambda r: _calc_not_maxima(r), axis=1)
    
    df['ema_20'] = talib.EMA(df['close'], timeperiod=20)
    df['ema_50'] = talib.EMA(df['close'], timeperiod=50)
    df['ema_100'] = talib.EMA(df['close'], timeperiod=100)
    df['ema_200'] = talib.EMA(df['close'], timeperiod=200)
    
    for i in [20, 50, 100, 200]:
        df['ema_{}_shift1'.format(i)] = df['ema_{}'.format(i)].shift(1)
        
    df['ema_up_fib_bound1'] = df['ema_20'] * 0.786 + df['ema_200'] * (1 - 0.786)
    df['ema_up_fib_bound2'] = df['ema_20'] * 0.618 + df['ema_200'] * (1 - 0.618)
    df['ema_up_fib_bound3'] = df['ema_20'] * 0.5 + df['ema_200'] * (1 - 0.5)
    df['ema_up_fib_bound4'] = df['ema_20'] * 0.372 + df['ema_200'] * (1 - 0.372)
    df['ema_up_fib_bound5'] = df['ema_20'] * 0.236 + df['ema_200'] * (1 - 0.236)
    df['RSI'] = talib.RSI(df['close'], timeperiod=14)
    df['fastk'], df['fastd'] = talib.STOCH(df['RSI'], df['RSI'], df['RSI'], fastk_period=14, slowk_period=3, slowk_matype=0, slowd_period=3, slowd_matype=0)
    df['fastk_shift1'] = df['fastk'].shift(1)
    df['fastd_shift1'] = df['fastd'].shift(1)
    
    df['volume_ema'] = talib.EMA(df['volume'], timeperiod=200)
    
    def _calc_volume_ema(r:pd.Series):
        if r['volume_ema'] <= r['volume']:
            return True
        return False
    
    df['volume_signal'] = df.apply(lambda r: _calc_volume_ema(r), axis=1)
    
    def _calc_rsi_signal(r:pd.Series):
        if r.fastk < 20.0 and r.fastd < 20.0:
            if r.fastk > r.fastd and r.fastk > r.fastk_shift1 and r.fastd > r.fastd_shift1:
                return True
        return False
    
    df['rsi_signal'] = df.apply(lambda r: _calc_rsi_signal(r), axis=1)
    
    def _calc_fib_rate(r:pd.Series):
        boundary_count = 5
        boundary_bucket = {i: 0 for i in range(1, boundary_count + 1)}
        for value in [20,50,100,200]:
            if r['ema_{}'.format(value)] - r['ema_{}_shift1'.format(value)] <= 0:
                return False
        if (r['ema_20'] - r['ema_200']) / r['ema_20'] < 0.01:
            return False
        if r['ema_50'] > r['ema_100'] and r['ema_100'] > r['ema_200']:
            for value in [50, 100, 200]:
                for i in range(1, boundary_count):
                    if r['ema_up_fib_bound{bound_count}'.format(bound_count=i)] <= r['ema_{value}'.format(value=value)] and r['ema_{value}'.format(value=value)] >= r['ema_up_fib_bound{bound_count}'.format(bound_count=i + 1)]:
                        boundary_bucket[i] += 1
            for i in boundary_bucket:
                if boundary_bucket[i] >= 2:
                    return False
            return True
        return False
    df['fib_rate'] = df.apply(lambda r: _calc_fib_rate(r), axis=1)
    
    def _build_result(r:pd.Series):
        if r['rsi_signal'] and r['fib_rate'] and r['trading_time'] and r['not_max']:
            return (StrategyResultColumnType.BUY, weight)
        return (StrategyResultColumnType.KEEP, 0)
    
    df[name] = df.apply(lambda r: _build_result(r), axis = 1)
    
    # add sell strategy
    buy_buffer = {'count': -1, 'idx': ""}
    
    for count, idx in enumerate(df.index):
        strategy, _ = df.at[idx, name]
        # buy signal meet
        if strategy == StrategyResultColumnType.BUY:
            if buy_buffer['count'] < 0 and buy_buffer['idx'] == "":
                buy_buffer['count'] = count
                buy_buffer['idx'] = idx

        if buy_buffer['count'] >= 0 and buy_buffer['idx'] != "":
            buy_buffer_idx = buy_buffer['idx']
            sell_loss = (df.at[buy_buffer_idx, 'low'] - df.at[buy_buffer_idx, 'close']) / df.at[buy_buffer_idx, 'close']
            for ema in [20, 50, 100, 200]:
                if df.at[buy_buffer_idx, 'ema_{}'.format(ema)] < df.at[buy_buffer_idx, 'low']:
                    sell_loss = (df.at[buy_buffer_idx, 'ema_{}'.format(ema)] - df.at[buy_buffer_idx, 'close']) / df.at[buy_buffer_idx, 'close']
                    break
            sell_profit = min(abs(sell_loss) * 1.5, sell_profit)
            profit_rate = (df.at[idx, 'high'] - df.at[buy_buffer_idx, 'close']) / df.at[buy_buffer_idx, 'close'] # calc profit_rate
            if profit_rate >= sell_profit or profit_rate <= sell_loss: 
                df.at[idx, name] = (StrategyResultColumnType.SELL, weight)
                buy_buffer = {'count': -1, 'idx': ""}  # init buy_buffer

    return df[[name, 'ema_20', 'ema_50', 'ema_100', 'ema_200']]


def stocastic_rsi_ema_mix_function(data: StockData, weight: int, name: str, timeperiod: int = 200, rsi_period: int = 14, fastk_period=3, fastd_period=3, fastd_matype=0,
                                buy_rate: float = 20.0, sell_profit: float = 0.0225, sell_loss: float = -0.015, heikin_ashi: dict = {}, compare_movement: int = 3):

    def _calculate_heikin_ashi(df, open, high, low, close):
        ha_close = (df[open] + df[high] + df[low] + df[close]) / 4
        ha_open = (df[open].shift(1) + df[close].shift(1)) / 2
        ha_high = df[[high, open, close]].max(axis=1)
        ha_low = df[[low, open, close]].min(axis=1)

        df['ha_close'] = ha_close
        df['ha_open'] = ha_open
        df['ha_high'] = ha_high
        df['ha_low'] = ha_low
        df['mov'] = np.where(df['ha_close'] > df['ha_open'], 'Up', 'Down')
        return df

    df = data.data
    df = _calculate_heikin_ashi(df, 'open', 'high', 'low', 'close')
    # argrelextrema will find the indices of relative minimums of a 1-D array
    local_minima_indices = argrelextrema(df['low'].values, np.less)

    # Initialize a new column with False values
    df['last_local_min_index'] = None

    # Set the last_local_min_index value to the index for local minima
    df.loc[df.index[local_minima_indices], 'last_local_min_index'] = df.index[local_minima_indices]
    df['last_local_min_index'] = df['last_local_min_index'].ffill()
    df['last_local_min_index'].fillna(df.index[0],inplace=True)

    df['ema'] = talib.EMA(df['close'], timeperiod=timeperiod)
    df['RSI'] = talib.RSI(df['close'], timeperiod=rsi_period)
    df['RSI_shift_1'] = df['RSI'].shift(1)
    df['fastk'], df['fastd'] = talib.STOCH(df['RSI'], df['RSI'], df['RSI'], fastk_period=14,slowk_period=3,slowk_matype=0,slowd_period=3, slowd_matype=0)
    df['before_fastk'] = df['fastk'].shift(1)
    df['before_fastd'] = df['fastd'].shift(1)
    df['local_min_fastk'] = df['fastk'].loc[df['last_local_min_index']].values
    df['local_min_fastd'] = df['fastd'].loc[df['last_local_min_index']].values
    df['ha_close_shift_1'] = df['ha_close'].shift(1)
    df['ha_open_shift_1'] = df['ha_open'].shift(1)
    df['ha_low_shift_1'] = df['ha_low'].shift(1)
    def _buy_signal(r: pd.Series):
        if r.close > r.ema:  # buy condition 1
            # buy condition 2
            if r.RSI >= 50.0 and r.RSI_shift_1:
                # buy condition3
                if r.fastk > r.fastd and r.fastk > buy_rate and r.fastk < 80.0 and r.fastd > buy_rate and r.fastd < 80.0 and r.fastk > r.before_fastk and r.fastd > r.before_fastd:
                    if r.before_fastk - r.before_fastd < r.fastk - r.fastd:
                        #buy_condition 4
                        before_candle_length = r.ha_close_shift_1 - r.ha_open_shift_1
                        candle_length = r.ha_close - r.ha_open
                        if r.ha_open == r.ha_low and r.ha_open_shift_1 == r.ha_low_shift_1 and candle_length > 0 and before_candle_length > 0 and candle_length > before_candle_length:
                            return (StrategyResultColumnType.BUY, weight)
        return (StrategyResultColumnType.KEEP, 0)
    
    df['result'] = df.apply(lambda r: _buy_signal(r), axis=1)
    
    # add sell strategy
    buy_buffer = {'count': -1, 'idx': ""}
    
    for count, idx in enumerate(df.index):
        strategy, _ = df.at[idx, 'result']
        # buy signal meet
        if strategy == StrategyResultColumnType.BUY:
            if buy_buffer['count'] < 0 and buy_buffer['idx'] == "":
                buy_buffer['count'] = count
                buy_buffer['idx'] = idx
            else:  # buy_buffer already exist
                if abs(count - buy_buffer['count']) < 7:
                    df.at[idx, 'result'] = (StrategyResultColumnType.KEEP, 0)
                else:
                    df.at[idx, 'result'] = (StrategyResultColumnType.SELL, weight)
                    if idx != df.index[-1]:
                        df['result'].iat[count + 1] = (StrategyResultColumnType.BUY, weight)
                    buy_buffer = {'count': -1, 'idx': ""}  # init buy_buffer

        if buy_buffer['count'] >= 0 and buy_buffer['idx'] != "":
            buy_buffer_idx = buy_buffer['idx']
            last_local_min_index = df['last_local_min_index'].loc[buy_buffer_idx]
            local_min_loss = (df.at[last_local_min_index, 'low'] - df.at[buy_buffer_idx, 'close']) / df.at[buy_buffer_idx, 'close']
            local_min_profit = abs(local_min_loss) * 1.5
            sell_profit = sell_profit if sell_profit <= local_min_profit else local_min_profit
            sell_loss = sell_loss if sell_loss >= local_min_loss else local_min_loss
            profit_rate = (df.at[idx, 'high'] - df.at[buy_buffer_idx, 'close']) / df.at[buy_buffer_idx, 'close'] # calc profit_rate
            if profit_rate >= sell_profit or profit_rate <= sell_loss: 
                df.at[idx, 'result'] = (StrategyResultColumnType.SELL, weight)
                buy_buffer = {'count': -1, 'idx': ""}  # init buy_buffer
        
    df[name] = df['result']
    return df[[name, 'ema', 'fastk', 'fastd']]
        
        
def min_max_function(data: StockData, weight: int, name: str, avg_rolling: int = 7, avg_vol_rate: float = 2.0, high_low_diff_rate: float = 0.10):
    response = pd.DataFrame(
        index=data.data.index, columns=[name])
    temp_df = data.data.copy()
    temp_df['avg_vol'] = temp_df['volume'].rolling(avg_rolling).mean()

    def _min_max_function(r):
        current_avg_vol_rate = (r.volume / r.avg_vol)
        current_high_low_rate = (r.high - r.low)
        if current_avg_vol_rate >= avg_vol_rate and current_high_low_rate >= high_low_diff_rate:
            return (StrategyResultColumnType.BUY, weight)
        else:
            return (StrategyResultColumnType.KEEP, 0)
    temp_df['result'] = temp_df.apply(lambda r: _min_max_function(r), axis=1)

    response[name] = temp_df['result']
    return response[['avg_vol', name]]


def _dataframe_sma(df: pd.DataFrame, weight: int, rolling=100):
    df['sma'] = df['close'].rolling(rolling).mean().fillna(0)
    df['smashift'] = df['sma'].shift(1).fillna(0)

    def _sma_internal(r):
        if (r.smashift - r.sma) > 0.0:
            return (StrategyResultColumnType.SELL, weight)
        elif (r.smashift - r.sma) == 0.0:
            return (StrategyResultColumnType.KEEP, weight)
        else:
            return (StrategyResultColumnType.BUY, weight)
    df['result'] = df.apply(lambda r: _sma_internal(r), axis=1)
    return df[['sma', 'smashift', 'result']]


def _dataframe_sma_multi(df: pd.DataFrame, weight: int, rolling_list: List[int]):
    for rolling in rolling_list:
        df['sma_{}'.format(rolling)] = df['close'].rolling(
            rolling).mean().fillna(0)

    def _sma_internal(r):
        if all([(r.close - r['sma_{}'.format(rolling)]) > 0.0 for rolling in rolling_list]):
            return (StrategyResultColumnType.SELL, weight)
        elif all([(r.close - r['sma_{}'.format(rolling)]) < 0.0 for rolling in rolling_list]):
            return (StrategyResultColumnType.BUY, weight)
        else:
            return (StrategyResultColumnType.KEEP, weight)
    df['result'] = df.apply(lambda r: _sma_internal(r), axis=1)
    column_list = ['sma_{}'.format(rolling) for rolling in rolling_list]
    column_list.append('result')
    return df[column_list]


def sma_function(data: StockData, weight: int, name: str, rolling=100):
    response = pd.DataFrame(
        index=data.data.index, columns=[name])
    """
    strategyfunction here
    """
    response = _dataframe_sma(data.data, weight, rolling)
    response = response.rename({'result': name}, axis=1)
    return response


def buy_rate_function(data: StockData, weight: int, name: str,
                      buy_rolling: int = 30, buy_rate: float = 0.5):
    temp_df = data.data.copy()
    temp_df['buy_rolling'] = temp_df['high'].rolling(
        buy_rolling).max()
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


def sell_rate_function(data: StockData, weight: int, name: str,
                       sell_rolling: int = 30, sell_rate: float = 0.5, base: str = "top"):
    temp_df = data.data.copy()
    if base.lower() == "bottom":
        temp_df['sell_rolling'] = temp_df['low'].rolling(sell_rolling).min()
    else:
        temp_df['sell_rolling'] = temp_df['high'].rolling(
            sell_rolling).max()
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


def sell_rate_custom_function(data: StockData, weight: int, name: str,
                              sell_rolling: int = 30, sell_rate: float = 0.5, keep_weight: float = -1):
    temp_df = data.data.copy()
    temp_df['sell_rolling'] = temp_df['low'].rolling(sell_rolling).min()
    """
    strategyfunction here 
    """
    def _sell_rate(r: pd.Series):
        if pd.isna(r.sell_rolling) or r.sell_rolling == 0.0:
            return (StrategyResultColumnType.KEEP, 0)
        else:
            maximum_price = r['high']
            sell_rolling = r['sell_rolling']
            if sell_rolling * sell_rate <= maximum_price:
                return (StrategyResultColumnType.SELL, weight)
        if keep_weight < 0:
            return (StrategyResultColumnType.KEEP, weight)
        else:
            return (StrategyResultColumnType.KEEP, keep_weight)

    temp_df[name] = temp_df.apply(lambda r: _sell_rate(r), axis=1)
    return temp_df[[name]]


def buy_rate_custom_function(data: StockData, weight: int, name: str,
                             buy_rolling: int = 30, buy_rate: float = 0.5, keep_weight: float = -1):
    temp_df = data.data.copy()
    temp_df['buy_rolling'] = temp_df['high'].rolling(
        buy_rolling).max()
    """
    strategyfunction here
    """

    def _buy_rate(r: pd.Series):
        if pd.isna(r.buy_rolling) or r.buy_rolling == 0.0:
            return (StrategyResultColumnType.KEEP, 0)
        else:
            minimum_price = r['low']
            buy_rolling = r['buy_rolling']
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
    response = pd.DataFrame(
        index=data.data.index, columns=[name])
    """
    strategyfunction here
    """
    response = _dataframe_sma(
        big_stock.data, weight, rolling)
    if big_stock.unit == 'D' and data.unit == 'M':
        response.index = pd.to_datetime(response.index)
        response = response.resample('T').fillna(method='bfill')
        response = response.reindex(data.data.index, method='ffill')

    response = response.rename({'result': name}, axis=1)
    return response


def sma_multi_function(data: StockData, weight: int, name: str, rolling_list: List[int] = [15, 100]):
    response = pd.DataFrame(
        index=data.data.index, columns=[name])
    """
    strategyfunction here
    """
    response = _dataframe_sma_multi(
        data.data, weight, rolling_list)
    response = response.rename({'result': name}, axis=1)
    return response


def sma_multi_big_stock_function(data: StockData, weight: int, name: str, big_stock: StockData, rolling_list: List[int] = [15, 100]):
    response = pd.DataFrame(
        index=data.data.index, columns=[name])
    """
    strategyfunction here
    """
    response = _dataframe_sma_multi(
        big_stock.data, weight, rolling_list)
    if big_stock.unit == 'D' and data.unit == 'M':
        response.index = pd.to_datetime(response.index)
        response = response.resample('T').fillna(method='bfill')
        response = response.reindex(data.data.index)

    response = response.rename({'result': name}, axis=1)
    return response


def _calculate_rsi(data, period):
    rsi = RSIIndicator(close=data.data['close'], window=period)

    return rsi.rsi()


def rsi_function(data: StockData, weight: int, name: str, period: int, sell_score: int, buy_score: int, keep_weight: int = -1):
    response = pd.DataFrame(
        index=data.data.index, columns=['rsi', name])
    response['rsi'] = _calculate_rsi(data, period)

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
    response[name] = response.apply(
        lambda r: _rsi_function(r['rsi']), axis=1)
    return response[['rsi', name]]


def rsi_sma_diff_function(data: StockData, weight: int, name: str, rsi_period: int, sma_period: int, buy_rsi: float = 20.0, sell_rsi: float = 70.0, keep_weight: int = -1):
    response = pd.DataFrame(
        index=data.data.index, columns=['rsi', 'sma', name])
    response['rsi'] = _calculate_rsi(data, rsi_period)
    response['sma'] = response['rsi'].rolling(sma_period).mean().fillna(0)
    response['sma_rsi_diff'] = response['sma'] - response['rsi']
    response['sma_rsi_diff_only_plus'] = response['sma_rsi_diff'].apply(
        lambda r: 0.0 if r < 0 else r)
    response['sma_rsi_diff_diff_rsi'] = response['rsi'] - \
        response['sma_rsi_diff_only_plus']
    response['sma_diff_raw_data'] = response['sma_rsi_diff_only_plus'] - \
        response['sma_rsi_diff_diff_rsi']
    # sma_rsi_diff_only_plus - smi_rsi_diff_diff_rsi

    def _rsi_sma_diff_function(r):
        if r['sma_diff_raw_data'] > 0 and r['rsi'] <= buy_rsi:
            return (StrategyResultColumnType.BUY, weight)
        elif (r['rsi'] - (r['sma_diff_raw_data'] * (-1.0))) < 0.01 and r['rsi'] >= sell_rsi:
            return (StrategyResultColumnType.SELL, weight)
        else:
            return (StrategyResultColumnType.KEEP, 0)

    response[name] = response.apply(
        lambda r: _rsi_sma_diff_function(r), axis=1)
    return response[[name]]


def rsi_big_stock_function(data: StockData, weight: int, name: str, big_stock: StockData, period: int, sell_score: int, buy_score: int, keep_weight: int = -1):
    response = pd.DataFrame(
        index=data.data.index, columns=[name])
    if big_stock.unit == 'D' and data.unit == 'M':
        tmp_big_stock = big_stock.data.copy()
        tmp_big_stock.index = pd.to_datetime(tmp_big_stock.index)
        tmp_big_stock = tmp_big_stock.resample('T').interpolate()
        tmp_big_stock = tmp_big_stock.reindex(data.data.index, method='ffill')
        data.data['rsi'] = _calculate_rsi(tmp_big_stock.data['close'], period)
    else:
        data.data['rsi'] = _calculate_rsi(big_stock.data['close'], period)

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
    response[name] = data.data.apply(
        lambda r: _rsi_function(r['rsi']), axis=1)
    return response[['rsi', name]]


def greed_fear_index_function(data: StockData, weight: int, name: str, greed_fear_index_data: pd.DataFrame, index_fear: int, index_greed: int):
    temp_data = greed_fear_index_data.copy()
    if data.unit == 'M':
        temp_data.index = pd.to_datetime(temp_data.index)
        temp_data = temp_data.resample('T').fillna(method='bfill')
        temp_data = temp_data.reindex(data.data.index, method='ffill')

    response = pd.DataFrame(
        index=data.data.index, columns=[name])
    raw_result = data.data.join(temp_data, how='inner')

    def _greed_fear_index(r):
        if (r['value']) <= index_fear:  # extreme greed
            return (StrategyResultColumnType.BUY, weight)
        elif (r['value'] >= index_greed):
            return (StrategyResultColumnType.SELL, weight)
        else:
            return (StrategyResultColumnType.KEEP, weight)
    response[name] = raw_result.apply(
        lambda r: _greed_fear_index(r), axis=1)
    return response


def _inner_strategy_execute(strategy: Strategy, data: StockData):
    try:
        if not strategy.function:
            strategy.function = basic_function
        response = strategy.function(
            data=data, weight=strategy.weight, name=strategy.name, **strategy.options)
        return ResponseSuccess(response)
    except Exception as e:
        return ResponseFailure(ResponseTypes.SYSTEM_ERROR, e)


def _sum_strategy(series: pd.Series, stockdata: StockData, weight_score_function):
    total_result = {StrategyResultColumnType.KEEP: 0,
                    StrategyResultColumnType.SELL: 0,
                    StrategyResultColumnType.BUY: 0}
    for idx in series.index:
        type, weight = series[idx]
        if stockdata.data['volume'][series.name] == 0.0:
            total_result[StrategyResultColumnType.KEEP] += weight
        else:
            total_result[type] += weight
    score_value = sorted(total_result.values(), reverse=True)
    strategy_rate = weight_score_function(
        first=score_value[0], second=score_value[1], third=score_value[2])
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
    return ((first + 1) * 2) / \
        (1 + second + third)


def strategy_execute(strategy_list: List[Strategy], stockdata: StockData, save_strategy_result: bool = False,
                     weight_score_function=_basic_weight_score_function, plot_package: BacktestPlotPackage = None):
    strategy_total_result = pd.DataFrame(
        index=stockdata.data.index)
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
            strategy.name = "{}_{}".format(
                strategy.name, strategy_dict[strategy.name])
        else:
            strategy_bucket.add(strategy.name)

        response = _inner_strategy_execute(
            strategy=strategy, data=copied_data)
        if isinstance(response, ResponseFailure):
            return ResponseFailure(ResponseTypes.SYSTEM_ERROR, "strategy function error occured!")
        else:
            strategy_result = response.value
            if strategy.inverse:
                strategy_result[strategy.name] = strategy_result.apply(
                    lambda row: _inverse_strategy(row, strategy.name), axis=1)
            if strategy.flag == StrategyExecuteFlagType.SELLONLY:
                strategy_result[strategy.name] = strategy_result.apply(
                    lambda row: _sellonly_strategy(row, strategy.name), axis=1)
            elif strategy.flag == StrategyExecuteFlagType.BUYONLY:
                strategy_result[strategy.name] = strategy_result.apply(
                    lambda row: _buyonly_strategy(row, strategy.name), axis=1)
            if plot_package:
                if stockdata.symbol not in plot_package.package_data_bucket.keys():
                    plot_package.package_data_bucket[stockdata.symbol] = []
                plot_package.package_data_bucket[stockdata.symbol].append(
                    {strategy.name: strategy_result})
            if len(stockdata) >= len(strategy_result):
                strategy_total_result = strategy_total_result.join(
                    strategy_result[[strategy.name]], how='left', rsuffix='{}_'.format(strategy.name))
            else:
                strategy_total_result = strategy_total_result.join(
                    strategy_result[[strategy.name]], how='inner', rsuffix='{}_'.format(strategy.name))
    # delete temporary data
    del copied_data
    # fill na with
    for column in strategy_total_result.columns:
        strategy_total_result[column] = strategy_total_result[column].fillna(
            {i: (StrategyResultColumnType.KEEP, 0) for i in strategy_total_result.index})
    if save_strategy_result:
        strategy_total_result.to_csv(
            "{}_strategy_total_result.csv".format(stockdata.symbol))
    return ResponseSuccess(StrategyResult(strategy_total_result.apply(
        lambda row: _sum_strategy(row, stockdata, weight_score_function), axis=1)))
