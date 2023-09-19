from backtest.module_compet.talib.common import CommonTALib
from backtest.module_compet.pandas import pd
import numpy as np


class PANDAS(CommonTALib):
    @staticmethod
    def EMA(series: pd.Series, timeperiod: int) -> pd.Series:
        return series.ewm(span=timeperiod, adjust=False).mean()

    @staticmethod
    def BBANDS(series: pd.Series, timeperiod: int, nbdevup: int, nbdevdn: int) -> (pd.Series, pd.Series, pd.Series):  # return upper, middle, lower band
        middle_band = series.rolling(window=timeperiod).mean()
        std_deviation = series.rolling(window=timeperiod).std()
        upper_band = middle_band + (std_deviation * nbdevup)
        lower_band = middle_band + (std_deviation * nbdevdn)
        
        return upper_band, middle_band, lower_band

    @staticmethod
    def RSI(series: pd.Series, timeperiod: int) -> pd.Series:
        close_diff = series.diff()

        gain = np.where(close_diff > 0, close_diff, 0)
        loss = np.where(close_diff < 0, abs(close_diff), 0)
        avg_gain = gain.rolling(window=timeperiod, min_periods=1).mean()
        avg_loss = loss.rolling(window=timeperiod, min_periods=1).mean()

        rs = avg_gain / avg_loss
        rsi = 100 - (100 / (1 + rs))

        return rsi

    @staticmethod
    def STOCH(high_rsi, low_rsi, close_rsi, fastk_period: int = 14, slowk_period: int = 3, slowk_matype: int = 0, slowd_period: int = 3, slowd_matype: int = 0) -> (pd.Series, pd.Series):  # return fastk fastd
        min_val = close_rsi.rolling(window=fastk_period, center=False).min()
        max_val = close_rsi.rolling(window=fastk_period, center=False).max()

        stoch = ((close_rsi - min_val) / (max_val - min_val)) * 100

        fast_k = stoch.rolling(window=slowk_period, center=False).mean()
        fast_d = fast_k.rolling(window=slowd_period, center=False).mean()
        return fast_k, fast_d
