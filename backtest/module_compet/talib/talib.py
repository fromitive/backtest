from backtest.module_compet.talib.common import CommonTALib
from backtest.module_compet.pandas import pd
import talib


class TALib(CommonTALib):
    @staticmethod
    def EMA(series: pd.Series, timeperiod: int) -> pd.Series:
        return talib.EMA(series, timeperiod=timeperiod)

    @staticmethod
    def BBANDS(series: pd.Series, timeperiod: int, nbdevup: int, nbdevdn: int) -> (pd.Series, pd.Series, pd.Series):  # return upper, middle, lower band
        return talib.BBANDS(series, timeperiod, nbdevup, nbdevdn)

    @staticmethod
    def RSI(series: pd.Series, timeperiod: int) -> pd.Series:
        return talib.RSI(series, timeperiod=timeperiod)

    @staticmethod
    def STOCH(high_rsi, low_rsi, close_rsi, fastk_period: int = 14, slowk_period: int = 3, slowk_matype: int = 0, slowd_period: int = 3, slowd_matype: int = 0) -> (pd.Series, pd.Series):  # return fastk fastd
        return talib.STOCH(high_rsi, low_rsi, close_rsi, fastk_period, slowk_period, slowk_matype, slowd_period, slowd_matype)
