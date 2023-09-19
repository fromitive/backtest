from abc import ABC, abstractmethod
from backtest.module_compet.pandas import pd


class CommonTALib(ABC):
    @staticmethod
    @abstractmethod
    def EMA(series: pd.Series, timeperiod: int) -> pd.Series:
        pass

    @staticmethod
    @abstractmethod
    def BBANDS(series: pd.Series, timeperiod: int, nbdevup: int, nbdevdn: int) -> (pd.Series, pd.Series, pd.Series):  # return upper, middle, lower band
        pass

    @staticmethod
    @abstractmethod
    def RSI(series: pd.Series, timeperiod: int) -> pd.Series:
        pass

    @staticmethod
    @abstractmethod
    def STOCH(high_rsi, low_rsi, close_rsi, fastk_period: int = 14, slowk_period: int = 3, slowk_matype: int = 0, slowd_period: int = 3, slowd_matype: int = 0) -> (pd.Series, pd.Series):  # return fastk fastd
        pass
