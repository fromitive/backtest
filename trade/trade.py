from abc import ABC, abstractmethod
from backtest.module_compet.pandas import pd
from datetime import datetime


class Trade(ABC):
    @abstractmethod
    def get_krw(self) -> float:
        pass

    @abstractmethod
    def get_trade_fee(self, symbol: str, payment_currency: str = "KRW") -> float:
        pass

    @abstractmethod
    def get_coin_price(self, symbol: str, payment_currency: str = "KRW", bid_ask: str = "bid") -> float:
        pass

    @abstractmethod
    def trade(self, symbol: str, units: str, type: str, price: float, payment_currency: str = "KRW") -> str:
        pass

    @abstractmethod
    def get_order_detail(self, order_id: str) -> dict:
        pass

    @abstractmethod
    def get_order_status(self, order_id: str, verbose=True) -> bool:
        pass

    @abstractmethod
    def get_cancel_status(self, order_id: str, verbose=True) -> bool:
        pass

    @abstractmethod
    def get_order_type(self, order_id: str) -> str:
        pass

    @abstractmethod
    def order_cancel(self, order_id: str, symbol: str, order_type: str, payment_currency: str = "KRW") -> None:
        pass

    @abstractmethod
    def get_current_market(self) -> pd.DataFrame:
        pass

    @abstractmethod
    def get_current_time(self) -> datetime:
        pass

    @abstractmethod
    def get_top_symbol_list(self, num: int, column_name: str) -> list:
        pass

    @abstractmethod
    def get_balance(self, symbol: str, payment_currency: str = "KRW") -> float | str:
        pass
