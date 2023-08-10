from abc import ABC, abstractmethod
import pandas as pd
from datetime import datetime

class Trade(ABC):
    @abstractmethod
    def get_krw(self) -> float:
        pass
    
    @abstractmethod
    def get_trade_fee(self, symbol: str, payment_currency: str = 'KRW') -> float:
        pass
    
    @abstractmethod
    def get_coin_price(self, symbol: str, payment_currency: str = 'KRW') -> float:
        pass
    
    @abstractmethod
    def trade(self, symbol: str, units: str, type: str, price: float, payment_currency: str = 'KRW') -> str:
        pass
    
    @abstractmethod
    def get_order_detail(self, order_id) -> dict:
        return self.upbit.get_order(order_id)

    @abstractmethod
    def get_order_status(self, order_id, verbose=True) -> bool:
        pass

    @abstractmethod
    def get_cancel_status(self, order_id, verbose=True) -> bool:
        pass

    @abstractmethod
    def get_order_type(self, order_id):
        pass
    
    @abstractmethod
    def order_cancel(self, order_id, symbol, order_type, payment_currency: str = 'KRW'):
        pass

    @abstractmethod
    def get_current_market(self):
        pass

    @abstractmethod
    def get_current_time(self):
        pass

    @abstractmethod    
    def get_top_symbol_list(self, num, column_name):
        pass 
    
    @abstractmethod        
    def get_current_time(self):
        pass
    
    @abstractmethod        
    def get_balance(self,symbol: str, payment_currency: str = 'KRW') -> float:
        pass
    