from trade.trade import Trade
import pyupbit
import requests
from datetime import datetime
import pandas as pd


class UpbitTrade(Trade):
    def __init__(self, access_key: str, secret: str):
        self.upbit = pyupbit.Upbit(access=access_key, secret=secret)

    def get_krw(self) -> float:
        balances = self.upbit.get_balances()
        for balance in balances:
            if balance["currency"] == "KRW":
                return float(balance["balance"])

        return float(0.0)

    def get_trade_fee(self, symbol: str, payment_currency: str = "KRW") -> float:
        return 0.0005

    def get_coin_price(self, symbol: str, payment_currency: str = "KRW", bid_ask: str = "bid", prior: int = 0) -> float:
        orderbooks = pyupbit.get_orderbook(
            ticker="{payment_currency}-{symbol}".format(payment_currency=payment_currency, symbol=symbol)
        )
        if bid_ask == "bid":
            return orderbooks["orderbook_units"][prior]["bid_price"]
        elif bid_ask == "ask":
            return orderbooks["orderbook_units"][prior]["ask_price"]

    def trade(self, symbol: str, units: str, type: str, price: float, payment_currency: str = "KRW") -> str:
        order_id = ""
        if type == "BUY":
            order_id = self.upbit.buy_limit_order(
                ticker="{payment_currency}-{symbol}".format(payment_currency=payment_currency, symbol=symbol),
                price=price,
                volume=units,
            )
        elif type == "SELL":
            order_id = self.upbit.sell_limit_order(
                ticker="{payment_currency}-{symbol}".format(payment_currency=payment_currency, symbol=symbol),
                price=price,
                volume=units,
            )

        return order_id["uuid"]

    def get_order_detail(self, order_id) -> dict:
        return self.upbit.get_order(order_id)

    def get_order_status(self, order_id, verbose=True) -> bool:
        order_detail = self.upbit.get_order(order_id)
        if verbose:
            print(order_detail)

        order_state = order_detail["state"] if "state" in order_detail.keys() else "no"

        if order_state == "done":
            return True
        else:
            return False

    def get_cancel_status(self, order_id, verbose=True) -> bool:
        order_detail = self.upbit.get_order(order_id)
        if verbose:
            print(order_detail)

        order_state = order_detail["state"] if "state" in order_detail.keys() else "no"

        if order_state == "cancel":
            return True
        else:
            return False

    def get_order_type(self, order_id) -> str:
        order_detail = self.upbit.get_order(order_id)
        order_type = order_detail["side"] if "side" in order_detail.keys() else "false"

        if order_type == "false":
            return ""
        else:
            return order_type

    def order_cancel(self, order_id, symbol, order_type, payment_currency: str = "KRW") -> None:
        self.upbit.cancel_order(uuid=order_id)

    def get_current_market(self):
        url = "https://api.upbit.com/v1/ticker"
        headers = {"accept": "application/json"}
        data = {"markets": ",".join(pyupbit.get_tickers(fiat="KRW"))}
        response = requests.get(url, headers=headers, params=data)
        data = response.json()
        df = pd.DataFrame(data)
        df = df.set_index("market")
        return df

    def get_top_symbol_list(self, num, column_name):
        raw_symbol_list = list(self.get_current_market().nlargest(num, column_name).index)
        symbol_list = [raw_symbol.split("-")[1] for raw_symbol in raw_symbol_list]
        return symbol_list

    def get_current_time(self):
        url = "https://crix-api-tv.upbit.com/v1/crix/tradingview/time"
        headers = {"accept": "application/json"}
        response = requests.get(url, headers=headers)
        data = int(response.text)
        current_time = datetime.fromtimestamp(data)

        return current_time

    def get_balance(self, symbol: str, payment_currency: str = "KRW") -> float | str:
        labeled_symbol = "{payment_currency}-{symbol}".format(payment_currency=payment_currency, symbol=symbol)
        result = self.upbit.get_balance(labeled_symbol)

        if isinstance(result, float) or isinstance(result, int):
            return float(result)
        else:
            return "error"
