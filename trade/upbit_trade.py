from datetime import datetime

import pyupbit
import requests

from backtest.module_compet.pandas import pd
from trade.trade import Trade


class UpbitTrade(Trade):
    def __init__(self, access_key: str, secret: str):
        """set upbit api key and secret for trading at upbit market.

        Args:
            access_key (str): upbit API access key here.
            secret (str): upbit API secret key here.
        """
        self.upbit = pyupbit.Upbit(access=access_key, secret=secret)

    def get_krw(self) -> float:
        """returns the krw value your wallet through registered Upbit API key.

        Returns:
            float: returns krw value your wallet
        """
        balances = self.upbit.get_balances()
        for balance in balances:
            if balance["currency"] == "KRW":
                return float(balance["balance"])

        return float(0.0)

    def get_trade_fee(self, symbol: str, payment_currency: str = "KRW") -> float:
        """returns trade fee, Upbit market default fee value is 0.0005.

        Args:
            symbol (str): set what you want to get the fee of the symobol which you trade.
            payment_currency (str, optional): set payment currency. it can be "KRW or BTC" market in Upbit. Defaults to "KRW".

        Returns:
            float: returns trade fee
        """
        return 0.0005

    def get_coin_price(self, symbol: str, payment_currency: str = "KRW", bid_ask: str = "bid", prior: int = 0) -> float:
        """get realtime current coin price which get through orderbook.

        Args:
            symbol (str): set what you want to get the price of the symbol.
            payment_currency (str, optional): set payment currency. it can be "KRW or BTC" market in Upbit Defaults to "KRW".
            bid_ask (str, optional): select which price type of orderbook, the value can be "bid(buy) or ask(sell)". Defaults to "bid".
            prior (int, optional): select priority of the price. if you set the value more smaller, you could get the price close to most bid(buy) or ask(sell) price. Defaults to 0.

        Returns:
            float: the price of the symbol what you specified
        """
        orderbooks = pyupbit.get_orderbook(
            ticker="{payment_currency}-{symbol}".format(payment_currency=payment_currency, symbol=symbol)
        )
        if bid_ask == "bid":
            return orderbooks["orderbook_units"][prior]["bid_price"]
        elif bid_ask == "ask":
            return orderbooks["orderbook_units"][prior]["ask_price"]

    def trade(self, symbol: str, units: str, type: str, price: float, payment_currency: str = "KRW") -> str:
        """execute bid(buy) or ask(sell) the specific symbol with the price and the units what you specific

        Args:
            symbol (str): the symbol what you want to trade
            units (str): the amount of the symbol what you want to trade
            type (str): the type of the trading what you want, it can be "bid"(buy) or "ask"(sell)
            price (float): the price of the symbol what you want to trade
            payment_currency (str, optional): set payment currency. it can be "KRW or BTC" market in Upbit. Defaults to "KRW".

        Returns:
            str: result the trade id (order id). that used for searching the trade status (cancel or done), executing cancel the order.
        """
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

    def get_order_detail(self, order_id: str) -> dict:
        """returns order status

        Args:
            order_id (str): set the order id what you want to get the status of trade result

        Returns:
            dict: returns dict data from upbit api server for more information set the Upbit API Documentation at (https://docs.upbit.com/reference/%EA%B0%9C%EB%B3%84-%EC%A3%BC%EB%AC%B8-%EC%A1%B0%ED%9A%8C)
        """
        return self.upbit.get_order(order_id)

    def get_order_status(self, order_id: str, verbose=True) -> bool:
        """return order status only (done or not)

        Args:
            order_id (str): set the order id what you want to get the status of trade result
            verbose (bool, optional): debug the order status value. Defaults to True.

        Returns:
            bool: if your trade is successful done, return True, else set False.
        """
        order_detail = self.upbit.get_order(order_id)
        if verbose:
            print(order_detail)

        order_state = order_detail["state"] if "state" in order_detail.keys() else "no"

        if order_state == "done":
            return True
        else:
            return False

    def get_cancel_status(self, order_id: str, verbose=True) -> bool:
        """return order status only (cancel or not)

        Args:
            order_id (str): set the order id what you want to get the status of trade result
            verbose (bool, optional): debug the order status value. Defaults to True.
        Returns:
            bool: if your trade is successful canceled, return True, else set False.
        """
        order_detail = self.upbit.get_order(order_id)
        if verbose:
            print(order_detail)

        order_state = order_detail["state"] if "state" in order_detail.keys() else "no"

        if order_state == "cancel":
            return True
        else:
            return False

    def get_order_type(self, order_id: str) -> str:
        """get order(trade) type (bid(buy) or ask(sell)) for caceling the trade.

        Args:
            order_id (str): set the order id what you want to get the type of trade

        Returns:
            str: returns order type that has bid(buy) or ask(sell).
        """
        order_detail = self.upbit.get_order(order_id)
        order_type = order_detail["side"] if "side" in order_detail.keys() else "false"

        if order_type == "false":
            return ""
        else:
            return order_type

    def order_cancel(self, order_id: str, symbol: str, order_type: str, payment_currency: str = "KRW") -> None:
        """cancel the trade which you executed

        Args:
            order_id (str): set the order_id what you want to cancel
            symbol (str): set the symbol of the trading
            order_type (str): set the order type of the trading. you should get the type using get_order_type method
            payment_currency (str, optional): set payment currency. it can be "KRW or BTC" market in Upbit. Defaults to "KRW".
        """
        self.upbit.cancel_order(uuid=order_id)

    def get_current_market(self) -> pd.DataFrame:
        """get all of a list of the symbol possible trading in upbit market

        Returns:
            pd.DataFrame: the symbol list with each status
        """
        url = "https://api.upbit.com/v1/ticker"
        headers = {"accept": "application/json"}
        data = {"markets": ",".join(pyupbit.get_tickers(fiat="KRW"))}
        response = requests.get(url, headers=headers, params=data)
        data = response.json()
        df = pd.DataFrame(data)
        df = df.set_index("market")
        return df

    def get_top_symbol_list(self, num: int, column_name: str) -> list:
        """returns the symbol list of top of [column_name] trading

        Args:
            num (int): the number of the symbol what you want
            column_name (str): the allign base what you want to get symbol list

        Returns:
            list: returns symbol list
        """
        raw_symbol_list = list(self.get_current_market().nlargest(num, column_name).index)
        symbol_list = [raw_symbol.split("-")[1] for raw_symbol in raw_symbol_list]
        return symbol_list

    def get_current_time(self) -> datetime:
        """get current time of the Upbit market

        Returns:
            datetime: the time of the Upbit market
        """
        url = "https://crix-api-tv.upbit.com/v1/crix/tradingview/time"
        headers = {"accept": "application/json"}
        response = requests.get(url, headers=headers)
        data = int(response.text)
        current_time = datetime.fromtimestamp(data)

        return current_time

    def get_balance(self, symbol: str, payment_currency: str = "KRW") -> float | str:
        """the balance(amount) of the symbol in your wallet

        Args:
            symbol (str): Specify the symbol which amount you want to query.
            payment_currency (str, optional): set payment currency. it can be "KRW or BTC" market in Upbit. Defaults to "KRW".

        Returns:
            float | str: if the query result successful return, you get the amount of coin else, you get "error" text.
        """
        labeled_symbol = "{payment_currency}-{symbol}".format(payment_currency=payment_currency, symbol=symbol)
        result = self.upbit.get_balance(labeled_symbol)

        if isinstance(result, float) or isinstance(result, int):
            return float(result)
        else:
            return "error"
