import requests
import pandas as pd
import time
from backtest.domains.stockdata import StockData


class UpbitRepo:
    API_URL = 'https://api.upbit.com/v1/candles/days?market={payment_currency}-{order_currency}&count=200'
    API_HEADERS = {"accept": "application/json"}

    def __init__(self):
        self.order_currency = 'BTC'
        self.payment_currency = 'KRW'
        self.chart_intervals = '24h'
        self.from_date = ''
        self.to_date = ''

    def get(self, filters=None):
        if filters:
            filter = list(filters.keys())
            self.order_currency = filters['order__eq'] if 'order__eq' in filter else 'BTC'
            self.payment_currency = filters['payment__eq'] if 'payment__eq' in filter else 'KRW'
            self.chart_intervals = filters['chart_intervals__eq'] if 'chart_intervals__eq' in filter else '24h'
            self.from_date = filters['from__eq'] if 'from__eq' in filter else ''
            self.to_date = filters['to__eq'] if 'to__eq' in filter else ''

        request_url = self.API_URL.format(
            order_currency=self.order_currency,
            payment_currency=self.payment_currency,
            chart_intervals=self.chart_intervals)

        response = requests.get(request_url, headers=self.API_HEADERS)
        if response.status_code == 200:
            dict_data = response.json()
            temp_df = pd.DataFrame(dict_data, columns=[
                                   'candle_date_time_kst', 'opening_price', 'high_price', 'low_price', 'trade_price', 'candle_acc_trade_volume'])
            temp_df.rename(columns={'opening_price': 'open', 'high_price': 'high',
                           'low_price': 'low', 'trade_price': 'close', 'candle_date_time_kst': 'date', 'candle_acc_trade_volume': 'volume'}, inplace=True)
            return StockData.from_dict(temp_df.to_dict('list'))
        else:
            raise Exception('request error', response.status_code)
