import requests
import pandas as pd
import numpy as np
import time
from backtest.domains.stockdata import StockData


class BinanceRepo:
    API_URL = 'https://www.binance.com/api/v3/klines?symbol={order_currency}{payment_currency}&interval={chart_intervals}&limit=1000'
    API_HEADERS = {"accept": "application/json"}

    def __init__(self):
        self.order_currency = 'BTC'
        self.payment_currency = 'USDT'
        self.chart_intervals = '1d'

    def get(self, filters=None):
        if filters:
            filter = list(filters.keys())
            self.order_currency = filters['order__eq'] if 'order__eq' in filter else 'BTC'
            self.payment_currency = filters['payment__eq'] if 'payment__eq' in filter else 'USDT'
            self.chart_intervals = filters['chart_intervals__eq'] if 'chart_intervals__eq' in filter else '1d'
            if self.chart_intervals == '24h':
                self.chart_intervals = '1d'

        request_url = self.API_URL.format(
            order_currency=self.order_currency,
            payment_currency=self.payment_currency,
            chart_intervals=self.chart_intervals)

        response = requests.get(request_url, headers=self.API_HEADERS)
        if response.status_code == 200:
            columns = ['open_time', 'open', 'high', 'low', 'close', 'volume', 'close_time',
                       'qav', 'num_trades', 'taker_base_vol', 'taker_quote_vol', 'ignore']
            data = response.json()
            temp_df = pd.DataFrame(data,
                                   columns=columns)
            temp_df['date'] = temp_df['open_time'].apply(
                lambda x: time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(x/1000)))
            usecols = ['date', 'open', 'high', 'low', 'close', 'volume']
            temp_df = temp_df[usecols]
            return StockData.from_dict(temp_df.to_dict('list'))
        else:
            raise Exception('request error', response.status_code)
