import time

import requests

from backtest.domains.stockdata import StockData
from backtest.module_compet.pandas import pd


class BithumbRepo:

    API_URL = 'https://api.bithumb.com/public/candlestick/{order_currency}_{payment_currency}/{chart_intervals}'
    API_HEADERS = {"accept": "application/json"}

    def __init__(self):
        self.order_currency = 'BTC'
        self.payment_currency = 'KRW'
        self.chart_intervals = '24h'
        self.from_date = ''
        self.to_date = ''
        self.unit = 'D'

    def get(self, filters=None):
        if filters:
            filter = list(filters.keys())
            self.order_currency = filters['order__eq'] if 'order__eq' in filter else 'BTC'
            self.payment_currency = filters['payment__eq'] if 'payment__eq' in filter else 'KRW'
            self.chart_intervals = filters['chart_interval__eq'] if 'chart_interval__eq' in filter else '24h'
            self.from_date = filters['from__eq'] if 'from__eq' in filter else ''
            self.to_date = filters['to__eq'] if 'to__eq' in filter else ''
            if self.chart_intervals not in ['1m', '3m', '5m', '10m', '30m', '1h', '6h', '12h', '24h']:
                raise Exception(
                    'request error - this repo not support chart_intervals {} TT'.format(self.chart_intervals))

            if self.chart_intervals == '24h':
                self.unit = 'D'
            else:
                self.unit = 'M'

        request_url = self.API_URL.format(
            order_currency=self.order_currency,
            payment_currency=self.payment_currency,
            chart_intervals=self.chart_intervals)

        response = requests.get(request_url, headers=self.API_HEADERS)
        if response.status_code == 200:
            dict_data = response.json().get('data')
            temp_df = pd.DataFrame(
                dict_data, columns=['time', 'open', 'close',
                                    'high', 'low', 'volume'])
            temp_df['date'] = temp_df['time'].apply(
                lambda x: time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(x / 1000)))
            temp_df.set_index('date', inplace=True)
            temp_df.sort_index(ascending=True, inplace=True)
            temp_df['date'] = temp_df.index
            if self.from_date:
                temp_df = temp_df.loc[self.from_date:]
            if self.to_date:
                temp_df = temp_df.loc[:self.to_date]

            return StockData.from_dict(temp_df.to_dict('list'), symbol=self.order_currency, unit=self.unit)
        else:
            raise Exception('request error', response.status_code)
