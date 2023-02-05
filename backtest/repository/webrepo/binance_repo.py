import requests
import pandas as pd
import numpy as np
import time
from datetime import datetime, timedelta
from backtest.domains.stockdata import StockData


class BinanceRepo:
    API_URL = 'https://www.binance.com/api/v3/klines'
    API_HEADERS = {"accept": "application/json"}
    API_PARAMS = {
        'symbol': '{order_currency}{payment_currency}',
        'interval': '{chart_intervals}',
        'limit': '1000',
        'endTime': '{to_date}'
    }

    def __init__(self):
        self.order_currency = 'BTC'
        self.payment_currency = 'USDT'
        self.chart_intervals = '1d'
        self.from_date = ''
        self.to_date = ''

    def get(self, filters=None):
        if filters:
            filter = list(filters.keys())
            self.order_currency = filters['order__eq'].upper(
            ) if 'order__eq' in filter else 'BTC'
            self.payment_currency = filters['payment__eq'].upper(
            ) if 'payment__eq' in filter else 'USDT'
            self.chart_intervals = filters['chart_intervals__eq'] if 'chart_intervals__eq' in filter else '1d'
            if self.chart_intervals == '24h':
                self.chart_intervals = '1d'
            self.from_date = datetime.strptime(
                filters['from__eq'], "%Y-%m-%d") if 'from__eq' in filter else ''
            self.to_date = datetime.strptime(
                filters['to__eq'], "%Y-%m-%d") if 'to__eq' in filter else ''

        request_params = self.API_PARAMS
        request_params['symbol'] = request_params['symbol'].format(
            order_currency=self.order_currency, payment_currency=self.payment_currency)
        request_params['interval'] = request_params['interval'].format(
            chart_intervals=self.chart_intervals)

        if self.to_date:
            self.to_date = int(self.to_date.timestamp() * 1000)
            request_params['endTime'] = self.to_date
        else:
            del request_params['endTime']

        temp_list = []
        while True:
            response = requests.get(
                self.API_URL, headers=self.API_HEADERS, params=request_params)
            if response.status_code == 200:
                result_list = response.json()
                data_last_date = datetime.fromtimestamp(
                    int(result_list[0][0]) / 1000).replace(hour=0, minute=0, second=0, microsecond=0)
                print(data_last_date.strftime("%Y-%m-%d"))
            else:
                raise Exception('request error', response.status_code)
            if self.from_date == '':
                temp_list += result_list
                break
            elif self.from_date < data_last_date:
                data_last_date = data_last_date + timedelta(days=1)
                data_last_date = int(data_last_date.timestamp() * 1000)
                request_params['endTime'] = data_last_date
            elif self.from_date > data_last_date:
                flag = False
                for idx, candledata in enumerate(result_list):
                    compare_date = datetime.fromtimestamp(
                        int(candledata[0]) / 1000).replace(hour=0, minute=0, second=0, microsecond=0)
                    if compare_date > self.from_date:
                        result_list = result_list[idx:]
                        flag = True
                        break
                if flag:
                    temp_list += result_list
                    break
            elif self.from_date == data_last_date:
                break
            else:
                raise Exception(
                    'date_convert error data_last_date: ', data_last_date)
            temp_list += result_list
            time.sleep(0.3)
        columns = ['open_time', 'open', 'high', 'low', 'close', 'volume', 'close_time',
                   'qav', 'num_trades', 'taker_base_vol', 'taker_quote_vol', 'ignore']
        temp_df = pd.DataFrame(temp_list, columns=columns)
        temp_df['date'] = temp_df['open_time'].apply(
            lambda x: time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(x/1000)))
        usecols = ['date', 'open', 'high', 'low', 'close', 'volume']
        temp_df = temp_df[usecols]
        return StockData.from_dict(temp_df.to_dict('list'))
