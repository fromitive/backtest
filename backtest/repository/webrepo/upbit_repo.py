import requests
import pandas as pd
import time
from backtest.domains.stockdata import StockData
from datetime import datetime, timedelta


class UpbitRepo:
    API_URL = 'https://api.upbit.com/v1/candles/{chart_interval_kind}?market={payment_currency}-{order_currency}&count=200&to={to_date}'
    API_HEADERS = {"accept": "application/json"}

    def __init__(self):
        self.order_currency = 'BTC'
        self.payment_currency = 'KRW'
        self.chart_intervals = '24h'
        self.from_date = ''
        self.to_date = ''
        self.start_time = ''
        self.end_time = ''
        self.chart_interval_kind = ''

    def get(self, filters=None):
        if filters:
            filter = list(filters.keys())
            self.order_currency = filters['order__eq'] if 'order__eq' in filter else 'BTC'
            self.payment_currency = filters['payment__eq'] if 'payment__eq' in filter else 'KRW'
            self.chart_intervals = filters['chart_interval__eq'] if 'chart_interval__eq' in filter else '24h'
            self.from_date = datetime.strptime(
                filters['from__eq'], "%Y-%m-%d") if 'from__eq' in filter else ''
            self.to_date = datetime.strptime(
                filters['to__eq'], "%Y-%m-%d") if 'to__eq' in filter else ''

            if self.chart_intervals == '30m':
                start_hours = filters['start_time__eq'].split(
                    ':')[0] if 'start_time__eq' in filter else '00'
                start_minutes = filters['start_time__eq'].split(
                    ':')[1] if 'start_time__eq' in filter else '00'
                end_hours = filters['end_time__eq'].split(
                    ':')[0] if 'end_time__eq' in filter else '00'
                end_minutes = filters['end_time__eq'].split(
                    ':')[1] if 'end_time__eq' in filter else '00'
                self.start_time = timedelta(
                    hours=int(start_hours), minutes=int(start_minutes))
                self.end_time = timedelta(
                    hours=int(end_hours), minutes=int(end_minutes))
                self.chart_interval_kind = 'minutes/30'
            else:
                self.chart_interval_kind = 'days'
        if self.to_date:
            if self.start_time:
                self.to_date += self.end_time
            self.to_date = self.to_date.strftime('%Y-%m-%dT%H:%M:%S')
        if self.from_date and self.start_time:
            self.from_date += self.start_time
        request_url = self.API_URL.format(
            order_currency=self.order_currency,
            payment_currency=self.payment_currency,
            chart_interval_kind=self.chart_interval_kind,
            to_date=self.to_date)

        temp_list = []
        before_date = ""

        while True:
            response = requests.get(request_url, headers=self.API_HEADERS)
            if response.status_code == 200:
                result_list = response.json()  # list
                if result_list == []:
                    break
                data_last_date = datetime.strptime(
                    result_list[-1]['candle_date_time_kst'], '%Y-%m-%dT%H:%M:%S')
                if self.chart_intervals == '24h':
                    data_last_date = data_last_date.replace(
                        hour=0, minute=0, second=0, microsecond=0)

                print(before_date, data_last_date)
                if before_date != '' and data_last_date == before_date:
                    break
            else:
                raise Exception('request error', response.status_code)
            if self.from_date == '':
                temp_list += result_list
                break
            elif self.from_date < data_last_date:
                before_date = data_last_date
                data_last_date = data_last_date.strftime('%Y-%m-%dT%H:%M:%S')
                request_url = self.API_URL.format(
                    order_currency=self.order_currency,
                    payment_currency=self.payment_currency,
                    chart_interval_kind=self.chart_interval_kind,
                    to_date=data_last_date)
            elif self.from_date > data_last_date:
                idx = -1
                flag = False
                while result_list[0] != result_list[idx]:
                    compare_date = datetime.strptime(
                        result_list[idx]['candle_date_time_kst'], '%Y-%m-%dT%H:%M:%S')
                    if self.chart_intervals == '24h':
                        compare_date = compare_date.replace(
                            hour=0, minute=0, second=0, microsecond=0)
                    if compare_date == self.from_date:
                        result_list = result_list[:idx+1]
                        flag = True
                        break
                    idx -= 1
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
        temp_df = pd.DataFrame(temp_list, columns=[
            'candle_date_time_kst', 'opening_price', 'high_price', 'low_price', 'trade_price', 'candle_acc_trade_volume'])

        temp_df.rename(columns={'opening_price': 'open', 'high_price': 'high',
                                'low_price': 'low', 'trade_price': 'close', 'candle_date_time_kst': 'date', 'candle_acc_trade_volume': 'volume'}, inplace=True)
        if self.chart_intervals == '30m':
            temp_df.set_index('date', inplace=True)
            temp_df.index = pd.to_datetime(temp_df.index)
            temp_df = temp_df.astype({'open': 'float',
                                      'high': 'float',
                                      'close': 'float',
                                      'low': 'float',
                                      'volume': 'float'})
            temp_df.sort_index(ascending=True, inplace=True)
            temp_df = temp_df.between_time('06:00', '23:30').resample('D', label='left', closed='left').agg(
                {'open': 'first', 'high': 'max', 'low': 'min', 'close': 'last', 'volume': 'sum'})
            return StockData(symbol=self.order_currency, data=temp_df)
        return StockData.from_dict(temp_df.to_dict('list'), self.order_currency)
