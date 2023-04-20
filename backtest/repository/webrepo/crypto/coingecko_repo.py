# coingeckco repo - return SelectorReference
import time
from datetime import datetime

import requests

from backtest.domains.selector_reference import SelectorReference
from backtest.module_compet.pandas import pd
from backtest.util.selector_reference_util import get_coingecko_symbol_id


def convert_timestamp(str_date: str) -> int:
    return int(datetime.strptime(str_date, "%Y-%m-%d").timestamp())


class CoinGeckoRepo:
    API_URL = 'https://www.coingecko.com/market_cap/{symbol_id}/usd/custom.json?from={from_timestamp}&to={to_timestamp}'
    API_HEADERS = {"accept": "application/json"}

    def __init__(self):
        self.symbol = 'BTC'
        self.from_date = '1999-01-01'
        self.to_date = datetime.strftime(datetime.now(), "%Y-%m-%d")

        self.from_timestamp = convert_timestamp('1999-01-01')
        self.to_timestamp = int(datetime.now().timestamp())
        self.symbol_id = '1'

    def get(self, filters=None):
        if filters:
            filter = list(filters.keys())
            self.symbol = filters['symbol__eq'] if 'symbol__eq' in filter else 'BTC'
            self.from_date = filters['from__eq'] if 'from__eq' in filter else '1999-01-01'
            self.to_date = filters['to__eq'] if 'to__eq' in filter else datetime.strftime(
                datetime.now(), "%Y-%m-%d")
            self.from_timestamp = convert_timestamp(self.from_date)
            self.to_timestamp = convert_timestamp(
                self.to_date) + 86320  # for add 23:59
            self.symbol_id = get_coingecko_symbol_id(self.symbol)
        request_url = self.API_URL.format(
            symbol_id=self.symbol_id,
            from_timestamp=self.from_timestamp,
            to_timestamp=self.to_timestamp)
        response = requests.get(request_url, headers=self.API_HEADERS)
        if response.status_code == 200:
            dict_data = response.json().get('stats')
            temp_df = pd.DataFrame(
                dict_data, columns=['time', 'marketcap'])
            temp_df['date'] = temp_df['time'].apply(
                lambda x: time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(x / 1000)))
            temp_df.set_index('date', inplace=True)
            temp_df.sort_index(ascending=True, inplace=True)
            temp_df['date'] = temp_df.index
            if self.from_date:
                temp_df = temp_df.loc[self.from_date:]
            if self.to_date:
                temp_df = temp_df.loc[:'{} 23:59'.format(self.to_date)]
            temp_df = temp_df[['date', 'marketcap']]
            return SelectorReference.from_dict(temp_df.to_dict('list'), self.symbol)
        else:
            raise Exception('request error', response.status_code)
