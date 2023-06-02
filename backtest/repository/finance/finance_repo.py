import FinanceDataReader as fdr

from backtest.domains.stockdata import StockData


# df = fdr.DataReader('US500', '2018-01-01', '2018-03-30')
# df.to_dict('list')
class FinanceRepo:
    def __init__(self):
        self.order_currency = 'US500'
        self.from_date = ''
        self.to_date = ''

    def get(self, filters=None):
        if filters:
            filter = list(filters.keys())
            self.order_currency = filters['order__eq'] if 'order__eq' in filter else 'US500'
            self.from_date = filters['from__eq'] if 'from__eq' in filter else ''
            self.to_date = filters['to__eq'] if 'to__eq' in filter else ''

        temp_df = fdr.DataReader(
            self.order_currency, self.from_date, self.to_date)
        temp_df.rename(columns={'Open': 'open', 'High': 'high',
                                'Low': 'low', 'Close': 'close', 'Volume': 'volume'}, inplace=True)
        temp_df.index.name = 'date'
        temp_df.index = temp_df.index.strftime('%Y-%m-%d %H:%M:%S')
        temp_df = temp_df[['open', 'high', 'low', 'close', 'volume']]
        return StockData(symbol=self.order_currency, data=temp_df)
