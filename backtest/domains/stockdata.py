import dataclasses

import pandas as pd


@dataclasses.dataclass
class StockData:
    symbol: str = ""
    data: pd.DataFrame = dataclasses.field(default_factory=pd.DataFrame)

    @classmethod
    def from_dict(cls, dict_data, symbol=''):
        df = pd.DataFrame(dict_data, columns=['open', 'high', 'low', 'close',
                                              'volume', 'date'])
        df.drop_duplicates(inplace=True)
        df.set_index('date', inplace=True)
        df.index = pd.to_datetime(df.index, dayfirst=True).normalize()
        df = df.astype({'open': 'float',
                        'high': 'float',
                        'close': 'float',
                        'low': 'float',
                        'volume': 'float'})
        df.sort_index(ascending=True, inplace=True)
        return cls(symbol=symbol, data=df)

    @classmethod
    def from_csv(cls, csv_path, symbol=''):
        df = pd.read_csv(csv_path)
        df.drop_duplicates(inplace=True)
        df.set_index('date', inplace=True)
        df.index = pd.to_datetime(df.index, dayfirst=True).normalize()
        return cls(symbol=symbol, data=df)

    def to_csv(self, csv_path):
        self.data.to_csv(csv_path)

    def __len__(self):
        return len(self.data)

    def __add__(self, o):
        if self.data.index[0] > o.data.index[0]:
            self.data = o.data.add(self.data, fill_value=0.0)
        else:
            self.data.add(self.data, fill_value=0.0)
        return self

    def __name__(self):
        return 'stockdata'
