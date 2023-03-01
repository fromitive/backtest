import dataclasses
import pandas as pd
from datetime import datetime


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
        df.index = pd.to_datetime(df.index).normalize()
        df = df.astype({'open': 'float',
                        'high': 'float',
                        'close': 'float',
                        'low': 'float',
                        'volume': 'float'})
        df.sort_index(ascending=True, inplace=True)
        return cls(symbol=symbol, data=df)

    def __len__(self):
        return len(self.data)
