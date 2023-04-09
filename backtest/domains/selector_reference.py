import dataclasses
import pandas as pd


@dataclasses.dataclass
class SelectorReference:
    symbol: str = ""
    data: pd.DataFrame = dataclasses.field(default_factory=pd.DataFrame)

    @classmethod
    def from_dict(cls, dict_data, symbol='', type_options: dict = {}):
        df = pd.DataFrame(dict_data)
        df.drop_duplicates(inplace=True)
        df.set_index('date', inplace=True)
        df.index = pd.to_datetime(df.index).normalize()
        if len(type_options):
            df = df.astype(type_options)
        else:
            df = df.astype(float)
        df.sort_index(ascending=True, inplace=True)
        return cls(symbol=symbol, data=df)

    def __len__(self):
        return len(self.data)

    def __add__(self, o):
        if self.data.index[0] > o.data.index[0]:
            self.data = o.data.add(self.data, fill_value=0.0)
        else:
            self.data.add(self.data, fill_value=0.0)
        return self
