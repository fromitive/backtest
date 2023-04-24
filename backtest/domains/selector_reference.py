import dataclasses

import numpy as np
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
        df.index = pd.to_datetime(df.index, dayfirst=True).normalize()
        if len(type_options):
            df = df.astype(type_options)
        else:
            df = df.astype(float)
        df.sort_index(ascending=True, inplace=True)
        return cls(symbol=symbol, data=df)

    @classmethod
    def from_csv(cls, csv_path, symbol='', type_options: dict = {}):
        df = pd.read_csv(csv_path)
        df.drop_duplicates(inplace=True)
        df.set_index('date', inplace=True)
        df.index = pd.to_datetime(df.index, dayfirst=True).normalize()
        if len(type_options):
            df = df.astype(type_options)
        else:
            df = df.astype(float)
        df.sort_index(ascending=True, inplace=True)
        return cls(symbol=symbol, data=df)

    def to_csv(self, csv_path):
        self.data.to_csv(csv_path)

    def __len__(self):
        return len(self.data)

    def __add__(self, o):
        if self.data.index[0] > o.data.index[0] or len(self.data.index) < len(o.data.index):
            self.data = o.data.add(self.data)
        else:
            self.data.add(self.data)
        df = self.data
        # drop duplicate value

        df = df.reset_index()

        df = df.drop_duplicates(subset='date', keep='first')
        df = df.set_index('date')
        # fill missing date value with mean value
        df = df.interpolate()
        df = df.fillna(0.0)
        self.data = df
        return self

    def __name__(self):
        return 'selector_reference'
