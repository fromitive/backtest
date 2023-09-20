import dataclasses

from backtest.module_compet.pandas import pd


@dataclasses.dataclass
class StockData:
    symbol: str = ""
    unit: str = "D"
    data: pd.DataFrame = dataclasses.field(default_factory=pd.DataFrame)

    @classmethod
    def from_dict(cls, dict_data, symbol="", unit="D"):
        df = pd.DataFrame(dict_data, columns=["open", "high", "low", "close", "volume", "date"])
        df.drop_duplicates(inplace=True)
        df.set_index("date", inplace=True)
        df.index = pd.to_datetime(df.index)
        df.index = df.index.strftime("%Y-%m-%d %H:%M:%S")
        df = df.astype({"open": "float", "high": "float", "close": "float", "low": "float", "volume": "float"})
        df.sort_index(ascending=True, inplace=True)
        return cls(symbol=symbol, data=df, unit=unit)

    @classmethod
    def from_csv(cls, csv_path, symbol="", unit="D"):
        df = pd.read_csv(csv_path)
        df.drop_duplicates(inplace=True)
        df.set_index("date", inplace=True)
        df.index = pd.to_datetime(df.index)
        df.index = df.index.strftime("%Y-%m-%d %H:%M:%S")
        return cls(symbol=symbol, data=df, unit=unit)

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
        df = df.drop_duplicates(subset="date", keep="first")
        df = df.set_index("date")
        # fill missing date value with mean value
        df = df.interpolate()
        df = df.fillna(0.0)
        self.data = df
        return self

    def __name__(self):
        return "stockdata"
