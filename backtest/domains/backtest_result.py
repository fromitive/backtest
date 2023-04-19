
import dataclasses

import pandas as pd


@dataclasses.dataclass
class BacktestResult:
    value: pd.DataFrame = dataclasses.field(default_factory=pd.DataFrame)

    @classmethod
    def from_dict(cls, adict):
        df = pd.DataFrame(adict,
                          columns=['stock_bucket', 'total_profit', 'total_potential_profit', 'total_stock_count', 'stock_count', 'date'])
        df.set_index('date', inplace=True)
        df.index = pd.to_datetime(df.index, format='mixed')
        return cls(value=df)
