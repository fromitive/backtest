import dataclasses
from enum import Enum

import pandas as pd


class StrategyResultColumnType(Enum):
    BUY = 1
    SELL = 2
    KEEP = 3


@dataclasses.dataclass
class StrategyResult:
    value: pd.DataFrame = dataclasses.field(default_factory=pd.DataFrame)
    target: str = 'ALL'

    @classmethod
    def from_dict(cls, adict, target='ALL'):
        df = pd.DataFrame(adict,
                          columns=[item for item in adict.keys()])
        df.set_index('date', inplace=True)
        df.index = pd.to_datetime(df.index, format='mixed')
        return cls(value=df, target=target)

    def __len__(self):
        return len(self.value)
