import dataclasses
import pandas as pd
from enum import Enum


class StrategyResultColumnType(Enum):
    BUY = 1
    SELL = 2
    KEEP = 3


@dataclasses.dataclass
class StrategyResult:
    value: pd.DataFrame = pd.DataFrame(
        columns=[''], index=pd.DatetimeIndex([]))

    @classmethod
    def from_dict(cls, adict):
        df = pd.DataFrame(adict,
                          columns=[item for item in adict.keys()])
        df.set_index('date', inplace=True)
        df.index = pd.to_datetime(df.index)
        return cls(value=df)
