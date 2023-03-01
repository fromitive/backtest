import dataclasses
import pandas as pd
from enum import Enum


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
        df.index = pd.to_datetime(df.index)
        return cls(value=df, target=target)

    def __len__(self):
        return len(self.value)
