import dataclasses
from enum import Enum

import pandas as pd


class SelectorResultColumnType(Enum):
    SELECT = 1
    KEEP = 2


@dataclasses.dataclass
class SelectorResult:
    value: pd.DataFrame = dataclasses.field(default_factory=pd.DataFrame)

    @classmethod
    def from_dict(cls, adict):
        df = pd.DataFrame(adict,
                          columns=[item for item in adict.keys()])
        df.set_index('date', inplace=True)
        df.index = pd.to_datetime(df.index, format='mixed')
        return cls(value=df)

    def __len__(self):
        return len(self.value)
