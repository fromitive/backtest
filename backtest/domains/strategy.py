import dataclasses
import typing
from enum import Enum


class StrategyExecuteFlagType(Enum):
    INVERSE = 1
    SELLONLY = 2
    BUYONLY = 3
    NORMAL = 4


@dataclasses.dataclass
class Strategy:
    name: str = ''
    function: typing.Callable = None
    weight: int = 0
    target: str = 'ALL'
    after: bool = False
    flag: StrategyExecuteFlagType = StrategyExecuteFlagType.NORMAL
    options: dict = dataclasses.field(default_factory=dict)
