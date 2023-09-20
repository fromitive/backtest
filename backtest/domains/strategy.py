import dataclasses
import typing
from enum import Enum


class StrategyExecuteFlagType(Enum):
    SELLONLY = 1
    BUYONLY = 2
    NORMAL = 3


@dataclasses.dataclass
class Strategy:
    name: str = ""
    function: typing.Callable = None
    weight: int = 0
    target: str = "ALL"
    after: bool = False
    flag: StrategyExecuteFlagType = StrategyExecuteFlagType.NORMAL
    inverse: bool = False
    options: dict = dataclasses.field(default_factory=dict)
