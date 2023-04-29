import dataclasses
import typing


@dataclasses.dataclass
class Strategy:
    name: str = ''
    function: typing.Callable = None
    weight: int = 0
    target: str = 'ALL'
    after: bool = False
    inverse: bool = False
    options: dict = dataclasses.field(default_factory=dict)
