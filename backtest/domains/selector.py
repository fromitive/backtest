import dataclasses
import typing

from backtest.domains.selector_reference import SelectorReference


@dataclasses.dataclass
class Selector:
    name: str = ""
    weight: float = 0.0
    selector_function: typing.Callable = None
    options: dict = dataclasses.field(default_factory=dict)
    reference: SelectorReference = dataclasses.field(default_factory=SelectorReference)
