import dataclasses
import typing

from backtest.domains.selector_reference import SelectorReference


@dataclasses.dataclass
class Selector:
    name: str = ''
    weight: float = 0.0
    selector_function: typing.Callable = None
    max_select_stock_num: int = 0
    options: dict = dataclasses.field(default_factory=dict)
    reference: SelectorReference = dataclasses.field(
        default_factory=SelectorReference)
