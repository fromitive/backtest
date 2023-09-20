import dataclasses
import typing


@dataclasses.dataclass
class TradeStrategyPackage:
    strategy_function: typing.Callable = None
    strategy_function_name: str = "default_name"
    strategy_options: dict = dataclasses.field(default_factory=dict)
    extra_column: typing.List[str] = dataclasses.field(default_factory=list)
    extra_function: typing.Callable = None
    min_observe_time_idx: int = -6
    max_observe_time_idx: int = -3
    market_timing: typing.List[int] = dataclasses.field(default_factory=list)
    message_function: typing.Callable = None
    buy_bid_ask: str = "bid"
    sell_prior: int = 4
