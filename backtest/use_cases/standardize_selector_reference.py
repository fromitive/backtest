from typing import List

from backtest.domains.selector_reference import SelectorReference
from backtest.util.selector_reference_util import generate_empty_selector_reference


def standardize_selector_reference(selector_reference_list: List[SelectorReference]):
    if len(selector_reference_list) == 0:
        return []
    indexes = set()
    for selector_reference in selector_reference_list:
        indexes.update(list(selector_reference.data.index))
    indexes = list(indexes)
    indexes.sort()
    for selector_reference in selector_reference_list:
        empty_selector_reference = generate_empty_selector_reference(
            indexes=indexes, symbol=selector_reference.symbol, columns=selector_reference.data.columns
        )
        selector_reference += empty_selector_reference
