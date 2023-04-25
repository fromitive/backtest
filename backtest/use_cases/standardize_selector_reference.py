from datetime import datetime
from typing import List

from backtest.domains.selector_reference import SelectorReference
from backtest.util.selector_reference_util import \
    generate_empty_selector_reference


def standardize_selector_reference(selector_reference_list: List[SelectorReference], to_date=''):
    if len(selector_reference_list) == 0:
        return []
    mindate = selector_reference_list[0].data.index[0]
    for selector_reference in selector_reference_list:
        mindate = min(selector_reference.data.index[0], mindate)
    for selector_reference in selector_reference_list:
        empty_selector_reference = generate_empty_selector_reference(
            from_date=datetime.strftime(mindate, "%Y-%m-%d"), to_date=to_date, symbol=selector_reference.symbol, columns=selector_reference.data.columns)
        selector_reference += empty_selector_reference
