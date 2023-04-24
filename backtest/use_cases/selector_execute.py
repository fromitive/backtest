from typing import List

from backtest.domains.selector import Selector
from backtest.domains.selector_reference import SelectorReference
from backtest.domains.selector_result import (SelectorResult,
                                              SelectorResultColumnType)
from backtest.module_compet.pandas import pd
from backtest.response import ResponseFailure, ResponseSuccess, ResponseTypes


def basic_function(symbol_list: List[str], weight: int, name: str, reference: SelectorReference):
    response = SelectorResult(value=pd.DataFrame(
        index=reference.data.index, columns=[symbol_list]))
    for symbol in symbol_list:
        response.value[symbol] = [(
            SelectorResultColumnType.KEEP, weight)] * len(reference.data)
    return response


def rank_marketcap_function(symbol_list: List[str], weight: int, name: str, reference: SelectorReference, top_rate: float = 0.4, select_pick: int = 10):
    rank_df = reference.data.rank(axis=1)
    nlargest_number = int(len(symbol_list) * top_rate)

    def sample_function(r: pd.Series, symbol_list):
        target_list = list(r.nlargest(
            nlargest_number).nsmallest(select_pick).index)
        return [(SelectorResultColumnType.SELECT, weight) if col_name in target_list else (SelectorResultColumnType.KEEP, weight) for col_name in symbol_list]
    rank_result = rank_df.apply(lambda r:  sample_function(
        r, symbol_list=rank_df.columns), axis=1)
    rank_df[rank_df.columns] = list(rank_result)
    response = SelectorResult(value=rank_df)
    return response


def selector_execute(selector: Selector, symbol_list: List):
    try:
        if not selector.selector_function:
            selector.selector_function = basic_function
        response = selector.selector_function(
            symbol_list=symbol_list, weight=selector.weight, name=selector.name, reference=selector.reference, **selector.options)
        return ResponseSuccess(response)
    except Exception as e:
        return ResponseFailure(ResponseTypes.SYSTEM_ERROR, e)
