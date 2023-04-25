from datetime import datetime
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


def generate_empty_selector_result_data(symbol_list: List[str], from_date: str = '1990-01-01', to_date: str = ''):
    if to_date == '':
        to_date = datetime.now().strftime('%Y-%m-%d')
    date_series = pd.date_range(start=from_date, end=to_date)
    df = pd.DataFrame(columns=symbol_list,
                      index=date_series)
    df = df.fillna('dummy')
    for column in df.columns:
        df[column] = df[column].apply(lambda cell: {
            SelectorResultColumnType.KEEP: 0, SelectorResultColumnType.SELECT: 0})
    return df


def sum_selector_result(row: pd.Series, selector_result_df: pd.DataFrame):
    # r.name = '2020-01-01'
    # r.axes = symbol
    symbols = row.index.to_list()
    for symbol in symbols:
        if symbol in selector_result_df.columns.to_list():
            column_type, weight = selector_result_df.at[row.name, symbol]
            row[symbol][column_type] += weight
    return row


def _sum_total_selector_result(row: pd.Series):
    symbols = row.index.to_list()
    for symbol in symbols:
        total_result = row[symbol]
        row[symbol] = max(total_result, key=total_result.get)
    return row


def _fill_na_with_keep(selector_result_df: pd.DataFrame) -> None:
    for column in selector_result_df.columns:
        selector_result_df[column] = selector_result_df[column].fillna(
            {i: (SelectorResultColumnType.KEEP, 0) for i in selector_result_df.index})


def selector_execute(selector_list: List[Selector], symbol_list: List, from_date='1990-01-01', to_date=''):
    if to_date == '':
        to_date = datetime.strftime(datetime.now(), "%Y-%m-%d")
    selector_total_result_df = generate_empty_selector_result_data(
        symbol_list, from_date=from_date, to_date=to_date)

    try:
        for selector in selector_list:
            if not selector.selector_function:
                selector.selector_function = basic_function
            response = selector.selector_function(
                symbol_list=symbol_list, weight=selector.weight, name=selector.name, reference=selector.reference, **selector.options)
            if isinstance(response, ResponseFailure):
                return ResponseFailure(ResponseTypes.SYSTEM_ERROR, "fail to exectue selector_function")
            else:
                selector_result_df = response.value.value
                selector_result_df = selector_result_df.reindex(
                    selector_total_result_df.index)
                _fill_na_with_keep(selector_result_df)
                selector_total_result_df = selector_total_result_df.apply(
                    lambda row: sum_selector_result(row, selector_result_df=selector_result_df), axis=1)
        selector_total_result_df = selector_total_result_df.apply(
            lambda row: _sum_total_selector_result(row), axis=1)
        selector_total_result = SelectorResult(selector_total_result_df)
        return ResponseSuccess(selector_total_result)
    except Exception as e:
        return ResponseFailure(ResponseTypes.SYSTEM_ERROR, e)
