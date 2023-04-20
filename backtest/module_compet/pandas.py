import functools

import pandas as pd
import pkg_resources

pandas_version = pkg_resources.get_distribution('pandas').version


def custom_datetime(function):
    @functools.wraps(function)
    def run(*args, **kwargs):
        if pandas_version >= '2.0.0':
            if 'format' not in kwargs:
                kwargs['format'] = 'mixed'
        return function(*args, **kwargs)
    return run


pd.to_datetime = custom_datetime(pd.to_datetime)
