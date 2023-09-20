import pytest


@pytest.fixture(scope="function")
def dict_stock_data():
    return {
        "open": [0.00000, "1.11111"],
        "high": [0.00000, "1.11111"],
        "low": [0.00000, "1.11111"],
        "close": [0.00000, "1.11111"],
        "volume": [0.00000, "1.11111"],
        "date": [1388070000000, "2021-11-11"],
    }
