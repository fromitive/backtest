import datetime
from collections.abc import Mapping


class StockDataFromRepoInvalidRequest:
    def __init__(self):
        self.errors = []

    def add_error(self, parameter, message):
        self.errors.append({"parameter": parameter, "message": message})

    def has_errors(self):
        return len(self.errors) > 0

    def __bool__(self):
        return False


class StockDataFromRepoValidRequest:
    def __init__(self, filters=None):
        self.filters = filters

    def __bool__(self):
        return True


def _validate_date_format(date_text: str):
    try:
        datetime.datetime.strptime(date_text, "%Y-%m-%d")
        return True
    except ValueError:
        return False


def build_stock_data_from_repo_request(filters=None):
    accepted_filters = [
        "order__eq",
        "payment__eq",
        "chart_interval__eq",
        "from__eq",
        "to__eq",
        "start_time__eq",
        "end_time__eq",
    ]
    accepted_chart_interval_filters = [
        "24h",
        "1d",
        "30m",
        "1m",
        "3m",
        "5m",
        "10m",
        "15m",
        "30m",
        "60m",
        "240m",
        "1h",
        "6h",
        "12h",
    ]
    accepted_payment_filters = ["KRW", "USDT"]
    invalid_req = StockDataFromRepoInvalidRequest()

    if filters is not None:
        if not isinstance(filters, Mapping):
            invalid_req.add_error("filters", "Is not iterable")
            return invalid_req

        for key, value in filters.items():
            if key not in accepted_filters:
                invalid_req.add_error("filters", "Key {} cannot be used".format(key))

            if key == "chart_interval__eq":
                if value not in accepted_chart_interval_filters:
                    invalid_req.add_error("filters", "key {} - value {} cannot be used".format(key, value))

            if key == "payment__eq":
                if value not in accepted_payment_filters:
                    invalid_req.add_error("filters", "Key {} - value {} cannot be used".format(key, value))
            if key in ["from__eq", "to__eq"]:
                if not _validate_date_format(value):
                    invalid_req.add_error(
                        "filters", "Key {} - value {} must formating (YYYY-MM-DD) e.g: 1990-01-02".format(key, value)
                    )
                elif key == "from__eq":
                    if datetime.datetime.strptime(value, "%Y-%m-%d") > datetime.datetime.now():
                        invalid_req.add_error("filters", "Key {} - value not accepted future date".format(key))

        if invalid_req.has_errors():
            return invalid_req

    return StockDataFromRepoValidRequest(filters=filters)
