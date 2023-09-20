import datetime
from collections.abc import Mapping


class SelectorReferenceFromRepoInvalidRequest:
    def __init__(self):
        self.errors = []

    def add_error(self, parameter, message):
        self.errors.append({"parameter": parameter, "message": message})

    def has_errors(self):
        return len(self.errors) > 0

    def __bool__(self):
        return False


class SelectorReferenceFromReponValidRequest:
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


def build_selector_reference_from_repo_request(filters=None):
    accepted_filters = ["symbol__eq", "from__eq", "to__eq"]
    invalid_req = SelectorReferenceFromRepoInvalidRequest()

    if filters is not None:
        if not isinstance(filters, Mapping):
            invalid_req.add_error("filters", "Is not iterable")
            return invalid_req

        for key, value in filters.items():
            if key not in accepted_filters:
                invalid_req.add_error("filters", "Key {} cannot be used".format(key))
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

    return SelectorReferenceFromReponValidRequest(filters=filters)
