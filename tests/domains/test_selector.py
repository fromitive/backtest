from backtest.domains.selector import Selector


def test_init_selector_without_parameters():
    selector = Selector()
    assert selector.name == ""
    assert selector.weight == 0.0
    assert selector.selector_function is None
    assert isinstance(selector.options, dict)


def test_init_selector_with_parameters():
    selector = Selector(
        name="marketcap_currency", weight=1, selector_function=None, options={"param1": "test", "param2": 1.1}
    )
    assert selector.name == "marketcap_currency"
    assert selector.weight == 1.0
    assert selector.selector_function is None
    assert selector.options["param1"] == "test"
    assert selector.options["param2"] == 1.1
