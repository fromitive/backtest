from backtest.domains.strategy import Strategy


def test_init_strategy_with_empty_parameters():
    strategy = Strategy()
    assert strategy.name == ""
    assert strategy.function is None
    assert strategy.weight == 0
    assert strategy.target == "ALL"


def test_init_strategy_with_parameters():
    strategy = Strategy(
        name="test strategy", weight=1, target="TARGET", options={"option1": "value1", "option2": "value2"}
    )
    assert strategy.name == "test strategy"
    assert strategy.weight == 1
    assert strategy.function is None
    assert strategy.target == "TARGET"
    assert strategy.options == {"option1": "value1", "option2": "value2"}
