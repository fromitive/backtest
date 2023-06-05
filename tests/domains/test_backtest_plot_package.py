from backtest.domains.backtest_plot_package import BacktestPlotPackage


def test_init_backtest_plot_package():
    backtest_result = BacktestPlotPackage()
    assert backtest_result.package_data_bucket == {}
    assert backtest_result.package_option_bucket == {}
