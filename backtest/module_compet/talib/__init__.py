from .common import CommonTALib


class CreateModule:
    @staticmethod
    def createCompatModule() -> CommonTALib:
        module_list = [
            ("backtest.module_compet.talib.talib", "TALib"),
            ("backtest.module_compet.talib.pandas", "PANDAS"),
        ]
        for module, class_name in module_list:
            try:
                imported_module = __import__(module, fromlist=[class_name])
                print("[INFO] using {module} for calculate strategy".format(module=module))
                talib = getattr(imported_module, class_name)
                return talib()
            except ImportError:
                print("[WARNING] could not import module {module} trying next..".format(module=module))


talib = CreateModule.createCompatModule()
