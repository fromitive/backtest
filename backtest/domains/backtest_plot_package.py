
import dataclasses
from typing import Dict


@dataclasses.dataclass
class BacktestPlotPackage:
    package_data_bucket: Dict[str, any] = dataclasses.field(
        default_factory=dict)
    package_option_bucket: Dict[str, any] = dataclasses.field(
        default_factory=dict)
