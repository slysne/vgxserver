import pkgutil
__path__ = pkgutil.extend_path(__path__, __name__)
from pytest.pytest import ListTestSets, RunTestSets

import pyvgx

from . import Fields
from . import Sort
from . import Rank
from . import Select

modules = [
    Fields,
    Sort,
    Rank,
    Select
]


def List():
    ListTestSets( modules )


def Run():
    RunTestSets( modules, __name__ )
