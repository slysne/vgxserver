import pkgutil
__path__ = pkgutil.extend_path(__path__, __name__)
from pytest.pytest import ListTestSets, RunTestSets

import pyvgx

from . import GraphObject


modules = [
  GraphObject
]


def List():
    ListTestSets( modules )


def Run():
    RunTestSets( modules, __name__ )


