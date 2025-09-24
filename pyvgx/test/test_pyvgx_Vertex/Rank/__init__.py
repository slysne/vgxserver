import pkgutil
__path__ = pkgutil.extend_path(__path__, __name__)
from pytest.pytest import ListTestSets, RunTestSets

import pyvgx

from . import SetRank
from . import GetRank


modules = [
  SetRank,
  GetRank
]



def List():
    ListTestSets( modules )



def Run():
    RunTestSets( modules, __name__ )

