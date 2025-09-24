import pkgutil
__path__ = pkgutil.extend_path(__path__, __name__)
from pytest.pytest import ListTestSets, RunTestSets

import pyvgx

from . import Core
from . import Connect
from . import Disconnect
from . import Count
from . import Accumulate


modules = [
  Core,
  Connect,
  Disconnect,
  Count,
  Accumulate
]



def List():
    ListTestSets( modules )


def Run():
    RunTestSets( modules, __name__ )
