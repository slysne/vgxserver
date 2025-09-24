import pkgutil
__path__ = pkgutil.extend_path(__path__, __name__)
from pytest.pytest import ListTestSets, RunTestSets

import pyvgx

from . import SetExpiration
from . import IsExpired
from . import GetExpiration
from . import ClearExpiration
from . import Reschedule


modules = [
  SetExpiration,
  IsExpired,
  GetExpiration,
  ClearExpiration,
  Reschedule
]



def List():
    ListTestSets( modules )



def Run():
    RunTestSets( modules, __name__ )
