import pkgutil
__path__ = pkgutil.extend_path(__path__, __name__)
from pytest.pytest import ListTestSets, RunTestSets

import pyvgx

from . import IsVirtual
from . import AsDict
from . import Descriptor
from . import Commit


modules = [
  IsVirtual,
  AsDict,
  Descriptor,
  Commit
]



def List():
    ListTestSets( modules )



def Run():
    RunTestSets( modules, __name__ )
