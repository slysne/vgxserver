import pkgutil
__path__ = pkgutil.extend_path(__path__, __name__)
from pytest.pytest import ListTestSets, RunTestSets


import pyvgx

from . import Close
from . import Escalate
from . import Readable
from . import Readonly
from . import Relax
from . import Writable


modules = [
  Close,
  Escalate,
  Readable,
  Readonly,
  Relax,
  Writable
]



def List():
    ListTestSets( modules )



def Run():
    RunTestSets( modules, __name__ )
