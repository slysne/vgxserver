import pkgutil
__path__ = pkgutil.extend_path(__path__, __name__)
from pytest.pytest import ListTestSets, RunTestSets

import pyvgx

from . import CharEncoding
from . import Wildcards
from . import Memory
from . import Filter
from . import Collector
from . import Limexec


modules = [
    CharEncoding,
    Wildcards,
    Memory,
    Filter,
    Collector,
    Limexec
]



def List():
    ListTestSets( modules )



def Run():
    RunTestSets( modules, __name__ )
