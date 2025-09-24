import pkgutil
__path__ = pkgutil.extend_path(__path__, __name__)
from pytest.pytest import ListModules, RunModules

import pyvgx

from . import Graph
from . import Vertex
from . import Evaluator
from . import Special
from . import Arc
from . import Query
from . import Management
from . import Debugging
from . import Results
from . import Multithreading

modules = [
  Graph,
  Vertex,
  Evaluator,
  Special,
  Arc,
  Query,
  Management,
  Debugging,
  Results,
  Multithreading
]


def List():
    ListModules( modules )


def Run():
    # NOTE: Graph tests use Feature-vector mode
    pyvgx.system.Initialize( __name__, euclidean=False )
    RunModules( modules )
    pyvgx.system.Unload()
