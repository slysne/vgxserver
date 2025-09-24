import pkgutil
__path__ = pkgutil.extend_path(__path__, __name__)
from pytest.pytest import ListTestSets, RunTestSets

import pyvgx

from . import CreateVertex
from . import NewVertex
from . import DeleteVertex
from . import OpenVertex
from . import OpenVertices
from . import CommitVertex
from . import CloseVertex
from . import CloseVertices
from . import CloseAll
from . import EscalateVertex
from . import RelaxVertex
from . import ShowOpenVertices
from . import GetOpenVertices


modules = [
    CreateVertex,
    NewVertex,
    DeleteVertex,
    OpenVertex,
    OpenVertices,
    CommitVertex,
    CloseVertex,
    CloseVertices,
    CloseAll,
    EscalateVertex,
    RelaxVertex,
    ShowOpenVertices,
    GetOpenVertices
]



def List():
    ListTestSets( modules )



def Run():
    RunTestSets( modules, __name__ )

