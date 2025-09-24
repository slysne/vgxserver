import pkgutil
__path__ = pkgutil.extend_path(__path__, __name__)
from pytest.pytest import ListModules, RunModules

import pyvgx

from . import Vertex
from . import Access
from . import Property
from . import Type
from . import Vector
from . import Expiration
from . import Miscellaneous
from . import Rank
from . import Query


modules = [
    Vertex,
    Access,
    Property,
    Type,
    Vector,
    Expiration,
    Miscellaneous,
    Rank,
    Query
]



def List():
    ListModules( modules )


def Run():
    # NOTE: Vector related tests use Feature-vector mode
    pyvgx.system.Initialize( __name__, euclidean=False )
    RunModules( modules )
    pyvgx.system.Unload()