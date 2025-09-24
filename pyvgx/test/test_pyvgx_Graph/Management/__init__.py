import pkgutil
__path__ = pkgutil.extend_path(__path__, __name__)
from pytest.pytest import ListTestSets, RunTestSets

import pyvgx

from . import Order
from . import Size
from . import Enumerator
from . import Relationships
from . import VertexTypes
from . import PropertyKeys
from . import PropertyStringValues
from . import GetMemoryUsage
from . import Save
from . import Truncate
from . import Lock
from . import ReadonlyGraph
from . import EventProcessor


modules = [
    Order,
    Size,
    Enumerator,
    Relationships,
    VertexTypes,
    PropertyKeys,
    PropertyStringValues,
    GetMemoryUsage,
    Save,
    Truncate,
    Lock,
    ReadonlyGraph,
    EventProcessor
]


def List():
    ListTestSets( modules )



def Run():
    RunTestSets( modules, __name__ )
