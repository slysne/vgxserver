import pkgutil
__path__ = pkgutil.extend_path(__path__, __name__)
from pytest.pytest import ListTestSets, RunTestSets

import pyvgx

from . import ShowVertex
from . import VertexDescriptor
from . import DebugPrintAllocators
from . import DebugCheckAllocators
from . import DebugGetObjectByAddress
from . import DebugFindObjectByIdentifier


modules = [
    ShowVertex,
    VertexDescriptor,
    DebugPrintAllocators,
    DebugCheckAllocators,
    DebugGetObjectByAddress,
    DebugFindObjectByIdentifier
]



def List():
    ListTestSets( modules )


def Run():
    RunTestSets( modules, __name__ )
