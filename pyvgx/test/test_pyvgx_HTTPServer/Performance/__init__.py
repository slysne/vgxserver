import pkgutil
__path__ = pkgutil.extend_path(__path__, __name__)
from pytest.pytest import ListTestSets, RunTestSets

import pyvgx

from . import Load

PORT = 9747

modules = [
    Load,
]



def List():
    ListTestSets( modules )



def Run():
    # Start HTTP Server
    pyvgx.system.StartHTTP( PORT )
    try:
        # Run tests
        RunTestSets( modules, __name__ )
    finally:
        # Stop HTTP Server
        pyvgx.system.StopHTTP()

