import pkgutil
__path__ = pkgutil.extend_path(__path__, __name__)
from pytest.pytest import ListModules, RunModules

import pyvgx

from . import Artifacts
from . import Metrics
from . import Plugin
from . import Dispatch
from . import Performance


modules = [
    Artifacts,
    Metrics,
    Plugin,
    Dispatch,
    Performance
]


def List():
    ListModules( modules )



def Run():
    pyvgx.system.Initialize( __name__ )
    RunModules( modules )
    pyvgx.system.Unload()
