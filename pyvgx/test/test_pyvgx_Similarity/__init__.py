import pkgutil
__path__ = pkgutil.extend_path(__path__, __name__)
from pytest.pytest import ListModules, RunModules

import pyvgx

from . import Similarity


modules = [
    Similarity
]


def List():
    ListModules( modules )



def Run():
    # NOTE: Similarity tests use Feature-vector mode
    pyvgx.system.Initialize( __name__, euclidean=False )
    RunModules( modules )
    pyvgx.system.Unload()