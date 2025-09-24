import pkgutil
__path__ = pkgutil.extend_path(__path__, __name__)
from pytest.pytest import ListModules, RunModules

import pyvgx

from . import lib_comlib
from . import lib_framehash

modules = [
    lib_comlib,
    lib_framehash
]


def List():
    ListModules( modules )


def Run():
    RunModules( modules )
    pyvgx.system.Unload()
