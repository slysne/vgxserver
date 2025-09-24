import pkgutil
__path__ = pkgutil.extend_path(__path__, __name__)
from pytest.pytest import ListModules, RunModules

import pyvgx

from . import FeatureVector
from . import EuclideanVector


modules = [
    FeatureVector,
    EuclideanVector
]


def List():
    ListModules( modules )


def Run():
    # NOTE: We will initialize and unload within each module
    RunModules( modules )
