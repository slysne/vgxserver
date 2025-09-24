import pkgutil
__path__ = pkgutil.extend_path(__path__, __name__)
from pytest.pytest import ListTestSets, RunTestSets

import pyvgx

from . import Parallel
from . import ExpirationFight
from . import ExpirationChaser
from . import RandomExpiration
from . import Chaos


modules = [
    Parallel,
    ExpirationFight,
    ExpirationChaser,
    RandomExpiration,
    Chaos
]


def List():
    ListTestSets( modules )


def Run():
    RunTestSets( modules, __name__ )

