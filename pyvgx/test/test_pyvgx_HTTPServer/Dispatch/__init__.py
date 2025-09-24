import pkgutil
__path__ = pkgutil.extend_path(__path__, __name__)
from pytest.pytest import ListTestSets, RunTestSets

import pyvgx

from . import SimpleProxy
from . import TopDispatcher
from . import FeedPartials

modules = [
    SimpleProxy,
    TopDispatcher,
    FeedPartials
]


def List():
    ListTestSets( modules )


def Run():
    # Run tests
    RunTestSets( modules, __name__ )

