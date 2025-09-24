import pkgutil
__path__ = pkgutil.extend_path(__path__, __name__)
from pytest.pytest import ListTestSets, RunTestSets

import pyvgx

from . import CoreEvaluator
from . import BasicEvaluator
from . import MathFunctions
from . import VectorArithmetic

modules = [
    CoreEvaluator,
    BasicEvaluator,
    MathFunctions,
    VectorArithmetic,
]


def List():
    ListTestSets( modules )


def Run():
    RunTestSets( modules, __name__ )
