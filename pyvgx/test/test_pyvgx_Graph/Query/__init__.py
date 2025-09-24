import pkgutil
__path__ = pkgutil.extend_path(__path__, __name__)
from pytest.pytest import ListTestSets, RunTestSets

import pyvgx

from . import Query
from . import Neighborhood
from . import SyntheticArc
from . import FilterExpressions
from . import DivergentTraversal
from . import Adjacent
from . import Aggregate
from . import ArcValue
from . import Vertices
from . import VerticesType
from . import Arcs
from . import HasVertex
from . import GetVertex
from . import Degree
from . import Inarcs
from . import Outarcs
from . import Initials
from . import Terminals
from . import Search
from . import Geo
from . import Cull


modules = [
  Query,
  Neighborhood,
  SyntheticArc,
  FilterExpressions,
  DivergentTraversal,
  Adjacent,
  Aggregate,
  ArcValue,
  Vertices,
  VerticesType,
  Arcs,
  HasVertex,
  GetVertex,
  Degree,
  Inarcs,
  Outarcs,
  Initials,
  Terminals,
  Search,
  Geo,
  Cull
]


def List():
    ListTestSets( modules )


def Run():
    RunTestSets( modules, __name__ )
