###############################################################################
# 
# VGX Server
# Distributed engine for plugin-based graph and vector search
# 
# Module:  pyvgx
# File:    __init__.py
# Author:  Stian Lysne <...>
# 
# Copyright © 2025 Rakuten, Inc.
# 
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# 
#     http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
# 
###############################################################################

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



###############################################################################
# List
#
###############################################################################
def List():
    """
    """
    ListTestSets( modules )



###############################################################################
# Run
#
###############################################################################
def Run():
    """
    """
    RunTestSets( modules, __name__ )
