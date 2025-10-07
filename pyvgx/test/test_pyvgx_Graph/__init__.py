###############################################################################
# 
# VGX Server
# Distributed engine for plugin-based graph and vector search
# 
# Module:  pyvgx.test
# File:    __init__.py
# Author:  Stian Lysne slysne.dev@gmail.com
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
from pyvgxtest.pyvgxtest import ListModules, RunModules

import pyvgx

from . import Graph
from . import Vertex
from . import Evaluator
from . import Special
from . import Arc
from . import Query
from . import Management
from . import Debugging
from . import Results
from . import Multithreading

modules = [
  Graph,
  Vertex,
  Evaluator,
  Special,
  Arc,
  Query,
  Management,
  Debugging,
  Results,
  Multithreading
]



###############################################################################
# List
#
###############################################################################
def List():
    """
    """
    ListModules( modules )



###############################################################################
# Run
#
###############################################################################
def Run():
    """
    """
    # NOTE: Graph tests use Feature-vector mode
    pyvgx.system.Initialize( __name__, euclidean=False )
    RunModules( modules )
    pyvgx.system.Unload()
