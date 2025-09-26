###############################################################################
# 
# VGX Server
# Distributed engine for plugin-based graph and vector search
# 
# Module:  pyvgx
# File:    CommitVertex.py
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

from pytest.pytest import RunTests, Expect, TestFailed
from . import _vertex_test_support as Support
from pyvgx import *
import pyvgx

graph = None



def TEST_CommitVertex():
    """
    pyvgx.Graph.CommitVertex()
    test_level=3101
    """
    # Reset
    graph.Truncate()

    # Implicitly create new vertices
    A = graph.OpenVertex( "A", "w" )
    B = graph.OpenVertex( "B", "w", timeout=0 )
    C = graph.OpenVertex( "C", "w", timeout=100 )
    D = graph.OpenVertex( "D", "w", timeout=100 )
    E = graph.OpenVertex( "E", "w", timeout=100 )

    Expect( graph.HasVertex( "A" ) is True,         "Vertex A should exist in graph" )

    # Commit B
    B.Commit()
    Expect( graph.HasVertex( "B" ) is True,         "Vertex B should exist in graph" )

    # Connect to uncommitted terminal
    B = graph.OpenVertex( "B" )
    Expect( graph.Connect( B, "to", C ) == 1,       "Terminal should auto-commit" )
    Expect( B.degree == 1,                          "B has one arc" )
    Expect( C.degree == 1,                          "C has one arc" )
    graph.CloseVertex( C ) 
    Expect( graph.HasVertex( "C" ) is True,         "Vertex C should exist in graph" )

    # Connect uncommitted initial to uncommitted terminal
    Expect( graph.Connect( D, "to", E ) == 1,       "Initial and terminal should auto-commit" )
    Expect( D.degree == 1,                          "D has one arc" )
    Expect( E.degree == 1,                          "E has one arc" )
    graph.CloseVertex( D )
    graph.CloseVertex( E )
    Expect( graph.HasVertex( "D" ) is True,         "Vertex D should exist in graph" )
    Expect( graph.HasVertex( "E" ) is True,         "Vertex E should exist in graph" )

    graph.CloseAll()



def Run( name ):
    """
    Run the tests in this module
    """
    global graph
    graph = pyvgx.Graph( name )
    RunTests( [__name__] )
    graph.Close()
    del graph
