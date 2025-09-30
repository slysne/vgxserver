###############################################################################
# 
# VGX Server
# Distributed engine for plugin-based graph and vector search
# 
# Module:  pyvgxtest
# File:    CloseAll.py
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

from pyvgxtest.pyvgxtest import RunTests, Expect, TestFailed
from . import _vertex_test_support as Support
import random
from pyvgx import *
import pyvgx

graph = None




###############################################################################
# TEST_CloseAll
#
###############################################################################
def TEST_CloseAll():
    """
    pyvgx.Graph.CloseAll()
    test_level=3101
    """
    # Reset
    graph.Truncate()

    # Populate
    N = 10000
    for n in range( N ):
        graph.CreateVertex( "node_%d" % n )

    Expect( len( graph.GetOpenVertices() ) == 0,    "no open vertices in empty graph" )

    # Open many vertices, recursively
    VERTICES = {}
    for n in range( 100000 ):
        node_wl = "node_%d" % random.randint( 0, N//2-1 )
        if not node_wl in VERTICES:
            VERTICES[ node_wl ] = []
        VERTICES[ node_wl ].append( graph.OpenVertex( node_wl, "w" ) )
        node_ro = "node_%d" % random.randint( N//2, N-1 )
        if not node_ro in VERTICES:
            VERTICES[ node_ro ] = []
        VERTICES[ node_ro ].append( graph.OpenVertex( node_ro, "w" ) )

    Expect( len( graph.GetOpenVertices() ) == len( VERTICES ),      "number of unique vertex names" )

    n_locks = sum( ( len(refs) for refs in list(VERTICES.values()) ) )
    
    # Close all, recursively until no locks are held by current thread
    n_released = graph.CloseAll()

    Expect( n_released == n_locks,      "number of released locks %d should match number of acquired locks %d" % ( n_released, n_locks ) )

    del VERTICES

    graph.Truncate()



###############################################################################
# Run
#
###############################################################################
def Run( name ):
    """
    Run the tests in this module
    """
    global graph
    graph = pyvgx.Graph( name )
    RunTests( [__name__] )
    graph.Close()
    del graph
