###############################################################################
# 
# VGX Server
# Distributed engine for plugin-based graph and vector search
# 
# Module:  pyvgxtest
# File:    NewVertex.py
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
from pyvgx import *
import pyvgx
import time

graph = None




###############################################################################
# TEST_NewVertex
#
###############################################################################
def TEST_NewVertex():
    """
    pyvgx.Graph.NewVertex()
    test_level=3101
    """
    Support._NewVertex_or_CreateVertex( graph, "NewVertex" )




###############################################################################
# TEST_NewVertex_with_attr
#
###############################################################################
def TEST_NewVertex_with_attr():
    """
    pyvgx.Graph.NewVertex()
    with type, lifespan, properties
    t_nominal=18
    test_level=3101
    """
    g = graph
    g.Truncate()
    node10 = "gone_in_ten_seconds"
    node12 = "gone_in_twelve_seconds"
    node14 = "gone_in_fourteen_seconds"
    node16 = "gone_in_sixteen_seconds"
    t0 = g.ts
    V10 = g.NewVertex( node10, type="node", lifespan=10, properties={ 'x':10 } )
    V12 = g.NewVertex( properties={ 'x':12 }, type="node", lifespan=12, id=node12 )
    V14 = g.NewVertex( properties={ 'x':14 }, lifespan=14, id=node14, type="node" )
    V16 = g.NewVertex( node16, "node", 16, { 'x':16 } )

    Expect( node10 in g and node12 in g and node14 in g and node16 in g )
    Expect( V10.type == "node" and V12.type == "node" and V14.type == "node" and V16.type == "node" )
    Expect( V10['x'] == 10 )
    Expect( V12['x'] == 12 )
    Expect( V14['x'] == 14 )
    Expect( V16['x'] == 16 )
    Expect( V10.tmx < V12.tmx )
    Expect( V12.tmx < V14.tmx )
    Expect( V14.tmx < V16.tmx )
    g.CloseVertices( [V10,V12,V14,V16] )

    t10 = t0 + 10
    t12 = t0 + 12
    t14 = t0 + 14
    t16 = t0 + 16
    while g.order > 0:
        order = g.order
        ts = g.ts
        if order == 3:
            Expect( node12 in g and node14 in g and node16 in g,    "order=3, %s should be deleted" % node10 )
            Expect( ts > t10,                                       "t > t10, got t=%f t10=%f" % (ts, t10) )
        elif order == 2:
            Expect( node14 in g and node16 in g,                    "order=2, %s should be deleted" % [node10, node12] )
            Expect( ts > t12,                                       "t > t12, got t=%f t12=%f" % (ts, t12) )
        elif order == 1:
            Expect( node16 in g,                                    "order=1, %s should be deleted" % [node10, node12, node14] )
            Expect( ts > t14,                                       "t > t14, got t=%f t14=%f" % (ts, t14) )
        time.sleep( 0.1 )

    ts = g.ts
    Expect( ts > t16,                                               "t > t16, got t=%f t16=%f" % (ts, t16) )





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
