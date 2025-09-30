###############################################################################
# 
# VGX Server
# Distributed engine for plugin-based graph and vector search
# 
# Module:  pyvgxtest
# File:    GraphObject.py
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
import time
from pyvgx import *
import pyvgx

graph = None




###############################################################################
# TEST_vxgraph_object
#
###############################################################################
def TEST_vxgraph_object():
    """
    Core vxgraph_object
    test_level=501
    """
    try:
        pyvgx.selftest( force=True, testroot="vgxtest", library="vgx", names=["vxgraph_object.c"] )
    except:
        Expect( False )




###############################################################################
# TEST_vxgraph_tick
#
###############################################################################
def TEST_vxgraph_tick():
    """
    Core vxgraph_tick
    test_level=501
    """
    try:
        pyvgx.selftest( force=True, testroot="vgxtest", library="vgx", names=["vxgraph_tick.c"] )
    except:
        Expect( False )




###############################################################################
# TEST_vxgraph_mapping
#
###############################################################################
def TEST_vxgraph_mapping():
    """
    Core vxgraph_mapping
    test_level=501
    """
    try:
        pyvgx.selftest( force=True, testroot="vgxtest", library="vgx", names=["vxgraph_mapping.c"] )
    except:
        Expect( False )




###############################################################################
# TEST_vxgraph_caching
#
###############################################################################
def TEST_vxgraph_caching():
    """
    Core vxgraph_caching
    test_level=501
    """
    try:
        pyvgx.selftest( force=True, testroot="vgxtest", library="vgx", names=["vxgraph_caching.c"] )
    except:
        Expect( False )




###############################################################################
# TEST_vxgraph_relation
#
###############################################################################
def TEST_vxgraph_relation():
    """
    Core vxgraph_relation
    test_level=501
    """
    try:
        pyvgx.selftest( force=True, testroot="vgxtest", library="vgx", names=["vxgraph_relation.c"] )
    except:
        Expect( False )




###############################################################################
# TEST_vxgraph_state
#
###############################################################################
def TEST_vxgraph_state():
    """
    Core vxgraph_state
    test_level=501
    """
    try:
        pyvgx.selftest( force=True, testroot="vgxtest", library="vgx", names=["vxgraph_state.c"] )
    except:
        Expect( False )




###############################################################################
# TEST_vxgraph_vxtable
#
###############################################################################
def TEST_vxgraph_vxtable():
    """
    Core vxgraph_vxtable
    test_level=501
    """
    try:
        pyvgx.selftest( force=True, testroot="vgxtest", library="vgx", names=["vxgraph_vxtable.c"] )
    except:
        Expect( False )




###############################################################################
# TEST_vxapi_simple
#
###############################################################################
def TEST_vxapi_simple():
    """
    Core vxapi_simple
    t_nominal=645
    test_level=502
    """
    try:
        pyvgx.selftest( force=True, testroot="vgxtest", library="vgx", names=["vxapi_simple.c"] )
    except:
        Expect( False )




###############################################################################
# TEST_vxapi_advanced
#
###############################################################################
def TEST_vxapi_advanced():
    """
    Core vxapi_advanced
    test_level=501
    """
    try:
        pyvgx.selftest( force=True, testroot="vgxtest", library="vgx", names=["vxapi_advanced.c"] )
    except:
        Expect( False )




###############################################################################
# TEST_SizeAndOrder
#
###############################################################################
def TEST_SizeAndOrder():
    """
    pyvgx.Graph.size
    pyvgx.Graph.order
    test_level=3101
    """
    # Test size and order
    graph.Truncate()
    Expect( graph.size == 0,                "empty graph should have no arcs" )
    Expect( graph.order == 0,               "empty graph should have no vertices" )
    # Populate
    N = 1000
    size = 0
    real = 0
    virtual = 0
    for n in range( N ):
        graph.Connect( "a%d" % n, "to", "b%d" % n )
        size += 1
        real += 1
        virtual += 1
        Expect( graph.size == size,             "graph with %d arcs should have size=%d" % (size, size) )
        Expect( graph.order == real+virtual,    "graph with %d vertices should have order=%d" % (real+virtual, real+virtual) )
    # Remove
    for n in range( N ):
        graph.Disconnect( "a%d" % n, "to", "b%d" % n )
        size -= 1
        virtual -= 1
        Expect( graph.size == size,             "graph with %d arcs should have size=%d" % (size, size) )
        Expect( graph.order == real+virtual,    "graph with %d vertices should have order=%d" % (real+virtual, real+virtual) )
    for node in graph.Vertices():
        graph.DeleteVertex( node )
        real -= 1
        Expect( graph.order == real+virtual,    "graph with %d vertices should have order=%d" % (real+virtual, real+virtual) )




###############################################################################
# TEST_CurrentTS
#
###############################################################################
def TEST_CurrentTS():
    """
    pyvgx.Graph.ts
    test_level=3101
    """
    now = time.time()
    delta = abs( graph.ts - now )
    Expect( delta < 2,  "Graph tick should match system time"  )




###############################################################################
# TEST_SimObject
#
###############################################################################
def TEST_SimObject():
    """
    pyvgx.Graph.sim
    test_level=3101
    """
    sim = graph.sim
    Expect( isinstance( sim, pyvgx.Similarity ),            "graph.sim should be a pyvgx.Similarity object" )






###############################################################################
# Run
#
###############################################################################
def Run( name ):
    """
    """
    global graph
    graph = pyvgx.Graph( name )
    RunTests( [__name__] )
    graph.Close()
    del graph
