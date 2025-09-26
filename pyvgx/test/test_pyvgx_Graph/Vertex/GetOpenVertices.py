###############################################################################
# 
# VGX Server
# Distributed engine for plugin-based graph and vector search
# 
# Module:  pyvgx
# File:    GetOpenVertices.py
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
import threading
from pyvgx import *
import pyvgx

graph = None



def TEST_vxgraph_tracker():
    """
    Core vxgraph_tracker
    test_level=501
    """
    try:
        pyvgx.selftest( force=True, testroot="vgxtest", library="vgx", names=["vxgraph_tracker.c"] )
    except:
        Expect( False )



def TEST_GetOpenVertices_simple():
    """
    pyvgx.Graph.GetOpenVertices()
    Simple test
    test_level=3101
    """
    THREAD_ID = threadid()
   
    node = "node_%s" % __name__
    
    # No open vertices
    V = graph.GetOpenVertices()
    Expect( type( V ) is list and len( V ) == 0,            "no open vertices" )

    # Open first vertex writable
    A = graph.NewVertex( node )
    V = graph.GetOpenVertices()
    Expect( type( V ) is list and len( V ) == 1,            "one open vertex" )
    Expect( type( V[0] ) is tuple and len( V[0] ) == 3,     "3-tuple for open vertex" )
    o_name, o_thread, o_lock = V[0]
    Expect( o_name == node,                                 "first item should be node name" )
    Expect( o_thread == THREAD_ID,                          "second item should be current thread id" )
    Expect( o_lock == "WL",                                 "third item should be 'WL'" )
    graph.CloseVertex( A )
    Expect( len(graph.GetOpenVertices()) == 0,              "no open vertices" )
    
    # Open second vertex readonly
    A = graph.OpenVertex( node, "r" )
    V = graph.GetOpenVertices()
    Expect( type( V ) is list and len( V ) == 1,            "one open vertex" )
    Expect( type( V[0] ) is tuple and len( V[0] ) == 3,     "3-tuple for open vertex" )
    o_name, o_thread, o_lock = V[0]
    Expect( o_name == node,                                 "first item should be node name" )
    Expect( o_thread == THREAD_ID,                          "second item should be current thread id" )
    Expect( o_lock == "RO",                                 "third item should be 'RO'" )
    graph.CloseVertex( A )
   
    

def TEST_GetOpenVertices_many():
    """
    pyvgx.Graph.GetOpenVertices()
    Many vertices
    test_level=3101
    """
    THREAD_ID = threadid()

    N = 100

    WL = []
    RO = []
    for n in range( N ):
        wl_node = "wl_%d_%s" % (n, __name__)
        ro_node = "ro_%d_%s" % (n, __name__)
        graph.CreateVertex( wl_node )
        graph.CreateVertex( ro_node )
        WL.append( graph.OpenVertex( wl_node, "w" ) )
        RO.append( graph.OpenVertex( ro_node, "r" ) )


    V = graph.GetOpenVertices()
    Expect( len(V) == 2*N,                                  "should have %d open vertices" % 2*N )
    n_WL = 0
    n_RO = 0
    for node, tid, lock in V:
        Expect( tid == THREAD_ID,                           "all vertices should be held by current thread" )
        if lock == "WL":
            Expect( node.startswith( "wl_" ),               "expected WL lock" )
            n_WL += 1
        elif lock == "RO":
            Expect( node.startswith( "ro_" ),               "expected RO lock" )
            n_RO += 1
        else:
            Expect( False,                                  "lock mode should be WL or RO" )

    Expect( n_WL == N and n_RO == N,                        "should have %d each of WL and RO vertices" % N )


    for V in WL:
        graph.CloseVertex( V )
    
    for V in RO:
        graph.CloseVertex( V )
    


def TEST_GetOpenVertices_threadid():
    """
    pyvgx.Graph.GetOpenVertices()
    With thread filter
    test_level=3101
    """
    THREAD_ID = threadid()

    node = "node_%s" % __name__
    A = graph.OpenVertex( node )
    Expect( len( graph.GetOpenVertices( THREAD_ID ) ) == 1,        "current thread should have one open vertex" )
    Expect( len( graph.GetOpenVertices( THREAD_ID+1 ) ) == 0,      "other thread should have no open vertices" )
    graph.CloseVertex( A )




def Run( name ):
    global graph
    graph = pyvgx.Graph( name )
    RunTests( [__name__] )
    graph.Close()
    del graph
