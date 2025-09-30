###############################################################################
# 
# VGX Server
# Distributed engine for plugin-based graph and vector search
# 
# Module:  pyvgx.test
# File:    OpenVertices.py
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
from pyvgxtest.threads import Worker
from pyvgx import *
import pyvgx
import random

graph = None




###############################################################################
# expect_Exc
#
###############################################################################
def expect_Exc( E, method, *args, **kwds ):
    """
    """
    try:
        method( *args, **kwds )
        Expect( False,              "Expected exception {}".format( E ) )
    except E:
        pass


OWNED = {}



###############################################################################
# get_ids
#
###############################################################################
def get_ids( graph, idlist, mode ):
    """
    """
    tid = threadid()
    L = graph.OpenVertices( idlist=idlist, mode=mode )
    OWNED[tid] = L



###############################################################################
# put_ids
#
###############################################################################
def put_ids( graph ):
    """
    """
    tid = threadid()
    if tid in OWNED:
        OWNED.pop( tid )




###############################################################################
# TEST_OpenVertices_basic
#
###############################################################################
def TEST_OpenVertices_basic():
    """
    pyvgx.Graph.OpenVertices()
    Basic tests
    test_level=3101
    """
    # Reset
    graph.Truncate()

    # Populate graph
    graph.CreateVertex( "A" )
    graph.CreateVertex( "B" )
    graph.CreateVertex( "C" )
    graph.CreateVertex( "D" )
    graph.CreateVertex( "E" )
    W = graph.NewVertex( "W" ) # writable

    for mode in [None, 'a', 'r']:
        if mode == 'r':
            writable = False
            readonly = True
        else:
            writable = True
            readonly = False
        for ids in [ [], ["A"], ["A","B"], ["A","B","C"], ["D","C","B","A"], ["B","D","E","C","A"], ["A","C","E","W","D","B"] ]:
            if mode:
                LIST = graph.OpenVertices( idlist=ids, mode=mode )
            else:
                LIST = graph.OpenVertices( idlist=ids )
            Expect( len(LIST) == len(ids),              "{} vertices".format( len(ids) ) )
            for i in range( len(ids) ):
                id = ids[i]
                Expect( LIST[i].id == id,                   "Vertex ID match"  )
                if id != "W":
                    Expect( LIST[i].Writable() == writable, "Vertex '{}' writable={}".format( id, writable ) )
                    Expect( LIST[i].Readonly() == readonly, "Vertex '{}' readonly={}".format( id, readonly ) )
                else:
                    Expect( LIST[i].Writable() == True,     "Vertex '{}' writable={}".format( id, writable ) )
                    Expect( LIST[i].Readonly() == False,    "Vertex '{}' readonly={}".format( id, readonly ) )
            graph.CloseVertices( LIST )
            del LIST
    W.Close()
    graph.CloseAll()




###############################################################################
# TEST_OpenVertices_locked
#
###############################################################################
def TEST_OpenVertices_locked():
    """
    pyvgx.Graph.OpenVertices()
    Access locked by other thread
    test_level=3101
    """
    # Reset
    graph.Truncate()

    # Populate graph
    graph.CreateVertex( "A" )
    graph.CreateVertex( "B" )
    graph.CreateVertex( "C" )
    graph.CreateVertex( "D" )
    graph.CreateVertex( "E" )

    # Other thread
    W = Worker( "w1" )

    for LIST in [ ["C"], ["C","D"], ["B","C"], ["B","C","D"], ["A","B","C","D"], ["A","B","C","D","E"] ]:
        # Other thread acquires C writable
        W.perform_sync( 1, get_ids, graph, ["C"], mode='a' )

        # Current thread can't access C in either mode
        for mode in ['a', 'r']:
            expect_Exc( pyvgx.AccessError, graph.OpenVertices, LIST, mode=mode )
            expect_Exc( pyvgx.AccessError, graph.OpenVertices, LIST, mode=mode, timeout=100 )

        # Other thread releases C
        W.perform_sync( 1, put_ids, graph )

        # Other thread acquires C readonly
        W.perform_sync( 1, get_ids, graph, ["C"], mode='r' )

        # Current thread can't access C writable
        expect_Exc( pyvgx.AccessError, graph.OpenVertices, LIST, mode='a' )
        expect_Exc( pyvgx.AccessError, graph.OpenVertices, LIST, mode='a', timeout=100 )

        # Current thread CAN access C readonly
        V = graph.OpenVertices( LIST, mode='r' )

        S = set( (x.id for x in V) ) 
        Expect( S == set(LIST),         "Vertex set opened" )
        
        del V
        W.perform_sync( 1, put_ids, graph )

    W.terminate()

    graph.CloseAll()






###############################################################################
# TEST_OpenVertices_invalid_args
#
###############################################################################
def TEST_OpenVertices_invalid_args():
    """
    pyvgx.Graph.OpenVertices()
    Invalid arguments
    test_level=3102
    """
    # Reset
    graph.Truncate()

    # Populate graph
    graph.CreateVertex( "A" )
    graph.CreateVertex( "B" )
    graph.CreateVertex( "C" )
    graph.CreateVertex( "D" )

    # Arguments required
    expect_Exc( TypeError, graph.OpenVertices )

    # Non-existent vertex
    for mode in ['a', 'r']:
        expect_Exc( KeyError, graph.OpenVertices, idlist=["X"], mode=mode )
        expect_Exc( KeyError, graph.OpenVertices, idlist=["A","X"], mode=mode )
        expect_Exc( KeyError, graph.OpenVertices, idlist=["X","A"], mode=mode )

    # Mode must be 'a' or 'r'
    expect_Exc( ValueError, graph.OpenVertices, idlist=["A"], mode="w" )

    # Timeout must be integer
    expect_Exc( TypeError, graph.OpenVertices, idlist=["A"], timeout="x" )

    # Duplicates not allowed in either mode
    for mode in ['a', 'r']:
        expect_Exc( pyvgx.AccessError, graph.OpenVertices, idlist=["A","A"], mode=mode )
        expect_Exc( pyvgx.AccessError, graph.OpenVertices, idlist=["A","B","C","B"], mode=mode )

    graph.CloseAll()




###############################################################################
# TEST_OpenVertices_large_with_duplicates
#
###############################################################################
def TEST_OpenVertices_large_with_duplicates():
    """
    pyvgx.Graph.OpenVertices()
    Large list with and without duplicates
    test_level=3102
    """
    # Reset
    graph.Truncate()

    # Large lists
    R = range( 10000 )
    for n in R:
        graph.CreateVertex( "node_%d" % n )

    # Many list sizes with and without duplicates
    for mode in ['a', 'r']:
        for N in list(range( 1, 800 )) + list(range(1020,1030)) + list(range(2040,2050)) + [3000,3500,4000,4500,5000,5500,6000,7000,8000,9000,9998,9999]:
            # Verify without duplicates
            LIST = [ "node_%d" % i for i in random.sample( R, N ) ]
            OPEN = graph.OpenVertices( LIST )
            Expect( len(OPEN) == N,                             "Opened %d vertices" % N )
            Expect( len(graph.GetOpenVertices()) == N,          "%d vertices are open" % N )
            # Recursive
            OPEN2 = graph.OpenVertices( OPEN )
            Expect( len(OPEN2) == N,                            "Opened %d vertices recursively" % N )
            Expect( len(graph.GetOpenVertices()) == N,          "%d vertices are open" % N )
            # Close and verify
            graph.CloseVertices( OPEN )
            Expect( len(graph.GetOpenVertices()) == N,          "%d vertices are open" % N )
            graph.CloseVertices( OPEN2 )
            Expect( len(graph.GetOpenVertices()) == 0,          "All vertices closed" )
            # Generate random duplicate
            x = random.sample( LIST, 1 )[0]
            LIST.insert( random.randint(0,len(LIST)), x )
            expect_Exc( pyvgx.AccessError, graph.OpenVertices, idlist=LIST, mode=mode )


    graph.CloseAll()




###############################################################################
# TEST_OpenVertices_invalid_access
#
###############################################################################
def TEST_OpenVertices_invalid_access():
    """
    pyvgx.Graph.OpenVertices()
    Invalid access
    test_level=3102
    """
    # Reset
    graph.Truncate()

    # Populate graph
    graph.CreateVertex( "A" )
    graph.CreateVertex( "B" )
    graph.CreateVertex( "C" )
    graph.CreateVertex( "D" )

    # Open one readonly
    C = graph.OpenVertex( "C", mode="r" )

    # Can't acquire writable when already readonly
    expect_Exc( pyvgx.AccessError, graph.OpenVertices, idlist=["C"], mode="a" )
    expect_Exc( pyvgx.AccessError, graph.OpenVertices, idlist=["B", "C"], mode="a" )
    expect_Exc( pyvgx.AccessError, graph.OpenVertices, idlist=["B", "C", "D"], mode="a" )
    expect_Exc( pyvgx.AccessError, graph.OpenVertices, idlist=["C", "D"], mode="a" )

    # Ok to open another readonly
    R = graph.OpenVertices( ["A", "B", "C", "D"], mode="r" )

    del C
    del R

    graph.CloseAll()





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
