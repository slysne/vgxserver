###############################################################################
# 
# VGX Server
# Distributed engine for plugin-based graph and vector search
# 
# Module:  pyvgxtest
# File:    DeleteVertex.py
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
from pyvgxtest.threads import Worker
import time
import random

graph = None




###############################################################################
# TEST_DeleteVertex
#
###############################################################################
def TEST_DeleteVertex():
    """
    pyvgx.Graph.DeleteVertex()
    test_level=3101
    """
    graph.Truncate()

    Expect( graph.CreateVertex( "V-delete" ) == 1,      "Should create vertex" )

    # Test deletion
    Expect( graph.DeleteVertex( "V-delete" ) == 1,      "Should delete vertex" )
    Expect( graph.DeleteVertex( "V-delete" ) == 0,      "Should not delete vertex, already deleted" )




###############################################################################
# TEST_DeleteVertex_timeout
#
###############################################################################
def TEST_DeleteVertex_timeout():
    """
    pyvgx.Graph.DeleteVertex() with timeout
    t_nominal=9
    test_level=3101
    """
    random.seed( 1234 )

    graph.Truncate()
    
    A = graph.NewVertex( "A" )
    A['prop'] = "This is A"
    B = graph.NewVertex( "B" )
    B['prop'] = "This is B"
    C = graph.NewVertex( "C" )
    C['prop'] = "This is C"

    graph.CloseVertex( A )
    graph.CloseVertex( B )
    graph.CloseVertex( C )
    del A
    del B
    del C

    N = 10000

    SIZE = 0
    ORDER = 3
    A_ODEG = 0 
    B_ODEG = 0 
    C_ODEG = 0 
    for n in range( N ):
        term = "node_%d" % n
        graph.CreateVertex( term )
        ORDER += 1
        graph.Connect( "A", "to", term )
        graph.Connect( "B", "to", term )
        graph.Connect( "C", "to", term )
        A_ODEG += 1
        B_ODEG += 1
        C_ODEG += 1
        SIZE += 3
        if n % 3:
            graph.Connect( "A", ("num",M_INT,n), term )
            A_ODEG += 1
            SIZE += 1
        else:
            graph.Connect( "B", ("num",M_INT,n), term )
            graph.Connect( "C", ("num",M_INT,n), term )
            B_ODEG += 1
            C_ODEG += 1
            SIZE += 2

        if n % 5:
            graph.Connect( "A", ("num",M_FLT,n), term )
            graph.Connect( "C", ("num",M_FLT,n), term )
            A_ODEG += 1
            C_ODEG += 1
            SIZE += 2
        else:
            graph.Connect( "B", ("num",M_FLT,n), term )
            SIZE += 1
            B_ODEG += 1

        if n % 7:
            graph.Connect( "C", ("num",M_CNT,n), term )
            C_ODEG += 1
            SIZE += 1
        else:
            graph.Connect( "A", ("num",M_CNT,n), term )
            graph.Connect( "B", ("num",M_CNT,n), term )
            A_ODEG += 1
            B_ODEG += 1
            SIZE += 2


    Expect( graph.order == ORDER )
    Expect( graph.size == SIZE )

    DELEX = None
    DELCODE = 0


    def wait_worker_idle( worker, maxwait ):
        time.sleep( 0.5 )
        t0 = time.time()
        while not worker.is_idle():
            time.sleep( 0.1 )
            if time.time() - t0 > maxwait:
                Expect( False, "Worker is taking too long" )


    def deltimeout( name, timeout ):
        global DELEX
        global DELCODE
        DELEX = None
        DELCODE = 0
        try:
            DELCODE = graph.DeleteVertex( name, timeout )
        except pyvgx.AccessError as access_err:
            DELEX = access_err
        except pyvgx.ArcError as arc_err:
            DELEX = arc_err
        except Exception as err:
            DELEX = err


    def trydelete( name, timeout_ms, expect_outarcs_orig, expect_outarcs_remain ):
        global DELEX
        global DELCODE
        V = graph.OpenVertex( name, "r", 1000 )
        Expect( "prop" in V,        "'%s' should have property" % name )
        Expect( V.outdegree == expect_outarcs_orig,   "'%s' should have %d outarcs" % (name, expect_outarcs_orig) )
        graph.CloseVertex( V )
        del V

        # Try to delete vertex. This should time out and raise ArcError
        deleter.perform( deltimeout, name, timeout_ms )

        # Give enough time for deleter to do its job
        wait_worker_idle( deleter, 10.0 )

        # Make sure deleter got the correct exception
        Expect( type(DELEX) == pyvgx.ArcError,  "delete '%s' %s, got %s %s (delcode=%d)" % (name, pyvgx.ArcError, type(DELEX), DELEX, DELCODE) )

        # Check vertex
        Expect( name in graph,      "'%s' should exist" % name )
        V = graph.OpenVertex( name, "r", timeout=1000 )
        Expect( "prop" not in V,    "'%s' property should be removed" % name )
        Expect( V.outdegree == expect_outarcs_remain,   "'%s' should still have %d outarcs" % (name, expect_outarcs_remain) )
        graph.CloseVertex( V )
        del V
        # Return True if vertex was deleted.
        return name not in graph



    REGISTER = {}

    def open( name, REG ):
        REG[name] = graph.OpenVertex( name, "r" )


    def close( name, REG ):
        graph.CloseVertex( REG.pop( name ) )


    def release_one( interval, REG ):
        names = [name for name, thread, mode in graph.GetOpenVertices()]
        t0 = time.time()
        t1 = t0 + interval
        for name in names:
            graph.CloseVertex( REG.pop(name) )
            while time.time() < t1:
                time.sleep( 0.01 )
            t1 += interval

    try:

        reader = Worker( "reader" )
        deleter = Worker( "deleter" )

        # Other thread will now open all terminals readonly
        for n in range( N ):
            reader.perform( open, "node_%d" % n, REGISTER )

        # Let reader perform its work
        wait_worker_idle( reader, 10.0 )

        # Check open vertices
        Expect( len( graph.GetOpenVertices() ) == N )

        # Try and fail to delete A
        Expect( trydelete( "A", 100, A_ODEG, A_ODEG ) == False,  "'A' should not be completely removed" )
        order, size = graph.order, graph.size
        Expect( order == ORDER,       "order should be %d, got %d" % (ORDER, order) )
        Expect( size == SIZE,        "size should be %d, got %d" % (SIZE, size) )

        # Reader now frees most terminals
        R = 50
        for n in random.sample( list(range(N)), N-R ):
            reader.perform( close, "node_%d" % n, REGISTER )

        # Let reader free its terminals
        wait_worker_idle( reader, 10.0 )

        # A few terminals still open
        still_open = [ name for name, tid, mode in graph.GetOpenVertices() ]
        Expect( len( still_open ) == R )

        # Count total outdegree to open nodes
        still_open_set = ",".join( [ "'%s'" % name for name in still_open ] )
        mem = graph.Memory( 4 )
        mem[R1] = 0
        graph.Neighborhood( "B", memory=mem, neighbor={ 'filter':"incif(vertex.id in {%s}, R1)" % still_open_set } )
        B_ODEG_REMAIN = mem[R1]

        # Try to delete B, but fail because some terminals still busy
        Expect( trydelete( "B", 100, B_ODEG, B_ODEG_REMAIN ) == False,  "'B' should not be completely removed" )
        order, size = graph.order, graph.size
        Expect( order == ORDER,       "order should be %d, got %d" % (ORDER, order) )

        # Now slowly release remaining terminal locks
        # Kick off gradual release of remaining terminals
        rel_interval = 0.05
        reader.perform( release_one, rel_interval, REGISTER )
        time.sleep( 0.5 )

        # Issue a long timeout delete for C while remaining terminal locks are slowly released by the other thread
        # Give ample timeout margin
        lenient_timeout = int( R * rel_interval + 5000 )
        try:
            Expect( graph.DeleteVertex( "C", timeout=lenient_timeout ) == 1,   "'C' should have been deleted" )
            Expect( "C" not in graph,           "'C' should not be in graph after deletion" )
            ORDER -= 1 # C gone
        except Exception as ex:
            graph.ShowOpenVertices()
            Expect( False, "'C' should have been deleted, got type(%s) %s" % (type(ex), ex) )

        # Now verify that event processor has also deleted previously failed deletions
        time.sleep( 3.0 ) # Give TTL some time to work
        ORDER -= 2 # A and B gone
        order, size = graph.order, graph.size
        Expect( order == ORDER,         "order should be %d, got %d" % (ORDER, order) )
        Expect( size == 0,              "no arcs should remain, got %d" % size )

        for init in ["A", "B", "C"]:
            Expect( init not in graph,          "'%s' should have been automatically deleted in the background" % init )

        for n in range( N ):
            node = "node_%d" % n
            Expect( node in graph,              "'%s' should exist in graph" % node )
            V = graph.OpenVertex( node, "r" )
            Expect( V.degree == 0,              "'%s' should have no arcs" % node )
            graph.CloseVertex( V )

        
        L = graph.GetOpenVertices()
        Expect( len( L ) == 0,     "should have no open vertices, got %s" % (L) )

    finally:
        # Shut down threads
        reader.terminate()
        deleter.terminate()

        t0 = time.time()
        while not reader.is_dead() or not deleter.is_dead():
            time.sleep( 0.1 )
            if time.time() - t0 > 10:
                Expect( False, "Threads failed to terminate" )
        del REGISTER

        LogInfo( "Background worker shutdown complete" )

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
