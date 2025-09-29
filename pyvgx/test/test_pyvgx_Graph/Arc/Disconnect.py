###############################################################################
# 
# VGX Server
# Distributed engine for plugin-based graph and vector search
# 
# Module:  pyvgx
# File:    Disconnect.py
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
from pyvgxtest.threads import Worker
import time
from pyvgx import *
import pyvgx
import random

graph = None






###############################################################################
# TEST_Disconnect_basic
#
###############################################################################
def TEST_Disconnect_basic():
    """
    Basic pyvgx.Graph.Disconnect()
    test_level=3101
    """
    graph.Truncate()

    initial = "A_basic"
    terminal = "B_basic"
    other = "C_basic"

    A = graph.NewVertex( initial )
    B = graph.NewVertex( terminal )
    C = graph.NewVertex( other )

    Expect( A.degree == 0 )
    Expect( B.degree == 0 )
    Expect( C.degree == 0 )

    # Nothing to disconnect
    for node in [initial, terminal]:
        Expect( graph.Disconnect( node ) == 0 )
        for d in [D_ANY, D_IN, D_OUT]:
            Expect( graph.Disconnect( node, ("to", d) ) == 0 )
            Expect( graph.Disconnect( node, ( "to", d, M_INT ) ) == 0 )
            Expect( graph.Disconnect( node, ( "to" ), terminal ) == 0 )
            Expect( graph.Disconnect( node, ( "to", d ), terminal ) == 0 )
            Expect( graph.Disconnect( node, ( "to", d, M_INT ), terminal ) == 0 )
            Expect( graph.Disconnect( node, ( "to" ), other ) == 0 )
            Expect( graph.Disconnect( node, ( "to", d ), other ) == 0 )
            Expect( graph.Disconnect( node, ( "to", d, M_INT ), other ) == 0 )


    # Simple arc
    graph.Connect( initial, ("to", M_INT, 123), terminal )
    Expect( graph.Disconnect( initial, ("to", D_IN) ) == 0 )
    Expect( graph.Disconnect( initial, ("to", D_IN, M_INT) ) == 0 )
    Expect( graph.Disconnect( initial, ("to", D_IN, M_INT), terminal ) == 0 )
    Expect( graph.Disconnect( initial, ("to", D_IN, M_INT), other ) == 0 )

    Expect( graph.Disconnect( initial, ("nope") ) == 0 )
    Expect( graph.Disconnect( initial, ("nope", D_OUT) ) == 0 )
    Expect( graph.Disconnect( initial, ("nope", D_OUT, M_INT) ) == 0 )
    Expect( graph.Disconnect( initial, ("nope"), other ) == 0 )
    Expect( graph.Disconnect( initial, ("nope", D_OUT), other ) == 0 )
    Expect( graph.Disconnect( initial, ("nope", D_OUT, M_INT), other ) == 0 )
    Expect( graph.Disconnect( initial, ("nope"), terminal ) == 0 )
    Expect( graph.Disconnect( initial, ("nope", D_OUT), terminal ) == 0 )
    Expect( graph.Disconnect( initial, ("nope", D_OUT, M_INT), terminal ) == 0 )

    Expect( graph.Disconnect( initial, ("to", D_OUT, M_FLT), other ) == 0 )
    Expect( graph.Disconnect( initial, ("to", D_OUT, M_FLT), terminal ) == 0 )
    Expect( graph.Disconnect( initial, ("to", D_OUT, M_INT), other ) == 0 )
    Expect( graph.Disconnect( initial, ("to", D_OUT, M_INT), terminal ) == 1 )

    Expect( A.degree == 0 )
    Expect( B.degree == 0 )
    Expect( C.degree == 0 )

    # Multiple arc
    graph.Connect( initial, ("to", M_INT, 123), terminal )
    graph.Connect( initial, ("also", M_FLT, 4.56), terminal )

    Expect( graph.Disconnect( initial, ("to", D_IN) ) == 0 )
    Expect( graph.Disconnect( initial, ("to", D_IN, M_INT) ) == 0 )
    Expect( graph.Disconnect( initial, ("to", D_IN, M_INT), terminal ) == 0 )
    Expect( graph.Disconnect( initial, ("to", D_IN, M_INT), other ) == 0 )

    Expect( graph.Disconnect( initial, ("nope") ) == 0 )
    Expect( graph.Disconnect( initial, ("nope", D_OUT) ) == 0 )
    Expect( graph.Disconnect( initial, ("nope", D_OUT, M_INT) ) == 0 )
    Expect( graph.Disconnect( initial, ("nope"), other ) == 0 )
    Expect( graph.Disconnect( initial, ("nope", D_OUT), other ) == 0 )
    Expect( graph.Disconnect( initial, ("nope", D_OUT, M_INT), other ) == 0 )
    Expect( graph.Disconnect( initial, ("nope"), terminal ) == 0 )
    Expect( graph.Disconnect( initial, ("nope", D_OUT), terminal ) == 0 )
    Expect( graph.Disconnect( initial, ("nope", D_OUT, M_INT), terminal ) == 0 )

    Expect( graph.Disconnect( initial, ("to", D_OUT, M_FLT), other ) == 0 )
    Expect( graph.Disconnect( initial, ("to", D_OUT, M_FLT), terminal ) == 0 )
    Expect( graph.Disconnect( initial, ("to", D_OUT, M_INT), other ) == 0 )
    Expect( graph.Disconnect( initial, ("to", D_OUT, M_CNT), terminal ) == 0 ) # no
    Expect( graph.Disconnect( initial, ("to", D_OUT, M_INT), terminal ) == 1 ) # yes

    Expect( graph.Disconnect( initial, ("also", D_OUT, M_FLT), other ) == 0 )
    Expect( graph.Disconnect( initial, ("also", D_OUT, M_INT), terminal ) == 0 ) # no
    Expect( graph.Disconnect( initial, ("also", D_OUT, M_FLT), terminal ) == 1 ) # yes

    Expect( A.degree == 0 )
    Expect( B.degree == 0 )
    Expect( C.degree == 0 )


    # Multiple arc more
    graph.Connect( initial, ("to", M_INT, 123), terminal )
    graph.Connect( initial, ("to", M_FLT, 4.56), terminal )
    graph.Connect( initial, ("also", M_INT, 789), terminal )
    graph.Connect( initial, ("also", M_FLT, 10.0), terminal )

    Expect( graph.Disconnect( initial, ("to", D_IN) ) == 0 )
    Expect( graph.Disconnect( initial, ("to", D_IN, M_INT) ) == 0 )
    Expect( graph.Disconnect( initial, ("to", D_IN, M_INT), terminal ) == 0 )
    Expect( graph.Disconnect( initial, ("to", D_IN, M_INT), other ) == 0 )

    Expect( graph.Disconnect( initial, ("nope") ) == 0 )
    Expect( graph.Disconnect( initial, ("nope", D_OUT) ) == 0 )
    Expect( graph.Disconnect( initial, ("nope", D_OUT, M_INT) ) == 0 )
    Expect( graph.Disconnect( initial, ("nope"), other ) == 0 )
    Expect( graph.Disconnect( initial, ("nope", D_OUT), other ) == 0 )
    Expect( graph.Disconnect( initial, ("nope", D_OUT, M_INT), other ) == 0 )
    Expect( graph.Disconnect( initial, ("nope"), terminal ) == 0 )
    Expect( graph.Disconnect( initial, ("nope", D_OUT), terminal ) == 0 )
    Expect( graph.Disconnect( initial, ("nope", D_OUT, M_INT), terminal ) == 0 )

    Expect( graph.Disconnect( initial, ("to", D_OUT, M_FLT), other ) == 0 )
    Expect( graph.Disconnect( initial, ("to", D_OUT, M_CNT), terminal ) == 0 ) # no
    Expect( graph.Disconnect( initial, ("to", D_OUT, M_FLT), terminal ) == 1 )
    Expect( graph.Disconnect( initial, ("to", D_OUT, M_FLT), terminal ) == 0 )

    Expect( graph.Disconnect( initial, ("also", D_OUT), other ) == 0 )
    Expect( graph.Disconnect( initial, ("also", D_OUT), terminal ) == 2 )
    Expect( graph.Disconnect( initial, ("also", D_OUT), terminal ) == 0 )

    Expect( graph.Disconnect( initial, ("to", D_OUT, M_INT), terminal ) == 1 )

    Expect( A.degree == 0 )
    Expect( B.degree == 0 )
    Expect( C.degree == 0 )




###############################################################################
# TEST_Disconnect_multiple
#
###############################################################################
def TEST_Disconnect_multiple():
    """
    Multiple pyvgx.Graph.Disconnect()
    test_level=3101
    """
    graph.Truncate()

    initial = "A_basic"
    terminal = "B_basic"
    other = "C_basic"

    A = graph.NewVertex( initial )
    B = graph.NewVertex( terminal )
    C = graph.NewVertex( other )

    # Multiple arc even more
    for extra_fanout in [ [], ["C_basic"], ["C_basic", "D_basic"], ["C_basic", "D_basic", "E_basic"] ]:
        for n_rels in [1, 2, 3, 4, 5, 10, 20, 50, 100, 200, 500, 1000]:
            for mods in [ [M_INT], [M_INT,M_CNT], [M_INT,M_CNT,M_FLT] ]:
                degree_to_terminal = n_rels * len(mods)
                degree = degree_to_terminal * (1 + len(extra_fanout))
                for mod in mods:
                    for r in range( n_rels ):
                        rel = "rel_%d" % r
                        graph.Connect( initial, (rel, mod, 1), terminal )
                        for extra in extra_fanout:
                            graph.Connect( initial, (rel, mod, 1), extra )
                # Terminal Specific:
                # No match
                Expect( graph.Disconnect( initial, ("no",D_OUT,M_INT), terminal ) == 0 )
                # Match
                Expect( graph.Disconnect( initial, ("rel_0",D_OUT,M_INT), terminal ) == 1 )
                degree -= 1
                degree_to_terminal -= 1
                Expect( A.degree == degree )
                # Terminal Filtered:
                # No match
                Expect( graph.Disconnect( initial, ("no",D_OUT), terminal ) == 0 )
                # Match
                remain_rel_0_to_terminal = len(mods) - 1
                Expect( graph.Disconnect( initial, ("rel_0",D_OUT), terminal ) == remain_rel_0_to_terminal )
                degree -= remain_rel_0_to_terminal
                degree_to_terminal -= remain_rel_0_to_terminal
                Expect( A.degree == degree )
                # Terminal Wildcard:
                remain_rel_to_terminal = graph.Disconnect( initial, ("*",D_OUT), terminal )
                degree -= remain_rel_to_terminal
                degree_to_terminal -= remain_rel_to_terminal
                Expect( degree_to_terminal == 0 )
                Expect( A.degree == degree )
                Expect( graph.Disconnect( initial, ("*",D_OUT), terminal ) == 0 )
                Expect( graph.Disconnect( initial, ("*",D_OUT) ) == degree )
                Expect( A.degree == 0 )




###############################################################################
# TEST_Disconnect_many
#
###############################################################################
def TEST_Disconnect_many():
    """
    Advanced pyvgx.Graph.Disconnect()
    test_level=3101
    """
    N = 100
    R = 300
    M = [M_INT, M_FLT, M_CNT, M_UINT]

    now = int( time.time() )
    A = graph.NewVertex( "initial_%d" % now )
    graph.CreateVertex( "not_a_terminal" ) 
    
    graph.Disconnect( A )
    Expect( A.degree == 0,          "vertex should have no arcs" )

    # Populate
    for n in range( N ):
        terminal = "terminal_%d_%d" % (now,n)
        for r in range( R ):
            rel = "rel_%d" % r
            for mod in M:
                Expect( graph.Connect( A, (rel, mod, n), terminal ) == 1,       "new unique arc created" )
    # Verify
    narcs = N * R * len(M)
    Expect( A.degree == narcs,              "vertex should have %d arcs" % narcs )
    

    # Disconnect and check
    # 

    # Delete all rel_0 arcs
    Expect( graph.Disconnect( A, ("rel_0") ) == N * len(M),     "should delete all rel_0 arcs" )
    narcs -= N * len(M)
    R -= 1
    Expect( A.degree == narcs,                                  "vertex should have %d arcs" % narcs )
    
    # No rel_0 arcs remain
    Expect( graph.Disconnect( A, ("rel_0") ) == 0,                          "should not delete any arcs" )

    # Should not delete arcs that don't exist
    Expect( graph.Disconnect( A, ("rel_1", D_OUT, M_TMC) ) == 0,            "should not delete any arcs" )
    Expect( graph.Disconnect( A, ("rel_1", D_IN, M_INT, ) ) == 0,           "should not delete any arcs" )
    Expect( graph.Disconnect( A, ("rel_1", D_OUT, M_INT, V_GT, N ) ) == 0,  "should not delete any arcs" )
    Expect( graph.Disconnect( A, ("rel_1", D_OUT, M_INT, V_EQ, N ) ) == 0,  "should not delete any arcs" )
    for n in range( N ):
        terminal = "terminal_%d_%d" % (now, n)
        Expect( graph.Disconnect( A, ("rel_1", D_OUT, M_INT, V_EQ, N ), terminal ) == 0,  "should not delete any arcs" )
        Expect( graph.Disconnect( A, ("rel_1", D_OUT, M_TMC ), terminal ) == 0,           "should not delete any arcs" )
        Expect( graph.Disconnect( A, ("nope", D_OUT, M_INT ), terminal ) == 0,            "should not delete any arcs" )
        Expect( graph.Disconnect( A, ("nope", D_OUT, M_INT, V_EQ, n ), terminal ) == 0,   "should not delete any arcs" )
    
    # Delete all M_INT arcs
    for n in range( N ):
        Expect( graph.Disconnect( A, ("rel_1", D_OUT, M_INT, V_EQ, n ) ) == 1,  "should delete one arc" )
        narcs -= 1
        Expect( A.degree == narcs,  "vertex should have %d arcs" % narcs )
        
    # Delete all M_FLT arcs where terminal is also specified
    for n in range( N ):
        terminal = "terminal_%d_%d" % (now, n)
        Expect( graph.Disconnect( A, ("rel_1", D_OUT, M_FLT, V_EQ, n ), terminal ) == 1,  "should delete one arc" )
        narcs -= 1
        Expect( A.degree == narcs,  "vertex should have %d arcs" % narcs )

    # Don't delete when no arc to this terminal
    for n in range( N ):
        terminal = "not_a_terminal"
        Expect( graph.Disconnect( A, ("rel_1", D_OUT, M_UINT, V_EQ, n ), terminal ) == 0,  "should not delete any arcs" )

    # Delete all M_UINT arcs
    for n in range( N ):
        Expect( graph.Disconnect( A, ("rel_1", D_OUT, M_UINT, V_EQ, n ) ) == 1,  "should delete one arc" )
        narcs -= 1
        Expect( A.degree == narcs,  "vertex should have %d arcs" % narcs )


    terminal_now_0 = "terminal_%d_0" % now
    # No more M_UINT arcs to terminal_now_0
    Expect( graph.Disconnect( A, ("rel_1", D_OUT, M_UINT), terminal_now_0 ) == 0,   "should not delete any arcs" )

    # Delete all M_CNT arcs to terminal_now_0
    c = graph.Disconnect( A, ("*", D_OUT, M_CNT), terminal_now_0 )
    Expect( c == R,         "should delete %d arcs, deleted %d" % (R, c)  )
    narcs -= R
    
    # Delete all M_CNT arcs to remaining arcs
    c = R * (N-1)
    Expect( graph.Disconnect( A, ("*", D_OUT, M_CNT) ) == c,                        "should delete %d arcs" % c )
    narcs -= c
    del M[ M.index( M_CNT ) ]

    # No more rel_1 arcs 
    Expect( graph.Disconnect( A, ("rel_1") ) == 0,                                  "should not delete any arcs" )
    R -= 1

    # Don't delete when no arcs to this terminal
    Expect( graph.Disconnect( A, (None, D_OUT), "not_a_terminal" ) == 0,        "should not delete any arcs" )

    # Delete 
    for n in range( N ):
        terminal = "terminal_%d_%d" % (now, n)
        nrels = R * len(M)
        Expect( graph.Disconnect( A, (None, D_OUT), terminal ) == nrels,        "should delete %d arcs" % nrels )
        narcs -= nrels
           
    # Empty
    Expect( narcs == 0 and A.degree == 0,                                   "all arcs have been removed" )

    # No more arcs can be removed
    Expect( graph.Disconnect( A ) == 0,                                     "no arcs should be deleted" )




###############################################################################
# TEST_Disconnect_locked
#
###############################################################################
def TEST_Disconnect_locked():
    """
    pyvgx.Graph.Disconnect() with access conflicts
    t_nominal=18
    test_level=3101
    """
    graph.Truncate()

    # Other thread
    V = {}
    def open_vertex( g, name, mode ):
        V[name] = g.OpenVertex( name, mode=mode )


    def close_vertex( g, name ):
        try:
            x = V.pop( name )
            g.CloseVertex( x )
        except KeyError:
            pass


    other = Worker( "other" )

    # Make a simple chain: A -> B -> C
    graph.CreateVertex( "A" )
    graph.CreateVertex( "B" )
    graph.CreateVertex( "C" )

    graph.Connect( "A", "to", "B" )
    graph.Connect( "B", "to", "C" )

    Expect( graph.Order() == 3,                 "Should have 3 vertices" )
    Expect( graph.Size() == 2,                  "Should have 2 arcs" )

    other.perform_sync( 10.0, open_vertex, graph, "A", "r" )
    other.perform_sync( 10.0, open_vertex, graph, "C", "r" )
    
    # A and C are now owned by other thread
    Expect( "A" in V and V["A"].Readonly(),     "'A' should be readonly" )
    Expect( "C" in V and V["C"].Readonly(),     "'C' should be readonly" )

    # Try to disconnect B in this thread
    try:
        graph.Disconnect( "B", D_OUT )
        Expect( False,                          "Disconnect should be incomplete" )
    except ArcError as ae:
        Expect( "incomplete" in str(ae),        "Should be incomplete, got '%s'" % ae )
    except Exception as ex:
        Expect( False,                          "Unexpected exception: %s" % ex )

    try:
        graph.Disconnect( "B", D_IN )
        Expect( False,                          "Disconnect should be incomplete" )
    except ArcError as ae:
        Expect( "incomplete" in str(ae),        "Should be incomplete, got '%s'" % ae )
    except Exception as ex:
        Expect( False,                          "Unexpected exception: %s" % ex )

    # Close C and disconnect B -> C
    other.perform_sync( 10.0, close_vertex, graph, "C" )
    Expect( "C" not in V )

    n = graph.Disconnect( "B", D_OUT )
    Expect( n == 1,                             "Should disconnect 1 arc, got %d" % n )

    # Shold still fail to disconnect A -> B
    try:
        graph.Disconnect( "B", D_IN )
        Expect( False,                          "Disconnect should be incomplete" )
    except ArcError as ae:
        Expect( "incomplete" in str(ae),        "Should be incomplete, got '%s'" % ae )
    except Exception as ex:
        Expect( False,                          "Unexpected exception: %s" % ex )


    # Close A and disconnect A -> B
    other.perform_sync( 10.0, close_vertex, graph, "A" )
    Expect( "A" not in V )

    n = graph.Disconnect( "B", D_IN )
    Expect( n == 1,                             "Should disconnect 1 arc, got %d" % n )


    Expect( graph.Order() == 3,                 "Should have 3 vertices" )
    Expect( graph.Size() == 0,                  "Should have 0 arcs" )


    # More complex.
    #
    # init_1 ----\           /---- term_1
    # init_2 -----\         /----- term_2
    #               -> B ->
    #               -> X ->
    # ...    -----/         \----- ...
    # init_n ----/           \---- term_n
    #
    #
    N = 20
    initials = [ "init_%d" % n for n in range(1,N+1) ] 
    terminals = [ "term_%d" % n for n in range(1,N+1) ] 
    for init, term in zip( initials, terminals ):
        graph.CreateVertex( init, type="initial" )
        graph.CreateVertex( term, type="initial" )


    # Try disconnects with different timeouts
    for timeout in [0, 100]:
        # Create a vertex which will be set to auto expire
        X = graph.NewVertex( "X" )
        X['intact'] = True
        graph.CloseVertex( X )
        del X

        # Connect initials to X and X to terminals
        for init, term in zip( initials, terminals ):
            graph.Connect( init, "to", "X" )
            graph.Connect( "X", "to", term )

        # Connect initials to B and B to terminals
        for init, term in zip( initials, terminals ):
            graph.Connect( init, "to", "B" )
            graph.Connect( "B", "to", term )

        # Verify
        size = 2 * (len( initials ) + len( terminals ))
        Expect( graph.Size() == size,               "Should have %d arcs, got %d" % (size, graph.Size()) )
        
        # Other thread will now own all initials and terminals
        for init in initials:
            other.perform_sync( 10.0, open_vertex, graph, init, "r" )

        for term in terminals:
            other.perform_sync( 10.0, open_vertex, graph, term, "r" )

        # Try to disconnect B in this thread
        try:
            graph.Disconnect( "B", timeout=timeout )
            Expect( False,                          "Disconnect should be incomplete" )
        except ArcError as ae:
            Expect( "incomplete" in str(ae),        "Should be incomplete, got '%s'" % ae )
        except Exception as ex:
            Expect( False,                          "Unexpected exception: %s" % ex )

        # No arcs should be removed
        Expect( graph.Size() == size,               "Should have %d arcs, got %d" % (size, graph.Size()) )

        # Let's shuffle the name lists and gradually release all but one
        random.seed( 1234 )
        initials_shuffle_less_one = random.sample( initials, N - 1  )
        terminals_shuffle_less_one = random.sample( terminals, N - 1 )

        # Set X to expire in the background, then wait until it has expired to ensure TTL activity has begun
        X = graph.OpenVertex( "X" )
        Expect( X['intact'] == True,                "X should still be intact" )
        X.SetExpiration( 1, True )
        graph.CloseVertex( X )
        # Wait for TTL to strip the node of all properties (leaving only arcs)
        Xr = graph.OpenVertex( "X", mode="r", timeout=1000 )
        t0 = timestamp()
        deadline = t0 + 10.0
        while Xr.HasProperty( 'intact' ):
            graph.CloseVertex( Xr )
            Expect( timestamp() < deadline,         "X should expire but remains intact after %d seconds" % (deadline - t0) )
            time.sleep( 0.1 )
            Xr = graph.OpenVertex( "X", mode="r", timeout=1000 )
        graph.CloseVertex( Xr )
        del Xr

        # Keep B writable for disconnects below
        B = graph.OpenVertex( "B" )

        for init, term in zip( initials_shuffle_less_one, terminals_shuffle_less_one ):
            # Close one initial and one terminal
            other.perform_sync( 10.0, close_vertex, graph, init )
            other.perform_sync( 10.0, close_vertex, graph, term )

            # Degrees of B before disconnect
            ideg_pre = B.ideg
            odeg_pre = B.odeg

            # Disconnect one inarc for B should succeed
            try:
                graph.Disconnect( "B", D_IN, timeout=timeout )
                Expect( False,                          "Disconnect should be incomplete" )
            except ArcError as ae:
                Expect( "incomplete" in str(ae),        "Should be incomplete, got '%s'" % ae )
            except Exception as ex:
                Expect( False,                          "Unexpected exception: %s" % ex )

            ideg_post = B.ideg
            n_removed = ideg_pre - ideg_post
            Expect( n_removed == 1,                     "One inarc should be removed, %d actually removed" % n_removed )
            Expect( B.odeg == odeg_pre,                 "No outarcs should be removed" )

            # Disconnect one outarc for B should succeed
            try:
                graph.Disconnect( "B", D_OUT, timeout=timeout )
                Expect( False,                          "Disconnect should be incomplete" )
            except ArcError as ae:
                Expect( "incomplete" in str(ae),        "Should be incomplete, got '%s'" % ae )
            except Exception as ex:
                Expect( False,                          "Unexpected exception: %s" % ex )

            odeg_post = B.odeg
            n_removed = odeg_pre - odeg_post
            Expect( n_removed == 1,                     "One outarc should be removed, %d actually removed" % n_removed )
            Expect( B.ideg == ideg_post,                "No inarcs should be removed" )

            # X should still exist as REAL in the graph even though it has expired
            Expect( "X" in graph,                       "X should exist" )
            Xr = graph.OpenVertex( "X", mode="r", timeout=1000 )
            Expect( Xr.virtual == False,                "X should be REAL" )
            graph.CloseVertex( Xr )
            del Xr
            Expect( "X" in graph,                       "X should exist" )


        # Now make sure other thread releases all nodes
        for init, term in zip( initials, terminals ):
            other.perform_sync( 10.0, close_vertex, graph, init )
            other.perform_sync( 10.0, close_vertex, graph, term )

        # Remove last arcs from B
        n_in = graph.Disconnect( "B", D_IN, timeout=timeout )
        n_out = graph.Disconnect( "B", D_OUT, timeout=timeout )

        Expect( n_in == 1,                              "Should disconnect 1 remaining inarc, got %d" % n_in )
        Expect( n_out == 1,                             "Should disconnect 1 remaining outarc, got %d" % n_out )

        Expect( B.ideg == 0,                            "B.ideg should be 0, got %d" % B.ideg )
        Expect( B.odeg == 0,                            "B.odeg should be 0, got %d" % B.odeg )

        graph.CloseVertex( B )
        del B

        # Check that all X outarcs eventually disappear
        Expect( "X" in graph,                           "X should exist" )
        
        try:
            Xr = graph.OpenVertex( "X", mode="r", timeout=1000 )
        except:
            print(graph.Vertices( timeout=10000 ))
            print(graph.Arcs( timeout=10000 ))
            raise

        t0 = timestamp()
        deadline = t0 + 10.0
        while Xr.odeg > 0:
            graph.CloseVertex( Xr )
            Expect( timestamp() < deadline,             "X still has outarcs after %d seconds" % (deadline - t0) )
            time.sleep( 0.1 )
            Expect( "X" in graph,                       "X should exist" )
            Xr = graph.OpenVertex( "X", mode="r", timeout=1000 )

        Expect( Xr.virtual == True,                     "X is VIRTUAL once all outarcs are removed" )
        Expect( Xr.ideg == N,                           "All X inarcs should still remain, got %d" % Xr.ideg )
        graph.CloseVertex( Xr )

        # Finally disconnect X completely and it should disappear from graph
        graph.Disconnect( "X" )
        Expect( "X" not in graph,                       "VIRTUAL X should disappear once disconnected" )

        Expect( graph.Size() == 0,                      "No arcs in graph, got %d" % graph.Size() )


    other.terminate_sync( 10.0 )




###############################################################################
# TEST_Disconnect_in_locked
#
###############################################################################
def TEST_Disconnect_in_locked():
    """
    pyvgx.Graph.Disconnect() inarcs with access conflicts
    t_nominal=318
    test_level=3101
    """
    graph.Truncate()

    # Other thread
    V = {}
    def open_vertex( g, name, mode ):
        V[name] = g.OpenVertex( name, mode=mode )
        return name


    def close_vertex( g, name ):
        try:
            x = V.pop( name )
            g.CloseVertex( x )
            return name
        except KeyError:
            return None


    def disconnect_inarcs( g, name, timeout ):
        try:
            n = g.Disconnect( name, D_IN, timeout=timeout )
            print("Disconnect( %s, D_IN, timeout=%d ) -> %d" % ( name, timeout, n ))
            return n
        except Exception as ex:
            print("Disconnect( %s, D_IN, timeout=%d ) -> Exception: %s" % ( name, timeout, ex ))
            return -1


    def disconnect_outarcs( g, name, timeout ):
        try:
            n = g.Disconnect( name, D_OUT, timeout=timeout )
            print("Disconnect( %s, D_OUT, timeout=%d ) -> %d" % ( name, timeout, n ))
            return n
        except Exception as ex:
            print("Disconnect( %s, D_OUT, timeout=%d ) -> Exception: %s" % ( name, timeout, ex ))
            return -1


    def disconnect_outarc( g, init, rel, term, timeout ):
        try:
            n = g.Disconnect( init, rel, term, timeout=timeout )
            print("Disconnect( %s, %s, %s, timeout=%d ) -> %d" % ( init, rel, term, timeout, n ))
            return n
        except Exception as ex:
            print("Disconnect( %s, %s, %s, timeout=%d ) -> Exception: %s" % ( init, rel, term, timeout, ex ))
            return -1



    initial_owner = Worker( "initial_owner" )
    terminal_owner = Worker( "terminal_owner" )

    MAX_INITIALS = 5
    MAX_TERMINALS = 5
    MAX_ARC_WIDTH = 3
    for initial_count in range( 1, MAX_INITIALS+1 ):
        for terminal_count in range( 1, MAX_TERMINALS+1 ):
            for arc_width in range( 1, MAX_ARC_WIDTH+1 ):

                graph.Truncate()

                Expect( graph.Size() == 0 and graph.Order() == 0,                   "Empty graph" )

                # i1=====\    /======t1
                #         ===> 
                # i2=====/    \======t2
                # ...   /      \  ...
                # iN===/        \====tM
                #

                expect_size = 0
                expect_order = 0

                INITIALS = [ "i%d" % i for i in range( 1, initial_count+1 ) ]
                TERMINALS = [ "t%d" % t for t in range( 1, terminal_count+1 ) ]
                ARCS = [ "a%d" % a for a in range( 1, arc_width+1 ) ]

                for init in INITIALS:
                    graph.CreateVertex( init )
                    expect_order += 1

                for term in TERMINALS:
                    graph.CreateVertex( term )
                    expect_order += 1

                for init in INITIALS:
                    for term in TERMINALS:
                        for arc in ARCS:
                            graph.Connect( init, arc, term )
                            expect_size += 1

                Expect( graph.Size() == expect_size,                                "Should have %d arcs, got %d" % (expect_size, graph.Size()) )
                Expect( graph.Order() == expect_order,                              "Should have %d vertices, got %d" % (expect_order, graph.Order()) )


                # Open initials in one thread
                for init in INITIALS:
                    initial_owner.perform_sync( 10.0, open_vertex, graph, init, "a" )
                    Expect( initial_owner.collect() == init,                        "Should open %s" % init )

                # Open terminals in another thread
                for term in TERMINALS:
                    terminal_owner.perform_sync( 10.0, open_vertex, graph, term, "a" )
                    Expect( terminal_owner.collect() == term,                       "Should open %s" % term )




                # Disconnect all terminal inarcs should fail
                for term in TERMINALS:
                    terminal_owner.perform_sync( 10.0, disconnect_inarcs, graph, term, 10 )
                    Expect( terminal_owner.collect() == -1,                         "Thread %d should not disconnect %s's inarcs due to thread %d's locks on all initials" % (terminal_owner.ident, term, initial_owner.ident) )


                # Disconnect all initials to specific terminal should fail
                for init in INITIALS:
                    for term in TERMINALS:
                        initial_owner.perform_sync( 10.0, disconnect_outarc, graph, init, "*", term, 10 )
                        Expect( initial_owner.collect() == -1,                      "Thread %d should not disconnect %s-[*]->%s due to thread %d's lock on %s" % (initial_owner.ident, init, term, terminal_owner.ident, term) )
                        for arc in ARCS:
                            initial_owner.perform_sync( 10.0, disconnect_outarc, graph, init, arc, term, 10 )
                            Expect( initial_owner.collect() == -1,                  "Thread %d should not disconnect %s-[%s]->%s due to thread %d's lock on %s" % (initial_owner.ident, init, arc, term, terminal_owner.ident, term) )



                # Set up long timeout inarc disconnect for terminals running while we try the operations below
                for term in TERMINALS:

                    terminal_owner.perform( disconnect_inarcs, graph, term, 30000 )  # Now running in the background

                    time.sleep( 0.1 )

                    # Try (and fail) to disconnect all initials to current terminal
                    for init in INITIALS:
                        initial_owner.perform_sync( 10.0, disconnect_outarc, graph, init, "*", term, 10 )
                        Expect( initial_owner.collect() == -1,                      "Thread %d should not disconnect %s-[*]->%s due to thread %d's lock on %s" % (initial_owner.ident, init, term, terminal_owner.ident, term) )

                    # Try (and fail) to disconnect all initials to any terminal
                    for init in INITIALS:
                        initial_owner.perform_sync( 10.0, disconnect_outarcs, graph, init, 10 )
                        Expect( initial_owner.collect() == -1,                      "Thread %d should not disconnect %s-[*]->* due to thread %d's lock on all terminals" % (initial_owner.ident, init, terminal_owner.ident) )


                    # Release all initials. The running terminal's inarc disconnect should make progress
                    REMAINING_INIT = list( INITIALS )
                    for init in INITIALS:
                        initial_owner.perform_sync( 10.0, close_vertex, graph, init )
                        Expect( initial_owner.collect() == init,                    "%s should be closed" % init )
                        REMAINING_INIT.pop( 0 )
                        # Terminal's inarc disconnect should still be pending becuase not all initials are released yet
                        for remaining_init in REMAINING_INIT:
                            initial_owner.perform_sync( 10.0, disconnect_outarc, graph, remaining_init, "*", term, 10 )
                            Expect( initial_owner.collect() == -1,                  "Thread %d should not disconnect %s-[*]->%s due to thread %d's lock on %s" % (initial_owner.ident, remaining_init, term, terminal_owner.ident, term) )

                    # All initials now released. Running terminal inarc disconnect should make progress and complete

                    # Give terminal thread a chance to finish disconnecting running terminal's inarcs
                    t0 = timestamp()
                    deadline = t0 + 4.0
                    while not terminal_owner.is_idle():
                        if timestamp() < deadline:
                            time.sleep( 0.1 )
                        else:
                            Expect( False, "Terminal owner thread should be idle" )

                    expect_inarcs_removed = initial_count * arc_width
                    expect_size -= expect_inarcs_removed

                    # Verify
                    n_arcs = terminal_owner.collect()
                    Expect( n_arcs == expect_inarcs_removed,                        "Thread %d should have disconnected %d initial(s), each with %d arc(s), to %s, got %d" % (terminal_owner.ident, initial_count, arc_width, term, n_arcs) )
                    Expect( graph.Size() == expect_size,                            "Graph should have %d arcs, got %d" % (expect_size, graph.Size()) )
                    Expect( graph.Order() == expect_order,                          "Graph should have %d vertices, got %d" % (expect_order, graph.Order()) )

                    # Re-open all initials
                    for init in INITIALS:
                        initial_owner.perform_sync( 10.0, open_vertex, graph, init, "a" )
                        Expect( initial_owner.collect() == init,                    "Should open %s" % init )


                # Close all terminals
                for term in TERMINALS:
                    terminal_owner.perform_sync( 10.0, close_vertex, graph, term )
                    Expect( terminal_owner.collect() == term,                       "%s should be closed" % term )

                # Close all initials
                for init in INITIALS:
                    initial_owner.perform_sync( 10.0, close_vertex, graph, init )
                    Expect( initial_owner.collect() == init,                        "%s should be closed" % init )

                # All vertices closed
                Expect( len(V) == 0,                                                "No open vertices, got %s" % V )

                # No remaining arcs
                Expect( graph.Size() == 0,                                          "Should have no remaining arcs, got %d" % graph.Size() )


    # Terminate
    terminal_owner.terminate_sync( 10.0 )
    initial_owner.terminate_sync( 10.0 )
    



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
    graph.Truncate()
    graph.Close()
    del graph
