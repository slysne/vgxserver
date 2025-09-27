###############################################################################
# 
# VGX Server
# Distributed engine for plugin-based graph and vector search
# 
# Module:  pyvgx
# File:    Connect.py
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
import time
from pyvgx import *
import pyvgx

graph = None




A = "A"
B = "B"
C = "C"
D = "D"

SETUP = False



###############################################################################
# TEST_Connect_setup
#
###############################################################################
def TEST_Connect_setup( reset=False ):
    """
    pyvgx.Graph.Connect()
    Setup
    test_level=3101
    """
    global SETUP
    if not SETUP or reset:
        graph.Truncate()
        graph.CreateVertex( A )
        graph.CreateVertex( B )
        graph.CreateVertex( C )
        graph.CreateVertex( D )
        SETUP = True




###############################################################################
# TEST_Connect_simple
#
###############################################################################
def TEST_Connect_simple():
    """
    pyvgx.Graph.Connect()
    Simple arc connection
    test_level=3101
    """
    TEST_Connect_setup()
    Expect( graph.Connect( A, "to", B ) == 1,             "Should insert 1 new relationship" )
    Expect( graph.Connect( A, "to", B ) == 0,             "Connection already exists" )




###############################################################################
# TEST_Connect_multiple
#
###############################################################################
def TEST_Connect_multiple():
    """
    pyvgx.Graph.Connect()
    Multiple arc connection
    test_level=3101
    """
    TEST_Connect_setup()
    # Insert multiple relationships
    Expect( graph.Connect( B, arc=( "likes", M_INT, 123 ),          terminal=C ) == 1,  "Insert arc #1" )
    Expect( graph.Connect( B, arc=( "knows", M_INT, 456 ),          terminal=C ) == 1,  "Insert arc #2" )
    Expect( graph.Connect( B, arc=( "calls", M_INT, 789 ),          terminal=C ) == 1,  "Insert arc #3" )

    # Insert same relationships with different modifiers
    Expect( graph.Connect( B, arc=( "likes", M_UINT, 123 ),         terminal=C ) == 1,  "Insert arc #4" )
    Expect( graph.Connect( B, arc=( "knows", M_FLT,  456.0 ),       terminal=C ) == 1,  "Insert arc #5" )
    Expect( graph.Connect( B, arc=( "calls", M_LSH,  0x1234abcd),   terminal=C ) == 1,  "Insert arc #6" )

    # Re-insert existing arcs
    Expect( graph.Connect( B, arc=( "likes", M_INT, 123 ),          terminal=C ) == 0,  "Arc should already exist" )
    Expect( graph.Connect( B, arc=( "knows", M_INT, 456 ),          terminal=C ) == 0,  "Arc should already exist" )
    Expect( graph.Connect( B, arc=( "calls", M_INT, 789 ),          terminal=C ) == 0,  "Arc should already exist" )
    Expect( graph.Connect( B, arc=( "likes", M_UINT, 123 ),         terminal=C ) == 0,  "Arc should already exist" )
    Expect( graph.Connect( B, arc=( "knows", M_FLT,  456.0 ),       terminal=C ) == 0,  "Arc should already exist" )
    Expect( graph.Connect( B, arc=( "calls", M_LSH, 0x1234abcd ),   terminal=C ) == 0,  "Arc should already exist" )




###############################################################################
# TEST_Connect_conditional
#
###############################################################################
def TEST_Connect_conditional():
    """
    pyvgx.Graph.Connect()
    Conditional connect
    test_level=3101
    """
    TEST_Connect_setup( reset=True )

    # A-int->B (simple after this)
    Expect( graph.Connect( A, arc=( "int", M_INT, 123 ), terminal=B, condition=("int", D_OUT) ) == 0,             "Condition not met, no connect" )
    Expect( graph.Connect( A, arc=( "int", M_INT, 123 ), terminal=B, condition=(False, ("int", D_OUT)) ) == 1,    "Inverse condition, connect A-int->B" )

    # A-int->C (array of arcs after this)
    Expect( graph.Connect( A, arc=( "int", M_INT, 456 ), terminal=C, condition=("int", D_OUT) ) == 0,             "Condition not met, no connect" )
    Expect( graph.Connect( A, arc=( "int", M_INT, 456 ), terminal=C, condition=(False, ("int", D_OUT)) ) == 1,    "Inverse condition, connect A-int->C" )

    # A-flt->B (multiple arc after this)
    Expect( graph.Connect( A, arc=( "flt", M_FLT, 3.21 ), terminal=B, condition=("int", D_OUT, M_INT, V_EQ, 100) ) == 0,        "Condition not met, no connect" )
    Expect( graph.Connect( A, arc=( "flt", M_FLT, 3.21 ), terminal=B, condition=("int", D_OUT, M_INT, V_LT, 100) ) == 0,        "Condition not met, no connect" )
    Expect( graph.Connect( A, arc=( "flt", M_FLT, 3.21 ), terminal=B, condition=("int", D_OUT, M_INT, V_GT, 100) ) == 1,        "Condition met, connect A-flt->B" )

    # A-cnt->B
    Expect( graph.Connect( A, arc=( "cnt", M_CNT, 1 ), terminal=B, condition=("int", D_OUT, M_INT, V_EQ, 100) ) == 0,            "Condition not met, no connect" )
    Expect( graph.Connect( A, arc=( "cnt", M_CNT, 1 ), terminal=B, condition=("int", D_OUT, M_INT, V_LT, 100) ) == 0,            "Condition not met, no connect" )
    Expect( graph.Connect( A, arc=( "cnt", M_CNT, 1 ), terminal=B, condition=("int", D_OUT, M_INT, V_RANGE, (100, 200)) ) == 1,  "Condition met, connect A-cnt->B" )





###############################################################################
# TEST_Connect_terminal_list
#
###############################################################################
def TEST_Connect_terminal_list():
    """
    pyvgx.Graph.Connect()
    Terminals specified as list
    test_level=3101
    """
    TEST_Connect_setup( reset=True )

    # Simple
    Expect( graph.Connect( A, "to", [] ) == 0,          "No terminals" )
    Expect( graph.Connect( A, "to", [B] ) == 1,         "Insert A->B" )
    Expect( graph.Connect( A, "to", [C,D] ) == 2,       "Insert A->C and A->D" )
    Expect( graph.Connect( A, "to", [B,C,D] ) == 0,     "Arcs already exist" )

    # Duplicates not allowed
    for init, terms in [ (A, [B,B]), (A, [B,A]), (A, [B,C,D,B]) ]:
        try:
            graph.Connect( init, "to", terms )
            Expect( False,  "Duplicate vertices not allowed when terminal list is used" )
        except pyvgx.AccessError:
            Expect( True )
        except Exception as ex:
            Expect( False,  "Unexpected exception %s" % ex )

    # No implicit vertex creation when terminal list is used
    try:
        graph.Connect( A, "to", ["term_0"] )
        Expect( False,  "No implicit vertex creation when terminal list is used" )
    except KeyError:
        Expect( True )
    except Exception as ex:
        Expect( False,  "Unexpected exception %s" % ex )
   

    # Create many vertices
    for n in range( 1000 ):
        graph.CreateVertex( "term_%d" % n )

    # Connect to more and more terminals
    for n in range( 1000 ):
        terms = [ "term_%d" % i for i in range(n+1) ]
        Expect( graph.Connect( A, "to", terms ) == 1,           "One new arc each iteration" )

    # Conditional connect
    terms = [ "term_%d" % i for i in range( 1000 ) ]
    Expect( graph.Connect( A, ("int", M_INT, 1234), terms, condition=("special", D_OUT) ) == 0,        "Condition not met, no connect" )
    for i in [1,10,100]:
        Expect( graph.Connect( A, ("special", M_INT, 12345), "term_%d" % i ) == 1,   "Connect special" )
    Expect( graph.Connect( A, ("int", M_INT, 1234), terms, condition=("special", D_OUT, M_INT, V_EQ, 54321) ) == 0,        "Condition not met, no connect" )
    Expect( graph.Connect( A, ("int", M_INT, 1234), terms, condition=("special", D_OUT, M_INT, V_EQ, 12345) ) == 3,        "Condition not met, connect 3" )

    # Detect duplicate in long list
    terms = [ "term_%d" % i for i in range(n+1) ]
    terms[ 700 ] = "term_555"
    try:
        graph.Connect( A, "to", terms )
        Expect( False,  "Duplicate vertices not allowed when terminal list is used" )
    except pyvgx.AccessError:
        Expect( True )
    except Exception as ex:
        Expect( False,  "Unexpected exception %s" % ex )

    # Allow vertex instance instead of vertex ID
    graph.Disconnect( A )
    INIT = graph.OpenVertex( A )
    TERMS = [ B, graph.OpenVertex(C) ]
    Expect( graph.Connect( INIT, "to", TERMS ) == 2,            "Two arcs" )




###############################################################################
# TEST_Connect_virtual
#
###############################################################################
def TEST_Connect_virtual():
    """
    pyvgx.Graph.Connect()
    Connect and create virtual node
    test_level=3101
    """
    TEST_Connect_setup()
    VIRTUAL = "Virtual Node"

    Expect( graph.Connect( B, "to", VIRTUAL ) == 1,         "Connect to and create virtual node" )
    V = graph.OpenVertex( VIRTUAL, mode="r" )
    Expect( V.IsVirtual() == True,                          "Vertex should be virtual" )




###############################################################################
# TEST_Connect_implicit
#
###############################################################################
def TEST_Connect_implicit():
    """
    pyvgx.Graph.Connect()
    Implicit initial and terminal
    test_level=3101
    """
    TEST_Connect_setup()
    INITIAL = "Implicit Initial"
    TERMINAL1 = "Implicit Terminal 1"
    TERMINAL2 = "Implicit Terminal 2"

    Expect( INITIAL not in graph,                           "Initial should not exist" )
    Expect( TERMINAL1 not in graph,                         "Terminal 1 should not exist" )
    Expect( TERMINAL2 not in graph,                         "Terminal 2 should not exist" )

    # INITIAL -> TERMINAL1
    Expect( graph.Connect( INITIAL, "to", TERMINAL1 ) == 1, "Implicitly create both nodes and connect them" )

    V = graph.OpenVertex( INITIAL, mode="r" )
    Expect( V.IsVirtual() == False,                         "Initial should be REAL" )
    graph.CloseVertex( V )

    V = graph.OpenVertex( TERMINAL1, mode="r" )
    Expect( V.IsVirtual() == True,                          "Terminal 1 should be VIRTUAL" )
    graph.CloseVertex( V )

    # INITIAL -> TERMINAL2
    Expect( graph.Connect( INITIAL, "to", TERMINAL2 ) == 1, "Implicitly create terminal connect" )

    V = graph.OpenVertex( INITIAL, mode="r" )
    Expect( V.IsVirtual() == False,                         "Initial should be REAL" )
    graph.CloseVertex( V )

    V = graph.OpenVertex( TERMINAL2, mode="r" )
    Expect( V.IsVirtual() == True,                          "Terminal 2 should be VIRTUAL" )
    graph.CloseVertex( V )

    # Convert TERMINAL1 to REAL
    graph.CloseVertex( graph.OpenVertex( TERMINAL1, mode="a" ) )
    V = graph.OpenVertex( TERMINAL1, mode="r" )
    Expect( V.IsVirtual() == False,                         "Terminal 1 should be converted to REAL" )
    graph.CloseVertex( V )

    # Delete INITIAL
    Expect( graph.DeleteVertex( INITIAL ) == 1,             "Initial should be deleted" )
    Expect( INITIAL not in graph,                           "Initial should not exist" )
    Expect( TERMINAL2 not in graph,                         "Virtual Terminal 2 should be deleted" )
    Expect( TERMINAL1 in graph,                             "Terminal 1 should remain" )
    V = graph.OpenVertex( TERMINAL1, mode="r" )
    Expect( V.IsVirtual() == False,                         "Terminal 1 should be REAL" )
    graph.CloseVertex( V )

    # Delte TERMINAL 1
    Expect( graph.DeleteVertex( TERMINAL1 ) == 1,           "Terminal 1 should be deleted" )
    Expect( TERMINAL1 not in graph,                         "Terminal 1 should not exist" )




###############################################################################
# TEST_Connect_all_modifiers
#
###############################################################################
def TEST_Connect_all_modifiers():
    """
    pyvgx.Graph.Connect()
    Test all modifiers
    test_level=3101
    """
    TEST_Connect_setup()
    N = 100
    M = 12 # modifiers
    for modflag in [0, pyvgx.M_FWDONLY]:
        now = int( time.time() )
        never = now + 3600*24*365
        A = graph.NewVertex( "initial_%d" % now )
        B = graph.NewVertex( "terminal_%d" % now )
        outdegree = 0
        for n in range( N ):
            rel = "rel_%d" % n
            Expect( graph.Connect( A, (rel, pyvgx.M_STAT|modflag            ), B ) == 1,    "new M_STAT arc" )
            outdegree += 1
            Expect( A.outdegree == outdegree,           "vertex should have %d outarcs, found %d" % (outdegree, A.outdegree) )
            Expect( graph.Connect( A, (rel, pyvgx.M_SIM|modflag,  float(n)/N), B ) == 1,    "new M_SIM arc" )
            Expect( graph.Connect( A, (rel, pyvgx.M_DIST|modflag, float(n)  ), B ) == 1,    "new M_DIST arc" )
            Expect( graph.Connect( A, (rel, pyvgx.M_LSH|modflag,  n         ), B ) == 1,    "new M_LSH arc" )
            Expect( graph.Connect( A, (rel, pyvgx.M_INT|modflag,  n         ), B ) == 1,    "new M_INT arc" )
            Expect( graph.Connect( A, (rel, pyvgx.M_UINT|modflag, n         ), B ) == 1,    "new M_UINT arc" )
            Expect( graph.Connect( A, (rel, pyvgx.M_FLT|modflag,  float(n)  ), B ) == 1,    "new M_FLT arc" )
            Expect( graph.Connect( A, (rel, pyvgx.M_CNT|modflag,  n         ), B ) == 1,    "new M_CNT arc" )
            Expect( graph.Connect( A, (rel, pyvgx.M_ACC|modflag,  n         ), B ) == 1,    "new M_ACC arc" )
            Expect( graph.Connect( A, (rel, pyvgx.M_TMC|modflag,  now+n     ), B ) == 1,    "new M_TMC arc" )
            Expect( graph.Connect( A, (rel, pyvgx.M_TMM|modflag,  now+n     ), B ) == 1,    "new M_TMM arc" )
            Expect( graph.Connect( A, (rel, pyvgx.M_TMX|modflag,  never     ), B ) == 1,    "new M_TMX arc" )
            outdegree += (M-1)
            Expect( A.outdegree == outdegree,           "vertex should have %d outarcs, found %d" % (outdegree, A.outdegree) )
        
        Expect( outdegree == N*M, "should have created %d arcs" % (N*M) )

        neighbors = graph.Neighborhood( A, fields=F_ARC, result=R_DICT )
        Expect( len(neighbors) == outdegree,        "vertex should have %d neighbors, found %d" % (outdegree, len(neighbors)) )

        graph.DeleteVertex( A.id )
        graph.DeleteVertex( B.id )




###############################################################################
# TEST_Connect_ForwardOnly
#
###############################################################################
def TEST_Connect_ForwardOnly():
    """
    pyvgx.Graph.Connect()
    Test all modifiers
    test_level=3101
    """
    TEST_Connect_setup()
    now = time.time()
    X = graph.NewVertex( "X_{}".format( now  ))
    Y = graph.NewVertex( "Y_{}".format( now  ))
    B1 = graph.NewVertex( "B1_{}".format( now ))
    B2 = graph.NewVertex( "B2_{}".format( now ))
    B3 = graph.NewVertex( "B3_{}".format( now ))
    B4 = graph.NewVertex( "B4_{}".format( now ))

    # (X) -xfwd-> (B1)
    # X outarcs now simple
    Expect( graph.Connect( X, ("xfwd", M_INT|M_FWDONLY), B1 ) == 1,         "(X) -xfwd-> (B1)" )
    Expect( X.odeg == 1,                                                    "X.odeg==1, got {}".format( X.odeg ) )
    Expect( B1.ideg == 1,                                                   "B1.ideg==1, got {}".format( B1.ideg ) )

    # Can't create regular arc to terminal when terminal already has forward-only arc pointing to it
    try:
        graph.Connect( X, ("regular", M_INT), B1 )
        Expect( False,                                                      "should not allow regular arc when terminal has forward-only inarc(s)" )
    except pyvgx.ArcError as arcex:
        Expect( "Regular arc not allowed" in str(arcex),                    "exception should indicate reason" )
    except Exception as ex:
        Expect( False,                                                      "should be ArcError, got {}".format(ex) )

    # (Y) -xfwd-> (B1)
    Expect( graph.Connect( Y, ("xfwd", M_INT|M_FWDONLY), B1 ) == 1,         "(Y) -xfwd-> (B1)" )
    Expect( Y.odeg == 1,                                                    "Y.odeg==1, got {}".format( Y.odeg ) )
    Expect( B1.ideg == 2,                                                   "B2.ideg==2, got {}".format( B2.ideg ) )

    Expect( graph.Connect( Y, ("xfwd", M_INT|M_FWDONLY), B1 ) == 0,         "already exists" )

    # (X) -xfwd-> (B2)
    # X outarcs now array of arcs
    Expect( graph.Connect( X, ("xfwd", M_INT|M_FWDONLY), B2 ) == 1,         "(X) -xfwd-> (B2)" )
    Expect( X.odeg == 2,                                                    "X.odeg==2, got {}".format( X.odeg ) )
    Expect( B2.ideg == 1,                                                   "B2.ideg==1, got {}".format( B2.ideg ) )

    # (X) -xfwd_2nd-> (B2)
    # X outracs now array of arcs with multiple arc to B2
    Expect( graph.Connect( X, ("xfwd_2nd", M_INT|M_FWDONLY), B2 ) == 1,     "(X) -xfwd_2nd-> (B2)" )
    Expect( X.odeg == 3,                                                    "X.odeg==3, got {}".format( X.odeg ) )
    Expect( B2.ideg == 2,                                                   "B2.ideg==2, got {}".format( B2.ideg ) )

    # (Y) -regular-> (B3)
    Expect( graph.Connect( Y, ("regular", M_INT), B3 ) == 1,                "(Y) -regular-> (B3)" )
    Expect( Y.odeg == 2,                                                    "Y.odeg==2, got {}".format( Y.odeg ) )
    Expect( B3.ideg == 1,                                                   "B2.ideg==1, got {}".format( B3.ideg ) )

    # Can't create forward-only arc to terminal when terminal already has regular arc pointing to it
    try:
        graph.Connect( Y, ("xfwd", M_INT|M_FWDONLY), B3 )
        Expect( False,                                                      "should not allow forward-only arc when terminal has regular inarc(s)" )
    except pyvgx.ArcError as arcex:
        Expect( "Forward-only arc not allowed" in str(arcex),               "exception should indicate reason" )
    except Exception as ex:
        Expect( False,                                                      "should be ArcError, got {}".format(ex) )

    # Disconnecting a forward-only arc from terminal has no effect
    i = B1.ideg
    Expect( graph.Disconnect( B1, arc=D_IN ) == 0,                          "no effect" )
    Expect( B1.ideg == i,                                                   "B1 indegree unaffected" )

    # Remove (X) -xfwd-> (B1)
    # X outarcs still array of arcs with multiple arc to B2
    o = X.odeg
    i = B1.ideg
    Expect( graph.Disconnect( X, arc=D_OUT, neighbor=B1 ) == 1,             "remove (X) -xfwd-> (B1)" )
    Expect( X.odeg == o-1,                                                  "X.odeg should be {}, got {}".format( o-1, X.odeg ) )
    Expect( B1.ideg == i-1,                                                 "B1.ideg should be {}, got {}".format( i-1, B1.ideg ) )

    # Remove (X)-xfwd->(B2) and (X)-xfwd_2nd->(B2)
    # X outarcs now empty
    Expect( graph.Disconnect( X, arc=D_OUT, neighbor=B2 ) == 2,             "remove (X) -*-> (B2)" )
    Expect( X.odeg == 0,                                                    "X.odeg should be 0, got {}".format( X.odeg ) )
    Expect( B2.ideg == 0,                                                   "B2.ideg should be 0, got {}".format( B2.ideg ) )

    # Remove (Y) -xfwd-> (B1)
    # Y outarcs now simple arc
    Expect( graph.Disconnect( Y, arc=D_OUT, neighbor=B1 ) == 1,             "remove (Y) -xfwd-> (B1)" )
    Expect( Y.odeg == 1,                                                    "Y.odeg should be 1, got {}".format( Y.odeg ) )
    Expect( B1.ideg == 0,                                                   "B1.ideg should be 1, got {}".format( B1.ideg ) )

    # Remove (Y) -regular-> (B3)
    # Y outarcs now empty
    Expect( graph.Disconnect( Y, arc=D_OUT, neighbor=B3 ) == 1,             "remove (Y) -regular-> (B3)" )
    Expect( Y.odeg == 0,                                                    "Y.odeg should be 0, got {}".format( Y.odeg ) )
    Expect( B3.ideg == 0,                                                   "B3.ideg should be 0, got {}".format( B3.ideg ) )

    graph.DeleteVertex( X.id )
    graph.DeleteVertex( Y.id )
    graph.DeleteVertex( B1.id )
    graph.DeleteVertex( B2.id )
    graph.DeleteVertex( B3.id )

    graph.CloseAll()




###############################################################################
# TEST_Connect_TMC
#
###############################################################################
def TEST_Connect_TMC():
    """
    pyvgx.Graph.Connect()
    Test arc creation time
    test_level=3101
    """
    TEST_Connect_setup()
    now = int( time.time() )
    A = graph.NewVertex( "initial_%d" % now )
    B = graph.NewVertex( "terminal_%d" % now )

    # Creation time arc, manual assign
    graph.Connect( A, ("manual", M_TMC, int(now)), B )
    tmc = graph.ArcValue( A, ("manual", D_OUT, M_TMC), B )
    Expect( tmc == int(now), "arc TMC should match assigned value" )

    # Creation time arc, auto assign
    gts = int( graph.ts )
    graph.Connect( A, ("auto", M_TMC), B )
    tmc = graph.ArcValue( A, ("auto", D_OUT, M_TMC), B )
    Expect( tmc - gts in [0, 1],  "arc TMC should be automatically set to current graph time" )

    # Re-assign creation time is not allowed
    try:
        try:
            graph.Connect( A, ("manual", M_TMC), B )
            Expect( False, "not allowed to re-assign creation time" )
        except pyvgx.ArcError:
            pass
        try:
            graph.Connect( A, ("auto", M_TMC), B )
            Expect( False, "not allowed to re-assign creation time" )
        except pyvgx.ArcError:
            pass
    except Exception as ex:
        Expect( False, "unexpected exception: %s" % ex )

    graph.DeleteVertex( A.id )
    graph.DeleteVertex( B.id )




###############################################################################
# TEST_Connect_TMM
#
###############################################################################
def TEST_Connect_TMM():
    """
    pyvgx.Graph.Connect()
    Test arc modification time
    test_level=3101
    """
    TEST_Connect_setup()
    now = int( time.time() )
    A = graph.NewVertex( "initial_%d" % now )
    B = graph.NewVertex( "terminal_%d" % now )

    # Modification time arc, manual assign
    graph.Connect( A, ("manual", M_TMM, int(now)), B )
    tmm = graph.ArcValue( A, ("manual", D_OUT, M_TMM), B )
    Expect( tmm == int(now), "arc TMM should match assigned value" )

    # Modification time arc, auto assign
    gts = int( graph.ts )
    graph.Connect( A, ("auto", M_TMM), B )
    tmm = graph.ArcValue( A, ("auto", D_OUT, M_TMM), B )
    Expect( tmm - gts in [0, 1],  "arc TMM should be automatically set to current graph time" )

    # Modification time arc, manual re-assign
    graph.Connect( A, ("manual", M_TMM, 12345), B )
    tmm = graph.ArcValue( A, ("manual", D_OUT, M_TMM), B )
    Expect( tmm == 12345, "arc TMM should match assigned value" )

    # Modification time arc, auto re-assign
    time.sleep(2)
    gts = int( graph.ts )
    graph.Connect( A, ("auto", M_TMM), B )
    tmm = graph.ArcValue( A, ("auto", D_OUT, M_TMM), B )
    Expect( tmm - gts in [0, 1],  "arc TMM should be automatically set to current graph time" )

    graph.DeleteVertex( A.id )
    graph.DeleteVertex( B.id )




###############################################################################
# TEST_Connect_TMX
#
###############################################################################
def TEST_Connect_TMX():
    """
    pyvgx.Graph.Connect()
    Test arc expiration time
    t_nominal=5
    test_level=3101
    """
    TEST_Connect_setup()
    now = int( time.time() )
    A_id = "initial_%d" % now
    B_id = "terminal_%d" % now
    graph.CreateVertex( A_id )
    graph.CreateVertex( B_id )

    # Create a permanent relationship
    graph.Connect( A_id, "permanent", B_id )

    # Create a relationship that will expire
    TTL = 5
    tmx = int(now) + TTL
    graph.Connect( A_id, "expires", B_id )
    graph.Connect( A_id, ("expires", M_TMX, tmx), B_id )
    Expect( graph.ArcValue( A_id, ("expires", D_OUT, M_TMX), B_id ) == tmx,  "Expiration time should be set" )

    # Wait until expiration and verify
    max_latency = 3
    deadline = tmx + max_latency
    while graph.ts < deadline:
        Expect( graph.Adjacent( A_id, "permanent", B_id, timeout=1000 ), "Permanent arc should remain" )
        if not graph.Adjacent( A_id, "expires", B_id, timeout=1000 ):
            break
        time.sleep( 0.1 )

    Expect( not graph.Adjacent( A_id, "expires", B_id, timeout=1000 ), "Expired arc should not exist" )
    Expect( graph.Adjacent( A_id, "permanent", B_id, timeout=1000 ), "Permanent arc should remain" )

    graph.DeleteVertex( A_id, timeout=1000 )
    graph.DeleteVertex( B_id, timeout=1000 )




###############################################################################
# TEST_Connect_Lifespan
#
###############################################################################
def TEST_Connect_Lifespan():
    """
    pyvgx.Graph.Connect()
    Test arc expiration time with lifespan
    t_nominal=5
    test_level=3101
    """
    TEST_Connect_setup()
    now = int( time.time() )
    A_id = "initial_%d" % now
    B_id = "terminal_%d" % now
    graph.CreateVertex( A_id )
    graph.CreateVertex( B_id )

    # Create a relationship that will expire
    TTL = 5
    tmx = int(now) + TTL
    graph.Connect( A_id, "expires", B_id, lifespan=TTL )
    arc_tmx = graph.ArcValue( A_id, ("expires", D_OUT, M_TMX), B_id )
    arc_tmc = graph.ArcValue( A_id, ("expires", D_OUT, M_TMC), B_id )
    Expect( arc_tmx - arc_tmc == TTL,  "Lifespan should be set to %d, got %s" % (TTL, arc_tmx - arc_tmc) )

    # Wait until expiration and verify
    max_latency = 3
    deadline = tmx + max_latency
    while graph.ts < deadline:
        if not graph.Adjacent( A_id, "expires", B_id, timeout=1000 ):
            break
        time.sleep( 0.1 )

    Expect( not graph.Adjacent( A_id, "expires", B_id, timeout=1000 ), "Expired arc should not exist" )

    graph.DeleteVertex( A_id, timeout=1000 )
    graph.DeleteVertex( B_id, timeout=1000 )





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
