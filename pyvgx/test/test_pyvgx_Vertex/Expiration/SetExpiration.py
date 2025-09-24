from pytest.pytest import RunTests, Expect, TestFailed
from pyvgx import *
import pyvgx
import time
import random

graph = None

TMX_TOLERANCE = 5


def TEST_SetExpiration_simple():
    """
    pyvgx.Vertex.SetExpiration()
    Set expiration
    t_nominal=9
    test_level=3101
    """
    TTL = 7

    A_id = "expiring_abs_node"
    R_id = "expiring_rel_node"
    L_id = "expiring_lifespan_node"
    P_id = "permanent_node"

    A = graph.NewVertex( A_id )
    R = graph.NewVertex( R_id )
    L = graph.NewVertex( L_id, lifespan=TTL  ) # Lifespan=TTL
    P = graph.NewVertex( P_id )

    tmx = int( graph.ts ) + TTL
    A.SetExpiration( tmx )          # Absolute tmx
    R.SetExpiration( TTL, True )    # Relative to now

    graph.CloseVertex( A )
    graph.CloseVertex( R )
    graph.CloseVertex( L )
    graph.CloseVertex( P )


    # Verify expiration is not premature
    while graph.ts < tmx - 1:
        Expect( A_id in graph, "%s should exist" % A_id )
        Expect( R_id in graph, "%s should exist" % R_id )
        Expect( L_id in graph, "%s should exist" % L_id )
        Expect( P_id in graph, "%s should exist" % P_id )
        time.sleep( 0.1 )

    # Wait until expiration and verify
    deadline = tmx + TMX_TOLERANCE
    while graph.ts < deadline:
        if not A_id in graph and not R_id in graph:
            break
        time.sleep( 0.1 )
    Expect( not A_id in graph, "%s should not exist" % A_id )
    Expect( not R_id in graph, "%s should not exist" % R_id )
    Expect( not L_id in graph, "%s should not exist" % L_id )
    Expect( P_id in graph,     "%s should still exist" % P_id )

    graph.DeleteVertex( P_id )



def TEST_SetExpiration_cancel():
    """
    pyvgx.Vertex.SetExpiration()
    Set and cancel expiration
    t_nominal=9
    test_level=3101
    """
    V_id = "expire_then_cancel_node"

    V = graph.NewVertex( V_id )

    now = graph.ts
    TTL = 5
    tmx = int(now) + TTL
    V.SetExpiration( tmx )

    graph.CloseVertex( V )

    # Sleep until 1 second before expiration
    while graph.ts < tmx - 1:
        time.sleep( 0.01 )

    # Cancel expiration
    V = graph.OpenVertex( V_id )
    V.ClearExpiration()
    graph.CloseVertex( V )
    
    # Sleep until well after original expiration time
    while graph.ts < tmx + TMX_TOLERANCE:
        time.sleep( 0.01 )

    # Verify node still exists
    Expect( V_id in graph, "%s should still exist" % V_id )
    V = graph.OpenVertex( V_id )
    Expect( not V.IsExpired(), "%s should not be expired" % V_id )
    Expect( V.GetExpiration() == T_NEVER, "%s should not have expiration set" % V_id  )
    graph.CloseVertex( V )

    graph.DeleteVertex( V_id )



def TEST_SetExpiration_with_arc_ttl():
    """
    pyvgx.Vertex.SetExpiration()
    Vertex expiration and arc expiration
    t_nominal=61
    test_level=3102
    """
    random.seed( 1000 )

    terminal = "virtual_terminal_for_vertex_with_expiration"
    node_ttls = {}
    for n in range(10000):
        name = "node_%d" % n
        node_ttl = 5 * random.randint(1,11)
        node_ttls[ name ] = node_ttl

    arc_ttls = [ (0,20), (1,50), (2,40), (3,10), (4,30), (-1,T_NEVER) ] 
    nodes = {}

    # Create vertices and set initial expirations
    t0 = int( graph.ts )
    for node, node_ttl in list(node_ttls.items()):
        V_id = "initial_%s_with_ttl_%d" % (node, node_ttl)
        V = graph.NewVertex( V_id )
        nodes[ V_id ] = (node_ttl, V)
        tmx = t0 + node_ttl + random.randint( -10, 10 )
        V.SetExpiration( tmx ) # slightly incorrect, we will correct after setting arc ttls
        # We hold writelock, TTL can't acquire nodes

    # Set arc expirations
    for n, arc_ttl in arc_ttls:
        if arc_ttl < T_NEVER:
            arc_tmx = t0 + arc_ttl
            rel = "expires_%d_%d" % (n, arc_ttl)
            for node in list(nodes.keys()):
                graph.Connect( node, (rel, M_TMX, arc_tmx), terminal )

    # Correct vertex expirations
    for node, node_ttl_and_V in list(nodes.items()):
        node_ttl, V = node_ttl_and_V
        nodes[ node ] = node_ttl
        tmx = t0 + node_ttl
        # Set correct tmx and close vertex
        V.SetExpiration( tmx )
        graph.CloseVertex( V )


    # Check expiring arcs in the order of expiration
    for n, arc_ttl in sorted( arc_ttls, key=lambda x:x[1] ):
        remain = set( nodes )
        arc_tmx = t0 + arc_ttl
        rel = "expires_%d_%d" % (n, arc_ttl)
        while remain:
            print(graph.EventBacklog())
            for node, node_ttl in list(nodes.items()):
                node_tmx = t0 + node_ttl
                graph_ts = graph.ts
                ts = int( graph_ts ) - t0
                # Vertex should still exist
                if graph_ts < node_tmx - 1:
                    Expect( node in graph, "vertex '%s' should still exist at ts=%d" % (node, ts) )
                    if node in remain and arc_ttl < T_NEVER:
                        V = graph.OpenVertex( node, "r", -1 )
                        # Arc should still exist
                        if graph_ts < arc_tmx - 1:
                            Expect( graph.Adjacent( V, (rel, D_OUT, M_TMX), terminal, timeout=1000 ), "arc '%s' should still exist at ts=%d" % (rel, ts) )
                        # Arc should have expired
                        #elif graph.ts > arc_tmx + 1:
                        elif graph_ts > arc_tmx + TMX_TOLERANCE:
                            Expect( not graph.Adjacent( V, (rel, D_OUT, M_TMX), terminal, timeout=1000 ), "arc '%s' should have expired at ts=%d" % (rel, ts) )
                            remain.remove( node ) # Verified arc expired
                        graph.CloseVertex( V )
                # Vertex should have expired
                elif graph_ts > node_tmx + TMX_TOLERANCE and node in remain:
                    Expect( node not in graph, "vertex '%s' should have expired at ts=%d" % (node, ts) )
                    remain.remove( node ) # Verified vertex expired
                    del nodes[ node ]
            # Wait a bit
            time.sleep(0.5)

    # No arcs should remain
    Expect( terminal not in graph, "%s should be removed" % terminal )

        

def TEST_SetExpiration_many():
    """
    pyvgx.Vertex.SetExpiration()
    Set expiration for many nodes
    t_nominal=430
    test_level=3103
    """
    graph.Truncate()
    Expect( graph.order == 0 and graph.size == 0 )

    graph.CreateVertex( "no_tmx" )

    t0 = int( graph.ts )
    for n in range( 50000000 ):
        V = graph.NewVertex( str(n) )
        v_ttl = 60 + n % 240 # up to 5 minutes
        v_tmx = t0 + v_ttl
        V.SetExpiration( v_tmx )
        term = str( n % 100000 )
        T = graph.NewVertex( term, lifespan=1, timeout=1000 )
        graph.Connect( V, "to", T )
        a_ttl = 60 + (n*1013) % 240 # up to 5 minutes
        a_tmx = t0 + a_ttl
        graph.Connect( V, ("tmx",M_TMX,a_tmx), T )
        T.Close()
        V.Close()
        if not n % 1000000:
            print(graph.EventBacklog(), n, graph.order, graph.size)

    print("---------------------------------------------")

    t1 = graph.ts

    ttl_max = 300

    while graph.order > 1:
        print(graph.EventBacklog(), graph.order, graph.size, end=' ')
        time.sleep( 3 )
        t2 = graph.ts - t1

        if t2 > ttl_max + TMX_TOLERANCE:
            late_by = t2 - ttl_max
            print("LATE BY %d seconds" % late_by)
        else:
            print()

    time.sleep( 2 )
    print(graph.EventBacklog(), graph.order, graph.size)

    Expect( graph.order == 1 )
    graph.DeleteVertex( "no_tmx" )
    Expect( graph.order == 0 )






def Run( name ):
    global graph
    graph = pyvgx.Graph( name )
    RunTests( [__name__] )
    graph.Close()
    del graph


