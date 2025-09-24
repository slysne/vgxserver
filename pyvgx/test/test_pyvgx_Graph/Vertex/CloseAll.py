from pytest.pytest import RunTests, Expect, TestFailed
from . import _vertex_test_support as Support
import random
from pyvgx import *
import pyvgx

graph = None



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


def Run( name ):
    """
    Run the tests in this module
    """
    global graph
    graph = pyvgx.Graph( name )
    RunTests( [__name__] )
    graph.Close()
    del graph
