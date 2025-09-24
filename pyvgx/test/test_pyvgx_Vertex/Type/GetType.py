from pytest.pytest import RunTests, Expect, TestFailed
from pyvgx import *
import pyvgx

graph = None



def TEST_GetType():
    """
    pyvgx.Vertex.GetType()
    test_level=3101
    """
    g = graph
    g.Truncate()

    # Create typeless vertex
    A = g.NewVertex( "vertex" )

    # Create typed vertex
    B = g.NewVertex( "node_0", type="node" )

    tp = A.GetType()
    Expect( tp == "__vertex__",                 "vertex should be typeless, got %s" % tp )

    tp = B.GetType()
    Expect( tp == "node",                       "vertex type should be \"node\", got %s" % tp )

    del A
    del B




def Run( name ):
    global graph
    graph = pyvgx.Graph( name )
    RunTests( [__name__] )
    graph.Close()
    del graph

