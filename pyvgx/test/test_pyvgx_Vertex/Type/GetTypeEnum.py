from pytest.pytest import RunTests, Expect, TestFailed
from pyvgx import *
import pyvgx

graph = None



def TEST_GetTypeEnum():
    """
    pyvgx.Vertex.GetTypeEnum()
    test_level=3101
    """
    g = graph
    g.Truncate()

    # Create typeless vertex
    A = g.NewVertex( "vertex" )

    # Create typed vertex
    B = g.NewVertex( "node_0", type="node" )

    tA = A.GetTypeEnum()
    tB = B.GetTypeEnum()

    Expect( type(tA) is int,            "type enum should be int, got %s" % type(tA) )

    Expect( tA != tB,                   "type enumerations should be different, got %d" % tA )
    
    A.SetType( "node" )
    t = A.GetTypeEnum()
    Expect( t == tB,                    "type enumerations should be the same, got %s" % [t, tB] )

    B.SetType( None )
    t = B.GetTypeEnum()
    Expect( t == tA,                    "type enumeration should be %d, got %d" % (tA, t) )

    del A
    del B



def Run( name ):
    global graph
    graph = pyvgx.Graph( name )
    RunTests( [__name__] )
    graph.Close()
    del graph

