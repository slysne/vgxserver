from pytest.pytest import RunTests, Expect, TestFailed
from pyvgx import *
import pyvgx

graph = None



def TEST_SetType():
    """
    pyvgx.Vertex.SetType()
    test_level=3101
    """
    g = graph
    g.Truncate()

    # Create typeless vertex
    g.CreateVertex( "vertex" )

    # Create typed vertex
    g.CreateVertex( "node_0", type="node" )

    # Types should be enumerated
    T = g.VertexTypes()
    Expect( len(T) == 2,                "graph should have two vertex types, got %s" % T)
    Expect( "__vertex__" in T,          "graph should contain the typeless vertex enum" )
    Expect( "node" in T,                "graph should contain the \"node\" vertex enum" )

    # Change from typeless to typed
    V = g.OpenVertex( "vertex" )
    Expect( V.type == "__vertex__",     "vertex should be typeless, got %s" % V.type )
    V.SetType( "node" )
    Expect( V.type == "node",           "vertex type should be \"node\", got %s" % V.type )
    T = g.VertexTypes()
    Expect( len(T) == 1,                "graph should have one vertex type, got %s" % T)
    Expect( "node" in T,                "graph should contain the \"node\" vertex enum" )
    g.CloseVertex( V )

    # Change from typed to typeless
    V = g.OpenVertex( "node_0" )
    Expect( V.type == "node",           "vertex type should be \"node\", got %s" % V.type )
    V.SetType( None )
    Expect( V.type == "__vertex__",     "vertex should be typeless, got %s" % V.type )
    T = g.VertexTypes()
    Expect( len(T) == 2,                "graph should have two vertex types, got %s" % T)
    Expect( "__vertex__" in T,          "graph should contain the typeless vertex enum" )
    Expect( "node" in T,                "graph should contain the \"node\" vertex enum" )
    g.CloseVertex( V )



def Run( name ):
    global graph
    graph = pyvgx.Graph( name )
    RunTests( [__name__] )
    graph.Close()
    del graph

