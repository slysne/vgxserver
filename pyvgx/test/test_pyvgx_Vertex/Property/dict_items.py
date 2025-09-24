from pytest.pytest import RunTests, Expect, TestFailed
from pyvgx import *
import pyvgx

graph = None



def TEST_items():
    """
    pyvgx.Vertex.items()
    Basic
    test_level=3101
    """
    if "vertex" in graph:
        graph.DeleteVertex( "vertex" )

    V = graph.NewVertex( "vertex" )
    Expect( len(list(V.items())) == 0,            "no items" )

    V['x'] = 1
    Expect( list(V.items()) == [('x',1)],         "integer" )

    V['x'] = "string"
    Expect( list(V.items()) == [('x','string')],  "string" )








def Run( name ):
    global graph
    graph = pyvgx.Graph( name )
    RunTests( [__name__] )
    graph.Close()
    del graph


