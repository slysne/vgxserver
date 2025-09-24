from pytest.pytest import RunTests, Expect, TestFailed
from pyvgx import *
import pyvgx

graph = None



def TEST_values():
    """
    pyvgx.Vertex.values()
    Basic
    test_level=3101
    """
    if "vertex" in graph:
        graph.DeleteVertex( "vertex" )

    V = graph.NewVertex( "vertex" )
    Expect( len(list(V.values())) == 0,                     "no values" )

    V['x'] = 1
    Expect( list(V.values()) == [1],                        "one value" )

    V['y'] = "string"
    Expect( set(V.values()) == set([1,'string']),     "two values" )





def Run( name ):
    global graph
    graph = pyvgx.Graph( name )
    RunTests( [__name__] )
    graph.Close()
    del graph


