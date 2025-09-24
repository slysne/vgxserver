from pytest.pytest import RunTests, Expect, TestFailed
from pyvgx import *
import pyvgx

graph = None



def TEST_keys():
    """
    pyvgx.Vertex.keys()
    Basic
    test_level=3101
    """
    if "vertex" in graph:
        graph.DeleteVertex( "vertex" )

    V = graph.NewVertex( "vertex" )
    Expect( len(list(V.keys())) == 0,                     "no keys" )

    V['x'] = 1
    Expect( list(V.keys()) == ['x'],                      "one key" )

    V['y'] = "string"
    Expect( set(V.keys()) == set(['x','y']),        "two keys" )





def Run( name ):
    global graph
    graph = pyvgx.Graph( name )
    RunTests( [__name__] )
    graph.Close()
    del graph


