from pytest.pytest import RunTests, Expect, TestFailed
from pyvgx import *
import pyvgx

graph = None



def TEST_GetProperties():
    """
    pyvgx.Vertex.GetProperties()
    Basic
    test_level=3101
    """
    if "vertex" in graph:
        graph.DeleteVertex( "vertex" )

    V = graph.NewVertex( "vertex" )
    Expect( len(V.GetProperties()) == 0,                "no properties" )

    V['x'] = 1
    Expect( V.GetProperties() == {'x':1},               "one property" )

    V['y'] = "string"
    Expect( V.GetProperties() == {'x':1,'y':'string'},  "two properties" )


def Run( name ):
    global graph
    graph = pyvgx.Graph( name )
    RunTests( [__name__] )
    graph.Close()
    del graph


