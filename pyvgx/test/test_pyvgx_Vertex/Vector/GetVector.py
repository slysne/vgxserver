from pytest.pytest import RunTests, Expect, TestFailed
from pyvgx import *
import pyvgx

graph = None



def TEST_GetVector():
    """
    pyvgx.Vertex.GetVector()
    Basic
    test_level=3101
    """
    if "vertex" in graph:
        graph.DeleteVertex( "vertex" )

    V = graph.NewVertex( "vertex" )

    Expect( len(V.GetVector()) == 0,                            "null vector" )

    V.SetVector( [('a', 1)] )
    Expect( len(V.GetVector()) == 1,                            "one element" )
    Expect( V.GetVector().external == [('a',1.0)],              "correct element" )

    V.SetVector( [('a', 1), ('b',0.5)] )
    Expect( len(V.GetVector()) == 2,                            "two elements" )
    Expect( V.GetVector().external == [('a',1.0), ('b',0.5)],   "correct elements" )





def Run( name ):
    global graph
    graph = pyvgx.Graph( name )
    RunTests( [__name__] )
    graph.Close()
    del graph

