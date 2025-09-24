from pytest.pytest import RunTests, Expect, TestFailed
from pyvgx import *
import pyvgx

graph = None



def TEST_HasProperties():
    """
    pyvgx.Vertex.HasProperties()
    Basic
    test_level=3101
    """
    if "vertex" in graph:
        graph.DeleteVertex( "vertex" )

    V = graph.NewVertex( "vertex" )
    Expect( V.HasProperties() is False,                 "no properties" )

    V['x'] = 1
    Expect( V.HasProperties() is True,                  "one property" )

    del V['x']
    Expect( V.HasProperties() is False,                 "no properties" )






def Run( name ):
    global graph
    graph = pyvgx.Graph( name )
    RunTests( [__name__] )
    graph.Close()
    del graph


