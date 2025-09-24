from pytest.pytest import RunTests, Expect, TestFailed
from pyvgx import *
import pyvgx

graph = None



def TEST_NumProperties():
    """
    pyvgx.Vertex.NumProperties()
    Basic
    test_level=3101
    """
    if "vertex" in graph:
        graph.DeleteVertex( "vertex" )

    V = graph.NewVertex( "vertex" )

    Expect( V.NumProperties() == 0,                         "no properties" )

    V['x'] = 1
    Expect( V.NumProperties() == 1,                         "one property" )

    V['y'] = 1
    Expect( V.NumProperties() == 2,                         "two properties" )
    
    del V['x']
    Expect( V.NumProperties() == 1,                         "one property" )

    del V['y']
    Expect( V.NumProperties() == 0,                         "no properties" )




def Run( name ):
    global graph
    graph = pyvgx.Graph( name )
    RunTests( [__name__] )
    graph.Close()
    del graph


