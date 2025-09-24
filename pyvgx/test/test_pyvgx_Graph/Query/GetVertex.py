from pytest.pytest import RunTests, Expect, TestFailed
from pyvgx import *
import pyvgx

graph = None


def TEST_GetVertex():
    """
    pyvgx.Graph.GetVertex()
    test_level=3101
    """
    node = "node_TEST_GetVertex"

    try:
        graph.GetVertex( node )
        Expect( False,              "node should not exist" )
    except KeyError:
        Expect( True,               "ok" )

    graph.CreateVertex( node )

    V = graph.GetVertex( node )
    Expect( V.Readonly(),           "readonly vertex" )

    del V
    pass
    graph.DeleteVertex( node )
    


    





def Run( name ):
    global graph
    graph = pyvgx.Graph( name )
    RunTests( [__name__] )
    graph.Close()
    del graph
