from pytest.pytest import RunTests, Expect, TestFailed
from pyvgx import *
import pyvgx

graph = None



def TEST_Relax():
    """
    pyvgx.Vertex.Relax()
    Call method
    test_level=3101
    """
    name = "relaxable"

    if name in graph:
        graph.DeleteVertex( name )

    graph.CreateVertex( name )

    # Open writable
    V = graph.OpenVertex( name, "a" )
    Expect( V.Readonly() is False,           "Vertex should not be readonly" )
    Expect( V.Writable() is True,            "Vertex should be writable" )

    # Relax to readonly
    V.Relax()
    Expect( V.Readonly() is True,           "Vertex should be readonly after relax" )
    Expect( V.Writable() is False,          "Vertex should be not writable after relax" )

    V.Close()



def Run( name ):
    global graph
    graph = pyvgx.Graph( name )
    RunTests( [__name__] )
    graph.Close()
    del graph

