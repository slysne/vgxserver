from pytest.pytest import RunTests, Expect, TestFailed
from pyvgx import *
import pyvgx

graph = None



def TEST_Escalate():
    """
    pyvgx.Vertex.Escalate()
    Call method
    test_level=3101
    """
    name = "escalatable"

    if name in graph:
        graph.DeleteVertex( name )

    graph.CreateVertex( name )

    # Open readonly
    V = graph.OpenVertex( name, "r" )
    Expect( V.Readonly() is True,           "Vertex should be readonly" )
    Expect( V.Writable() is False,          "Vertex should not be writable" )

    # Escalate to writable
    V.Escalate()
    Expect( V.Readonly() is False,           "Vertex should not be readonly after escalation" )
    Expect( V.Writable() is True,            "Vertex should be writable after escalation" )

    V.Close()



def Run( name ):
    global graph
    graph = pyvgx.Graph( name )
    RunTests( [__name__] )
    graph.Close()
    del graph

