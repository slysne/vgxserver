from pytest.pytest import RunTests, Expect, TestFailed
from pyvgx import *
import pyvgx

graph = None



def TEST_IsVirtual():
    """
    pyvgx.Vertex.IsVirtual()
    Basic
    test_level=3101
    """
    if "initial" in graph:
        graph.DeleteVertex( "initial" )
    if "terminal" in graph:
        graph.DeleteVertex( "terminal" )

    graph.Connect( "initial", "to", "terminal" )

    Expect( graph['initial'].IsVirtual() is False,      "initial implicitly created as REAL" )
    Expect( graph['terminal'].IsVirtual() is True,      "terminal implicitly created as VIRTUAL" )



def Run( name ):
    global graph
    graph = pyvgx.Graph( name )
    RunTests( [__name__] )
    graph.Close()
    del graph


