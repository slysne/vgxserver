from pytest.pytest import RunTests, Expect, TestFailed
from pyvgx import *
import pyvgx

graph = None



def TEST_ShowOpenVertices():
    """
    pyvgx.Graph.ShowOpenVertices()
    test_level=3101
    """
    Expect( graph.ShowOpenVertices() == None,       "returns None, will only print to stdout" )



def Run( name ):
    global graph
    graph = pyvgx.Graph( name )
    RunTests( [__name__] )
    graph.Close()
    del graph


