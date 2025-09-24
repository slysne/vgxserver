from pytest.pytest import RunTests, Expect, TestFailed
from pyvgx import *
import pyvgx

graph = None



def TEST_ShowVertex():
    """
    pyvgx.Graph.ShowVertex()
    Call method
    test_level=3101
    """
    graph.CreateVertex( "show_this" )
    graph.ShowVertex( "show_this" )





def Run( name ):
    global graph
    graph = pyvgx.Graph( name )
    RunTests( [__name__] )
    graph.Close()
    del graph

