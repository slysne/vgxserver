from pytest.pytest import RunTests, Expect, TestFailed
from pyvgx import *
import pyvgx

graph = None




def TEST_Relationships():
    """
    pyvgx.Graph.Relationships()
    Call method
    test_level=3101
    """
    graph.Relationships()




def Run( name ):
    global graph
    graph = pyvgx.Graph( name )
    RunTests( [__name__] )
    graph.Close()
    del graph

