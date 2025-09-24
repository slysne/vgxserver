from pytest.pytest import RunTests, Expect, TestFailed
from pyvgx import *
import pyvgx

graph = None






def TEST_PropertyStringValues():
    """
    pyvgx.Graph.PropertyStringValues()
    Call method
    test_level=3101
    """
    graph.PropertyStringValues()



def Run( name ):
    global graph
    graph = pyvgx.Graph( name )
    RunTests( [__name__] )
    graph.Close()
    del graph

