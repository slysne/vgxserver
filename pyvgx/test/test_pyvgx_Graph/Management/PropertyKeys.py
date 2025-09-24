from pytest.pytest import RunTests, Expect, TestFailed
from pyvgx import *
import pyvgx

graph = None




def TEST_PropertyKeys():
    """
    pyvgx.Graph.PropertyKeys()
    Call method
    test_level=3101
    """
    graph.PropertyKeys()



def Run( name ):
    global graph
    graph = pyvgx.Graph( name )
    RunTests( [__name__] )
    graph.Close()
    del graph

