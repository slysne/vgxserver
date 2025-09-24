from pytest.pytest import RunTests, Expect, TestFailed
from pyvgx import *
import pyvgx

graph = None



def TEST_DebugPrintAllocators():
    """
    pyvgx.Graph.DebugPrintAllocators()
    Call method
    test_level=3101
    """
    graph.DebugPrintAllocators()




def Run( name ):
    global graph
    graph = pyvgx.Graph( name )
    RunTests( [__name__] )
    graph.Close()
    del graph

