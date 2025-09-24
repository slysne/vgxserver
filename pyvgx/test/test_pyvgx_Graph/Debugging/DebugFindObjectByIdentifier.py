from pytest.pytest import RunTests, Expect, TestFailed
from pyvgx import *
import pyvgx

graph = None



def TEST_DebugFindObjectByIdentifier():
    """
    pyvgx.Graph.DebugFindObjectByIdentifier()
    Call method
    test_level=3101
    """
    graph.CreateVertex( "A" )
    graph.DebugFindObjectByIdentifier( "A" )




def Run( name ):
    global graph
    graph = pyvgx.Graph( name )
    RunTests( [__name__] )
    graph.Close()
    del graph

