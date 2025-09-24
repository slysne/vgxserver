from pytest.pytest import RunTests, Expect, TestFailed
from pyvgx import *
import pyvgx

graph = None


def TEST_vxquery_inspect():
    """
    Core vxquery_inspect
    test_level=501
    """
    try:
        pyvgx.selftest( force=True, testroot="vgxtest", library="vgx", names=["vxquery_inspect.c" ] )
    except:
        Expect( False )



def TEST_HasVertex():
    """
    pyvgx.Graph.HasVertex()
    test_level=3101
    """
    node = "node_TEST_HasVertex"

    Expect( graph.HasVertex( node ) == False,           "node does not exist" )
    graph.CreateVertex( node )
    Expect( graph.HasVertex( node ) == True,            "node exists" )
    graph.DeleteVertex( node )
    Expect( graph.HasVertex( node ) == False,           "node does not exist" )



def Run( name ):
    global graph
    graph = pyvgx.Graph( name )
    RunTests( [__name__] )
    graph.Close()
    del graph
