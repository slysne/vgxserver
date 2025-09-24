from pytest.pytest import RunTests, Expect, TestFailed
from . import _vertex_test_support as Support
from pyvgx import *
import pyvgx

graph = None



def TEST_CloseVertex():
    """
    pyvgx.Graph.CloseVertex()
    test_level=3101
    """
    # Reset
    graph.Truncate()

    # Open, then close and check
    A = graph.NewVertex( "A" )
    Support._VerifyNewVertex( graph, A, "A" )
    Expect( graph.CloseVertex( A ) is True,       "Vertex should be closed" )
    Expect( A.Writable() is False,                "Vertex should not be writable" )
    Expect( A.Readable() is False,                "Vertex should not be readable" )
    Expect( A.Readonly() is False,                "Vertex should not be readonly" )
    try:
        A.id
        Except( False,  "Vertex should not be accessible after close" )
    except pyvgx.AccessError as ex:
        Expect( str(ex).startswith( "Vertex is not accessible" ),  "Exception message should state that vertex is not accessible" )
    except:
        Except( False,  "Should not raise this exception" )



def Run( name ):
    """
    Run the tests in this module
    """
    global graph
    graph = pyvgx.Graph( name )
    RunTests( [__name__] )
    graph.Close()
    del graph
