from pytest.pytest import RunTests, Expect, TestFailed
from pyvgx import *
import pyvgx

graph = None



def TEST_Close():
    """
    pyvgx.Vertex.Close()
    Call method
    test_level=3101
    """
    vertexid = "vertex"

    if vertexid in graph:
        graph.DeleteVertex( vertexid )

    # Open
    V = graph.NewVertex( vertexid )
    # Check
    Expect( V.id == vertexid,                     "Vertex id should be %s" % vertexid )
    # Close
    V.Close()
    # Verify not accessible
    try:
        V.id
        Except( False,  "Vertex should not be accessible after close" )
    except pyvgx.AccessError as ex:
        Expect( str(ex).startswith( "Vertex is not accessible" ),  "Exception message should state that vertex is not accessible" )
    except:
        Except( False,  "Should not raise this exception" )


def Run( name ):
    global graph
    graph = pyvgx.Graph( name )
    RunTests( [__name__] )
    graph.Close()
    del graph

