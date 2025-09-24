from pytest.pytest import RunTests, Expect, TestFailed
from . import _vertex_test_support as Support
from pyvgx import *
import pyvgx

graph = None


NAME = "V-esc"
SETUP = False


def TEST_EscalateVertex_setup():
    """
    pyvgx.Graph.EscalateVertex()
    Setup
    test_level=3101
    t_nominal=1
    """
    global SETUP
    if not SETUP:
        graph.Truncate()
        Expect( graph.CreateVertex( NAME ) == 1,          "Should create vertex" )
        SETUP = True



def TEST_EscalateVertex_normal():
    """
    Normal escalation from readonly to writable
    test_level=3101
    t_nominal=1
    """
    TEST_EscalateVertex_setup()

    # Open the vertex readonly
    V = graph.OpenVertex( NAME, mode="r" )
    Expect( V.Readonly(),                             "Vertex should be readonly" )

    # Escalate vertex to writable
    graph.EscalateVertex( V )
    Expect( V.Writable(),                             "Vertex should be writable" )

    # Close
    graph.CloseVertex( V )



def TEST_EscalateVertex_error1():
    """
    pyvgx.Graph.EscalateVertex()
    Fail to escalate already writable
    test_level=3101
    t_nominal=1
    """
    TEST_EscalateVertex_setup()

    # Open the vertex writable
    V = graph.OpenVertex( NAME, mode="w" )
    Expect( V.Writable(),                             "Vertex should be writable" )

    # Try (and fail) to escalate already writable vertex
    try:
        graph.EscalateVertex( V )
        raise TestFailed( "Should not be able to escalate writable vertex" )
    # The correct exception
    except pyvgx.AccessError:
        pass
    # Incorrect exception
    except Exception as ex:
        raise TestFailed( "Incorrect exception: %s" % ex )



def TEST_EscalateVertex_error2():
    """
    pyvgx.Graph.EscalateVertex()
    Fail to escalate vertex with multiple read locks
    test_level=3101
    t_nominal=1
    """
    TEST_EscalateVertex_setup()

    # Open the vertex twice readonly
    V1 = graph.OpenVertex( NAME, mode="r" )
    V2 = graph.OpenVertex( NAME, mode="r" )

    # Try (and fail) to escalate vertex with multiple read locks
    try:
        graph.EscalateVertex( V1 )
        raise TestFailed( "Should not be able to escalate vertex with multiple read locks" )
    # The correct exception
    except pyvgx.AccessError:
        pass
    # Incorrect exception
    except Exception as ex:
        raise TestFailed( "Incorrect exception: %s" % ex )



def Run( name ):
    """
    Run the tests in this module
    """
    global graph
    graph = pyvgx.Graph( name )
    RunTests( [__name__] )
    graph.Close()
    del graph


