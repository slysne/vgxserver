from pytest.pytest import RunTests, Expect, TestFailed
from . import _vertex_test_support as Support
from pyvgx import *
import pyvgx

graph = None


NAME = "V-relax"
SETUP = False


def TEST_RelaxVertex_setup():
    """
    pyvgx.Graph.RelaxVertex()
    Setup
    test_level=3101
    """
    global SETUP
    if not SETUP:
        graph.Truncate()
        Expect( graph.CreateVertex( NAME ) == 1,            "Should create vertex" )
        SETUP = True


def TEST_RelaxVertex_normal():
    """
    pyvgx.Graph.RelaxVertex()
    Normal relax
    test_level=3101
    """
    TEST_RelaxVertex_setup()

    # Open the vertex writable
    V = graph.OpenVertex( NAME, mode="w" )
    Expect( V.Writable(),                               "Vertex should be writable" )

    # Relax vertex to readonly
    graph.RelaxVertex( V )
    Expect( V.Readonly(),                               "Vertex should be readonly" )

    # Close
    graph.CloseVertex( V )



def TEST_RelaxVertex_fallback():
    """
    pyvgx.Graph.RelaxVertex()
    Fallback to close vertex
    test_level=3101
    """
    TEST_RelaxVertex_setup()

    # Open the vertex writable twice
    V1 = graph.OpenVertex( NAME, mode="w" )
    Expect( V1.Writable(),                              "Vertex should be writable" )
    V2 = graph.OpenVertex( NAME, mode="w" )
    Expect( V2.Writable(),                              "Vertex should be writable" )

    # Relax = close
    graph.RelaxVertex( V1 )
    Expect( V1.Writable(),                              "Vertex should still be writable" )

    # Relax to readonly
    graph.RelaxVertex( V2 )
    Expect( V2.Readonly(),                              "Vertex should be readonly" )

    # Close
    graph.CloseVertex( V2 )



def TEST_RelaxVertex_noop():
    """
    pyvgx.Graph.RelaxVertex()
    Relax has no effect on readonly vertex
    test_level=3101
    """
    TEST_RelaxVertex_setup()

    # Open the vertex readonly
    V = graph.OpenVertex( NAME, mode="r" )
    Expect( V.Readonly(),                               "Vertex should be readonly" )

    # Relax with no effect
    graph.RelaxVertex( V )
    Expect( V.Readonly(),                               "Vertex should still be readonly" )

    # Close
    graph.CloseVertex( V )



def Run( name ):
    """
    Run the tests in this module
    """
    global graph
    graph = pyvgx.Graph( name )
    RunTests( [__name__] )
    graph.Close()
    del graph



