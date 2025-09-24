from pytest.pytest import RunTests, Expect, TestFailed
from . import _vertex_test_support as Support
from pyvgx import *
import pyvgx

graph = None



def TEST_CommitVertex():
    """
    pyvgx.Graph.CommitVertex()
    test_level=3101
    """
    # Reset
    graph.Truncate()

    # Implicitly create new vertices
    A = graph.OpenVertex( "A", "w" )
    B = graph.OpenVertex( "B", "w", timeout=0 )
    C = graph.OpenVertex( "C", "w", timeout=100 )
    D = graph.OpenVertex( "D", "w", timeout=100 )
    E = graph.OpenVertex( "E", "w", timeout=100 )

    Expect( graph.HasVertex( "A" ) is True,         "Vertex A should exist in graph" )

    # Commit B
    B.Commit()
    Expect( graph.HasVertex( "B" ) is True,         "Vertex B should exist in graph" )

    # Connect to uncommitted terminal
    B = graph.OpenVertex( "B" )
    Expect( graph.Connect( B, "to", C ) == 1,       "Terminal should auto-commit" )
    Expect( B.degree == 1,                          "B has one arc" )
    Expect( C.degree == 1,                          "C has one arc" )
    graph.CloseVertex( C ) 
    Expect( graph.HasVertex( "C" ) is True,         "Vertex C should exist in graph" )

    # Connect uncommitted initial to uncommitted terminal
    Expect( graph.Connect( D, "to", E ) == 1,       "Initial and terminal should auto-commit" )
    Expect( D.degree == 1,                          "D has one arc" )
    Expect( E.degree == 1,                          "E has one arc" )
    graph.CloseVertex( D )
    graph.CloseVertex( E )
    Expect( graph.HasVertex( "D" ) is True,         "Vertex D should exist in graph" )
    Expect( graph.HasVertex( "E" ) is True,         "Vertex E should exist in graph" )

    graph.CloseAll()



def Run( name ):
    """
    Run the tests in this module
    """
    global graph
    graph = pyvgx.Graph( name )
    RunTests( [__name__] )
    graph.Close()
    del graph

