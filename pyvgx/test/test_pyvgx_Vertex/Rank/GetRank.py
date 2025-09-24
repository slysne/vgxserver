from pytest.pytest import RunTests, Expect, TestFailed
from pyvgx import *
import pyvgx

graph = None



def TEST_GetRank():
    """
    pyvgx.Vertex.GetRank()
    Basic
    test_level=3101
    """
    if "vertex" in graph:
        graph.DeleteVertex( "vertex" )

    V = graph.NewVertex( "vertex" )

    c1, c0 = V.GetRank()
    Expect( c1 == 1.0 )
    Expect( c0 == 0.0 )

    Expect( V.SetRank( 1 ) == V.GetRank() )
    Expect( V.SetRank( 2 ) == V.GetRank() )
    Expect( V.SetRank( 3 ) == V.GetRank() )
    Expect( V.SetRank( 4, 100 ) == V.GetRank() )
    Expect( V.SetRank( 5, 200 ) == V.GetRank() )



def Run( name ):
    global graph
    graph = pyvgx.Graph( name )
    RunTests( [__name__] )
    graph.Close()
    del graph

