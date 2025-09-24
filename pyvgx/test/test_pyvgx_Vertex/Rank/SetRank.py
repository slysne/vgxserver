from pytest.pytest import RunTests, Expect, TestFailed
from pyvgx import *
import pyvgx

graph = None



def TEST_SetRank():
    """
    pyvgx.Vertex.SetRank()
    Basic
    test_level=3101
    """
    if "vertex" in graph:
        graph.DeleteVertex( "vertex" )

    V = graph.NewVertex( "vertex" )

    c1, c0 = V.SetRank()
    Expect( c1 == 1.0 )
    Expect( c0 == 0.0 )

    c1, c0 = V.SetRank( 2 )
    Expect( c1 == 2.0 )
    Expect( c0 == 0.0 )

    c1, c0 = V.SetRank( c0=1024 )
    Expect( c1 == 2.0 )
    Expect( c0 == 1024.0 )

    c1, c0 = V.SetRank( c1=4.0 )
    Expect( c1 == 4.0 )
    Expect( c0 == 1024.0 )

    c1, c0 = V.SetRank( c1=8.0, c0=512 )
    Expect( c1 == 8.0 )
    Expect( c0 == 512.0 )

    c1, c0 = V.SetRank()
    Expect( c1 == 8.0 )
    Expect( c0 == 512.0 )



def Run( name ):
    global graph
    graph = pyvgx.Graph( name )
    RunTests( [__name__] )
    graph.Close()
    del graph

