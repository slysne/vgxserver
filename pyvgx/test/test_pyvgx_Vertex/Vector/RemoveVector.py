from pytest.pytest import RunTests, Expect, TestFailed
from pyvgx import *
import pyvgx

graph = None



def TEST_RemoveVector():
    """
    pyvgx.Vertex.RemoveVector()
    Basic
    test_level=3101
    """
    if "vertex" in graph:
        graph.DeleteVertex( "vertex" )

    V = graph.NewVertex( "vertex" )

    Expect( V.HasVector() is False,             "no vector " ) 

    V.RemoveVector()
    Expect( V.HasVector() is False,             "no vector " ) 

    V.SetVector( [('a',1)] )
    Expect( V.HasVector() is True,              "has vector " ) 

    V.RemoveVector()
    Expect( V.HasVector() is False,             "no vector " ) 




def Run( name ):
    global graph
    graph = pyvgx.Graph( name )
    RunTests( [__name__] )
    graph.Close()
    del graph

