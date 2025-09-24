from pytest.pytest import RunTests, Expect, TestFailed
from pyvgx import *
import pyvgx

graph = None



def TEST_Readonly():
    """
    pyvgx.Vertex.Readonly()
    Basic
    test_level=3101
    """
    if "vertex" in graph:
        graph.DeleteVertex( "vertex" )

    V = graph.NewVertex( "vertex" )

    Expect( V.Readonly() == False,          "writable is not readonly" )

    V.Close()
    V = graph.OpenVertex( "vertex", "a" )
    Expect( V.Readonly() == False,          "writable is not readonly" )
    
    V.Close()
    V = graph.OpenVertex( "vertex", "r" )
    Expect( V.Readonly() == True,           "readonly" )

    V.Close()
    Expect( V.Readonly() == False,          "closed in not readonly" )



def Run( name ):
    global graph
    graph = pyvgx.Graph( name )
    RunTests( [__name__] )
    graph.Close()
    del graph

