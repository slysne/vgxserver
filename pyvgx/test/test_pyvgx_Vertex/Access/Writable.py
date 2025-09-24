from pytest.pytest import RunTests, Expect, TestFailed
from pyvgx import *
import pyvgx

graph = None



def TEST_Writable():
    """
    pyvgx.Vertex.Writable()
    Call method
    test_level=3101
    """
    if "vertex" in graph:
        graph.DeleteVertex( "vertex" )

    V = graph.NewVertex( "vertex" )

    Expect( V.Writable() == True,           "writable" )

    V.Close()
    V = graph.OpenVertex( "vertex", "a" )
    Expect( V.Writable() == True,           "writable" )
    
    V.Close()
    V = graph.OpenVertex( "vertex", "r" )
    Expect( V.Writable() == False,          "readonly is not writable" )

    V.Close()
    Expect( V.Writable() == False,          "closed in not writable" )



def Run( name ):
    global graph
    graph = pyvgx.Graph( name )
    RunTests( [__name__] )
    graph.Close()
    del graph

