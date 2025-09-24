from pytest.pytest import RunTests, Expect, TestFailed
from pyvgx import *
import pyvgx

graph = None



def TEST_Readable():
    """
    pyvgx.Vertex.Readable()
    Basic
    test_level=3101
    """
    if "vertex" in graph:
        graph.DeleteVertex( "vertex" )

    V = graph.NewVertex( "vertex" )

    Expect( V.Readable() == True,           "writable is readable" )

    V.Close()
    V = graph.OpenVertex( "vertex", "a" )
    Expect( V.Readable() == True,           "writable is readable" )
    
    V.Close()
    V = graph.OpenVertex( "vertex", "r" )
    Expect( V.Readable() == True,           "readonly is readable" )

    V.Close()
    Expect( V.Readable() == False,          "closed in not readable" )



def Run( name ):
    global graph
    graph = pyvgx.Graph( name )
    RunTests( [__name__] )
    graph.Close()
    del graph

