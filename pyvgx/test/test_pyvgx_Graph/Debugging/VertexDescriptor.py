from pytest.pytest import RunTests, Expect, TestFailed
from pyvgx import *
import pyvgx

graph = None



def TEST_VertexDescriptor():
    """
    pyvgx.Graph.VertexDescriptor()
    Call method
    test_level=3101
    """
    graph.CreateVertex( "some_vertex" )
    graph.VertexDescriptor( "some_vertex" )




def Run( name ):
    global graph
    graph = pyvgx.Graph( name )
    RunTests( [__name__] )
    graph.Close()
    del graph

