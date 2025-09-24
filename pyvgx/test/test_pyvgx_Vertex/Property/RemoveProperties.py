from pytest.pytest import RunTests, Expect, TestFailed
from pyvgx import *
import pyvgx

graph = None



def TEST_RemoveProperties():
    """
    pyvgx.Vertex.RemoveProperties()
    Call method
    test_level=3101
    """
    if "vertex" in graph:
        graph.DeleteVertex( "vertex" )

    V = graph.NewVertex( "vertex" )

    for n in range( 100 ):
        for i in range(n):
            V[ "prop_%d" % i ] = i
        V.RemoveProperties()
        Expect( V.NumProperties() == 0,             "no properties" )



def Run( name ):
    global graph
    graph = pyvgx.Graph( name )
    RunTests( [__name__] )
    graph.Close()
    del graph


