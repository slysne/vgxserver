from pytest.pytest import RunTests, Expect, TestFailed
from pyvgx import *
import pyvgx

graph = None



def TEST_Size():
    """
    pyvgx.Graph.Size()
    test_level=3101
    """
    g = Graph( "vertices" )
    g.Truncate()

    N = 100
    i = 0
    for n in range( N ):
        g.Connect( "node_%d" % n, "to", "node_%d" % (n+1) )
        i += 1
        Expect( g.Size() == i,                      "%d arcs" % i )
        
        g.Connect( "node_%d" % n, ("to",M_INT,n), "node_%d" % (n+1) )
        i += 1
        Expect( g.Size() == i,                      "%d arcs" % i )

    g.Erase()



def Run( name ):
    global graph
    graph = pyvgx.Graph( name )
    RunTests( [__name__] )
    graph.Close()
    del graph

