from pytest.pytest import RunTests, Expect, TestFailed
import time
from pyvgx import *
import pyvgx
import random

graph = None


def float_eq( a, b ):
    if b:
        return abs( 1 - ( a / b ) ) < 1e-5
    else:
        return abs( a ) < 1e-5


def TEST_Accumulate():
    """
    pyvgx.Graph.Accumulate()
    test_level=3101
    """
    graph.Truncate()

    A = graph.NewVertex( "A" )
    B = graph.NewVertex( "B" )

    random.seed( 1000 )

    val = 0.0
    for n in range( 1000 ):
        inc = 100 * (random.random()-0.5)
        val += inc
        acc = graph.Accumulate( A, "to", B, inc )
        Expect( float_eq( acc, val ), "accumulated value should be %f, got %f" % (val, acc) )


    graph.CloseVertex(A)
    graph.CloseVertex(B)
    graph.DeleteVertex( "A" )
    graph.DeleteVertex( "B" )


def Run( name ):
    global graph
    graph = pyvgx.Graph( name )
    RunTests( [__name__] )
    graph.Truncate()
    graph.Close()
    del graph

