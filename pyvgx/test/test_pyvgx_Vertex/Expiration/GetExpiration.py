from pytest.pytest import RunTests, Expect, TestFailed
from pyvgx import *
import pyvgx
import time

graph = None



def TEST_GetExpiration():
    """
    pyvgx.Vertex.GetExpiration()
    Basic
    test_level=3101
    """
    if "vertex" in graph:
        graph.DeleteVertex( "vertex" )

    V = graph.NewVertex( "vertex" )

    Expect( V.GetExpiration() == T_NEVER,   "no expiration" )

    tmx = int( time.time() ) + 60

    V.SetExpiration( tmx )
    Expect( V.GetExpiration() == tmx,       "expiration set" )

    V.ClearExpiration()
    Expect( V.GetExpiration() == T_NEVER,   "no expiration" )





def Run( name ):
    global graph
    graph = pyvgx.Graph( name )
    RunTests( [__name__] )
    graph.Close()
    del graph


