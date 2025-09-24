from pytest.pytest import RunTests, Expect, TestFailed
from pyvgx import *
import pyvgx

graph = None



def TEST_VertexVector():
    """
    TEST pyvgx.Graph.VertexVector()
    """
    pass



def Run( g ):
    global graph
    graph = g
    RunTests( [__name__] )
