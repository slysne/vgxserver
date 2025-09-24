from pytest.pytest import RunTests, Expect, TestFailed
from pyvgx import *
import pyvgx

graph = None



def TEST_Cosine():
    """
    TEST pyvgx.Graph.Cosine()
    """
    pass



def Run( g ):
    global graph
    graph = g
    RunTests( [__name__] )
