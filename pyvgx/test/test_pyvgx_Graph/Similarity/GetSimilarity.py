from pytest.pytest import RunTests, Expect, TestFailed
from pyvgx import *
import pyvgx

graph = None



def TEST_GetSimilarity():
    """
    TEST pyvgx.Graph.GetSimilarity()
    """
    pass



def Run( g ):
    global graph
    graph = g
    RunTests( [__name__] )
