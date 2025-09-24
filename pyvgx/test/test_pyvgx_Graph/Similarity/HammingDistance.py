from pytest.pytest import RunTests, Expect, TestFailed
from pyvgx import *
import pyvgx

graph = None



def TEST_HammingDistance():
    """
    TEST pyvgx.Graph.HammingDistance()
    """
    pass



def Run( g ):
    global graph
    graph = g
    RunTests( [__name__] )
