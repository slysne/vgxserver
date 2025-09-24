from pytest.pytest import RunTests, Expect, TestFailed
from . import _vertex_test_support as Support
from pyvgx import *
import pyvgx

graph = None


def TEST_AbortVertex():
    """
    pyvgx.Graph.AbortVertex()
    """
    # THIS API METHOD IS NO LONGER AVAILABLE AS OF CAP 2.0
    pass 



def Run( name ):
    """
    Run the tests in this module
    """
    global graph
    graph = pyvgx.Graph( name )
    RunTests( [__name__] )
    graph.Close()
    del graph
