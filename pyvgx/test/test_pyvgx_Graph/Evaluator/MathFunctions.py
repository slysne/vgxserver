from pytest.pytest import RunTests, Expect, TestFailed
from pyvgx import *
import pyvgx






def TEST_Evaluator_MathFunctions():
    """
    pyvgx Evaluator Math Functions
    test_level=3101
    """
    graph_name = "eval_graph"
    g = Graph( graph_name )

    Expect( g.Evaluate( "sqrt( 10 ) * sqrt( 10 ) == 10" ) )

    g.Erase()





def Run( name ):
    RunTests( [__name__] )
