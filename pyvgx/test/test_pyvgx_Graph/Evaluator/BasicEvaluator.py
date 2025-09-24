from pytest.pytest import RunTests, Expect, TestFailed
from pyvgx import *
import pyvgx


g = None
graph_name = "eval_graph"


# TODO: This is a stub. We need to add tests for all basic functionality
#
#


def TEST_properties():
    """
    Evaluator properties
    t_nominal=1
    test_level=501
    """
    g.Truncate()

    A = g.NewVertex("A")
    val1 = "First Disk Property"
    val2 = "Second Disk Property"
    val3 = "Third Disk Property"
    A['*vprop1'] = val1
    A['*vprop2'] = val2
    A['*vprop3'] = val3
    A.Close()

    Expect( g.Evaluate( f"vertex.property('vprop1') == '{val1}'", tail="A" ),       "vprop1" )
    Expect( g.Evaluate( f"vertex.property('vprop2') == '{val2}'", tail="A" ),       "vprop2" )
    Expect( g.Evaluate( f"vertex.property('vprop3') == '{val3}'", tail="A" ),       "vprop3" )

    vprop1 = g.Evaluate( "vertex.property('vprop1')", tail="A" )
    vprop2 = g.Evaluate( "vertex.property('vprop2')", tail="A" )
    vprop3 = g.Evaluate( "vertex.property('vprop3')", tail="A" )

    Expect( vprop1 == val1,                                         f"'{val1}', got '{vprop1}'" )
    Expect( vprop2 == val2,                                         f"'{val2}', got '{vprop2}'" )
    Expect( vprop3 == val3,                                         f"'{val3}', got '{vprop3}'" )

    joined = g.Evaluate( f"a = vertex.property('vprop1'); b = vertex.property('vprop2'); c = vertex.property('vprop3'); join(',',a,b,c)", tail="A" )
    xval = ",".join([val1,val2,val3])
    Expect( joined == xval,                                         f"'{xval}', got '{joined}'" )



def Run( name ):
    global g
    g = Graph( graph_name )

    RunTests( [__name__] )

    g.Erase()
