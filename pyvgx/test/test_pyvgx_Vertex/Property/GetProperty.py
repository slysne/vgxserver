from pytest.pytest import RunTests, Expect, TestFailed
from pyvgx import *
import pyvgx

graph = None



def TEST_GetProperty():
    """
    pyvgx.Vertex.GetProperty()
    Basic
    test_level=3101
    """
    if "vertex" in graph:
        graph.DeleteVertex( "vertex" )

    V = graph.NewVertex( "vertex" )

    Expect( V.GetProperty( "x" ) is None,                   "property does not exist" )
    Expect( V.GetProperty( "x", 100 ) == 100,               "default integer" )
    Expect( V.GetProperty( "x", "string" ) == "string",     "default string" )

    V['x'] = 1234
    Expect( V.GetProperty( "x" ) == 1234,                   "integer" )
    Expect( V.GetProperty( "x", 100 ) == 1234,              "integer" )
    Expect( V.GetProperty( "x", "string" ) == 1234,         "integer" )

    V['y'] = "onetwo"
    Expect( V.GetProperty( "y" ) == "onetwo",               "string" )
    Expect( V.GetProperty( "y", 100 ) == "onetwo",          "string" )
    Expect( V.GetProperty( "y", "string" ) == "onetwo",     "string" )




def Run( name ):
    global graph
    graph = pyvgx.Graph( name )
    RunTests( [__name__] )
    graph.Close()
    del graph


