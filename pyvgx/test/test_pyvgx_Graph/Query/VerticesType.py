from pytest.pytest import RunTests, Expect, TestFailed
from pyvgx import *
import pyvgx

graph = None



def TEST_VerticesType():
    """
    pyvgx.Graph.VerticesType()
    t_nominal=12
    test_level=3101
    """
    g = Graph( "vertices" )
    g.Truncate()

    TYPES = ['red', 'blue', 'green', 'orange', 'black']

    N = 1000
    for n in range( 1000 ):
        name = "typeless_%d" % n
        g.CreateVertex( name )
    for tp in TYPES:
        name = "%s_%d" % (tp, n)
        g.CreateVertex( name, type=tp )


    for tp in TYPES:
        result1 = set( g.VerticesType( tp ) )
        result2 = set( g.Vertices( condition={'type':tp} ) )
        Expect( result1 == result2,             "all type %s" % tp  )
        for name in result1:
            Expect( g[name].type == tp,         "all type %s" % tp )
 
    result1 = set( g.VerticesType( None ) )
    result2 = set( g.VerticesType( "__vertex__" ) )
    Expect( result1 == result2,                 "all typeless" )
    for name in result1:
        Expect( name.startswith( "typeless" ),  "typeless" )

    result = g.VerticesType( "nothing" )
    Expect( len(result) == 0,                   "no vertices of this type" )
    g.Truncate()




def Run( name ):
    global graph
    graph = pyvgx.Graph( name )
    RunTests( [__name__] )
    graph.Close()
    del graph

