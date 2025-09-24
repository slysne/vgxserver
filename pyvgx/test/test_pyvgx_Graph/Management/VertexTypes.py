from pytest.pytest import RunTests, Expect, TestFailed
from pyvgx import *
import pyvgx

graph = None






def TEST_VertexTypes():
    """
    pyvgx.Graph.VertexTypes()
    test_level=3101
    """
    g = Graph( "vertices" )
    g.Truncate()

    N = 100
    indexed = dict()

    # empty
    result = g.VertexTypes()
    Expect( len(result) == 0,               "no vertices" )

    # typeless
    indexed[ '__vertex__' ] = 0
    for n in range( N ):
        g.CreateVertex( "typeless_%d" % n )
        indexed[ '__vertex__' ] += 1

    result = g.VertexTypes()
    Expect( len(result) == 1,               "one type" )
    Expect( "__vertex__" in result,         "__vertex__ type" )

    # other types
    TYPES = [ 'something', 'another', 'this', 'that', 'thing', 'node' ]
    i = 0
    for tp in TYPES:
        i += 1
        indexed[ tp ] = 0
        for n in range( N+i ):
            name = "type_%s_%d" % (tp, n)
            g.CreateVertex( name, type=tp )
            indexed[ tp ] += 1

        result = g.VertexTypes()
        Expect( len(result) == len(indexed),        "%d types in graph" % len(indexed) )

    result = g.VertexTypes()
    typenames = set( result.keys() )
    Expect( typenames == set(indexed),              "all types indexed" )
    enums = set( [enum for enum, c in list(result.values())] )
    Expect( len(enums) == len(indexed),             "unique enumerations" )
    for enum in enums:
        Expect( type(enum) is int,                  "integer enumeration" )







def Run( name ):
    global graph
    graph = pyvgx.Graph( name )
    RunTests( [__name__] )
    graph.Close()
    del graph

