from pytest.pytest import RunTests, Expect, TestFailed
from pyvgx import *
import pyvgx

graph = None


NODE = "Vertex with Property"
SETUP = False


def TEST_SetProperties_setup():
    """
    pyvgx.Vertex.SetProperties()
    Setup
    test_level=3101
    t_nominal=1
    """
    global SETUP
    if not SETUP:
        graph.Truncate()
        V = graph.NewVertex( NODE, "props_node" )
        Expect( V.HasProperties() is False,                           "Vertex should have no properties" )
        Expect( V.NumProperties() == 0,                               "Vertex should have no properties" )
        graph.CloseVertex( V )
        SETUP = True



def TEST_SetProperties_basic():
    """
    pyvgx.Vertex.SetProperties()
    Basic properties
    test_level=3101
    t_nominal=1
    """
    TEST_SetProperties_setup()

    V = graph.OpenVertex( NODE, mode="a" )

    # Add nothing
    D = {}
    V.SetProperties( D )
    Expect( V.HasProperties() is False,                           "Vertex should have no properties" )

    # Add integer property
    D = { 'x': 2 }
    V.SetProperties( D )
    Expect( V.NumProperties() == 1,                               "Vertex should have one property, got %d" % V.NumProperties() )
    Expect( V['x'] == 2,                                          "x should be 2" )

    # Add nothing
    D = {}
    V.SetProperties( D )
    Expect( V.NumProperties() == 1,                               "Vertex should have one property, got %d" % V.NumProperties() )
    Expect( V['x'] == 2,                                          "x should be 2" )

    # Overwrite integer property
    D = { 'x': 1 }
    V.SetProperties( D )
    Expect( V.NumProperties() == 1,                               "Vertex should have one property, got %d" % V.NumProperties() )
    Expect( V['x'] == 1,                                          "x should be 1" )

    # Add float property
    D = { 'y': 2.0 }
    V.SetProperties( D )
    Expect( V.NumProperties() == 2,                               "Vertex should have two properties, got %d" % V.NumProperties() )
    Expect( V['x'] == 1,                                          "x should be 1" )
    Expect( V['y'] == 2.0,                                        "y should be 2.0" )

    # Add another integer property
    D = { 'z': 3 }
    V.SetProperties( D )
    Expect( V.NumProperties() == 3,                               "Vertex should have three properties, got %d" % V.NumProperties() )
    Expect( V['x'] == 1,                                          "x should be 1" )
    Expect( V['y'] == 2.0,                                        "y should be 2.0" )
    Expect( V['z'] == 3,                                          "z should be 3" )

    # Remove all properties
    V.RemoveProperties()
    Expect( V.NumProperties() == 0,                               "Vertex should have no properties, got %d" % V.NumProperties() )

    graph.CloseVertex( V )



def TEST_SetProperties_many():
    """
    pyvgx.Vertex.SetProperties()
    Many properties
    test_level=3102
    t_nominal=1
    """
    TEST_SetProperties_setup()

    V = graph.OpenVertex( NODE, mode="a" )
    V.RemoveProperties()
    Expect( V.NumProperties() == 0,                                 "Vertex should have no properties, got %d" % V.NumProperties() )

    # Set many
    N = 10000
    D = dict([("p%s"%x,x) for x in range(N)])
    V.SetProperties( D )

    # Verify many
    Expect( V.NumProperties() == N,                                 "Vertex should have %d properties, got %d" % (N, V.NumProperties()) )
    for k,v in list(D.items()):
        Expect( V[k] == v,                                          "property '%s' should be %s, got %s" % (k, v, V[k]) )

    V.RemoveProperties()
    Expect( V.NumProperties() == 0,                                 "Vertex should have no properties, got %d" % V.NumProperties() )
    graph.CloseVertex( V )






def Run( name ):
    global graph
    graph = pyvgx.Graph( name )
    RunTests( [__name__] )
    graph.Close()
    del graph



