from pytest.pytest import RunTests, Expect, TestFailed
from pyvgx import *
import pyvgx

graph = None



def TEST_VertexAsDict():
    """
    pyvgx.Vertex.AsDict()
    test_level=3101
    """
    # Get the node we will use for testing
    V = graph.NewVertex( "Node1", "node" )

    # Set some properties
    V['x'] = 17
    V['y'] = 3.14
    V['z'] = "something"

    # Set a vector
    V.SetVector( [ ("this",1.2), ("is",1.0), ("a",0.7), ("vector",0.3) ] )

    # Get the vertex as a dictionary
    D = V.AsDict()

    # Verify
    Expect( D['id'] == V.id,                                "V.AsDict()['id'] should match V.id" )
    Expect( D['internalid'] == V.internalid,                "V.AsDict()['internalid'] should match V.internalid" )
    Expect( D['odeg'] == V.odeg,                            "V.AsDict()['odeg'] should match V.odeg" )
    Expect( D['ideg'] == V.ideg,                            "V.AsDict()['ideg'] should match V.ideg" )
    Expect( D['deg'] == V.deg,                              "V.AsDict()['deg'] should match V.deg" )
    Expect( D['descriptor'] == V.descriptor,                "V.AsDict()['descriptor'] should match V.descriptor" )
    Expect( D['type'] == V.type,                            "V.AsDict()['type'] should match V.type" )
    Expect( D['man'] == V.man,                              "V.AsDict()['man'] should match V.man" )
    Expect( D['vector'] == V.vector,                        "V.AsDict()['vector'] should match V.vector" )
    Expect( D['properties'] == V.GetProperties(),           "V.AsDict()['properties'] should match V.properties" )
    Expect( D['tmc'] == V.tmc,                              "V.AsDict()['tmc'] should match V.tmc" )
    Expect( D['tmm'] == V.tmm,                              "V.AsDict()['tmm'] should match V.tmm" )
    Expect( D['tmx'] == V.tmx,                              "V.AsDict()['tmx'] should match V.tmx" )
    Expect( D['rank']['c1'] == V.c1,                        "V.AsDict()['rank']['c1'] should match V.c1" )
    Expect( D['rank']['c0'] == V.c0,                        "V.AsDict()['rank']['c0'] should match V.c0" )
    Expect( D['virtual'] == V.virtual,                      "V.AsDict()['virtual'] should match V.virtual" )
    Expect( D['address'] == V.address,                      "V.AsDict()['address'] should match V.address" )
    Expect( D['index'] == V.index,                          "V.AsDict()['index'] should match V.index" )
    Expect( D['bitindex'] == V.bitindex,                    "V.AsDict()['bitindex'] should match V.bitindex" )
    Expect( D['bitvector'] == V.bitvector,                  "V.AsDict()['bitvector'] should match V.bitvector" )
    Expect( D['op'] == V.op,                                "V.AsDict()['op'] should match V.op" )
    Expect( D['enum'] == V.enum,                            "V.AsDict()['enum'] should match V.enum" )

    # Close and delete vertex
    graph.CloseVertex( V )
    graph.DeleteVertex( "Node1" )




def Run( name ):
    global graph
    graph = pyvgx.Graph( name )
    RunTests( [__name__] )
    graph.Close()
    del graph


