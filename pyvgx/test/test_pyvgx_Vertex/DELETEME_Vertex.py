from pytest.pytest import Expect, RunTests, TestFailed
from pyvgx import *
import pyvgx
import random
import math
import sys
import time
import md5
import re
import exceptions


graph = None








def TEST_IntProperties():
    """
    Verify that we can set, get, manipulate and remove integer properties
    """
    # Get the node we will use for testing properties
    V = graph.OpenVertex( "P" )

    # Get the number of properties before start
    num_start = V.NumProperties()

    # Set integer property
    Expect( V.HasProperty( "int1" ) is False,                     "Vertex should not already contain property 'int1'" )
    V.SetProperty( "int1", 1 )

    # Get integer property
    Expect( V.HasProperty( "int1" ) is True,                      "Vertex should contain property 'int1' after being set" )
    Expect( V.GetProperty( "int1" ) == 1,                         "V.GetProperty( 'int1' ) should return value 1" )
    Expect( V.HasProperty( "int1", 1 ) is True,                   "V.HasProperty( 'int1', 1 ) should be True" )
    Expect( V.HasProperty( "int1", 2 ) is False,                  "V.HasProperty( 'int1', 2 ) should be False" )
    Expect( V.HasProperties() is True,                            "Vertex should contain properties" )
    Expect( V.GetProperties().get("int1") == 1,                   "V.GetProperties() should return a dict containing 'int1':1" )

    # Overwrite integer property
    V.SetProperty( "int1", 123 )
    Expect( V.GetProperty( "int1" ) == 123,                       "V.GetProperty( 'int1' ) should return value 123" )

    # Increment existing integer property
    Expect( V.IncProperty( "int1" ) == 124,                       "V.IncProperty( 'int1' ) should return 124" )
    Expect( V.GetProperty( "int1" ) == 124,                       "V.GetProperty( 'int1' ) should return 124 after increment" )
    Expect( V.IncProperty( "int1", 5 ) == 129,                    "V.IncProperty( 'int1', 5 ) should return 129" )
    Expect( V.GetProperty( "int1" ) == 129,                       "V.GetProperty( 'int1' ) should return 129 after delta increment" )
    Expect( V.IncProperty( "int1", -130 ) == -1,                  "V.IncProperty( 'int1', -130 ) should return -1" )
    Expect( V.GetProperty( "int1" ) == -1,                        "V.GetProperty( 'int1' ) should return -1 after delta increment by negative value" )

    # Remove integer property
    V.RemoveProperty( "int1" )
    Expect( V.HasProperty( "int1" ) is False,                     "Vertex should not contain property 'int1' after removal" )

    # Increment without pre-existing
    Expect( V.IncProperty( "val" ) == 1,                          "V.IncProperty( 'val' ) should return 1" )
    Expect( V.GetProperty( "val" ) == 1,                          "V.GetProperty( 'val' ) should return 1 after increment from non-exist" )
    Expect( V.IncProperty( "val" ) == 2,                          "V.IncProperty( 'val' ) should return 2" )
    Expect( V.GetProperty( "val" ) == 2,                          "V.GetProperty( 'val' ) should return 2 after increment" )

    # Increment, convert to float
    Expect( float_eq( V.IncProperty( "val", 1.1 ),  3.1),        "V.IncProperty( 'val', 1.1 ) should convert to float and return 3.1" )

    # Remove the property
    V.RemoveProperty( "val" )

    # Get the number of properties when done (should be the same as what we started with)
    num_end = V.NumProperties()
    Expect( num_start == num_end,                                 "Number of properties should be the same as what we started with" )

    # Close vertex
    graph.CloseVertex( V )



def TEST_FloatProperties():
    """
    Verify that we can set, get, manipulate and remove float properties
    """
    # Get the node we will use for testing properties
    V = graph.OpenVertex( "P" )

    # Get the number of properties before start
    num_start = V.NumProperties()

    # Set float property
    Expect( V.HasProperty( "float1" ) is False,                   "Vertex should not already contain property 'float1'" )
    V.SetProperty( "float1", 3.14 )

    # Get float property
    Expect( V.HasProperty( "float1" ) is True,                    "Vertex should contain property 'float1' after being set" )
    Expect( float_eq( V.GetProperty( "float1" ), 3.14 ),          "V.GetProperty( 'float1' ) should return value 3.14" )
    Expect( V.HasProperty( "float1", 3.14 ) is True,              "V.HasProperty( 'float1', 3.14 ) should be True" )
    Expect( V.HasProperty( "float1", 3.15 ) is False,             "V.HasProperty( 'float1', 3.15 ) should be False" )
    Expect( V.HasProperties() is True,                            "Vertex should contain properties" )
    Expect( float_eq( V.GetProperties().get("float1"), 3.14 ),    "V.GetProperties() should return a dict containing 'float1':3.14" )

    # Overwrite float property
    V.SetProperty( "float1", 3.5 )
    Expect( V.GetProperty( "float1" ) == 3.5,                     "V.GetProperty( 'float1' ) should return value 3.5" )

    # Increment existing float property
    Expect( float_eq( V.IncProperty( "float1" ), 4.5 ),           "V.IncProperty( 'float1' ) should return 4.5" )
    Expect( float_eq( V.GetProperty( "float1" ), 4.5 ),           "V.GetProperty( 'float1' ) should return 4.5 after increment" )
    Expect( float_eq( V.IncProperty( "float1", 5.4 ), 9.9 ),      "V.IncProperty( 'float1', 5.4 ) should return 9.9" )
    Expect( float_eq( V.GetProperty( "float1" ), 9.9 ),           "V.GetProperty( 'float1' ) should return 9.9 after delta increment" )
    Expect( float_eq( V.IncProperty( "float1", -10 ), -0.1 ) ,    "V.IncProperty( 'float1', -10 ) should return -0.1" )
    Expect( float_eq( V.GetProperty( "float1" ), -0.1 ),          "V.GetProperty( 'float1' ) should return -0.1 after delta increment by negative value" )

    # Remove float property
    V.RemoveProperty( "float1" )
    Expect( V.HasProperty( "float1" ) is False,                   "Vertex should not contain property 'float1' after removal" )

    # Increment without pre-existing
    Expect( float_eq( V.IncProperty( "val" ), 1.0 ),              "V.IncProperty( 'val' ) should return 1.0" )
    Expect( float_eq( V.GetProperty( "val" ), 1.0 ),              "V.GetProperty( 'val' ) should return 1.0 after increment from non-exist" )

    # Remove the property
    V.RemoveProperty( "val" )

    # Get the number of properties when done (should be the same as what we started with)
    num_end = V.NumProperties()
    Expect( num_start == num_end,                                 "Number of properties should be the same as what we started with" )

    # Close vertex
    graph.CloseVertex( V )



def TEST_StringProperties():
    """
    Verify that we can set, get, and remove string properties
    """
    # Get the node we will use for testing properties
    V = graph.OpenVertex( "P" )

    # Get the number of properties before start
    num_start = V.NumProperties()

    # Set string property
    Expect( V.HasProperty( "str1" ) is False,                     "Vertex should not already contain property 'str1'" )
    V.SetProperty( "str1", "Hello!" )

    # Get string property
    Expect( V.HasProperty( "str1" ) is True,                      "Vertex should contain property 'str1' after being set" )
    Expect( V.GetProperty( "str1" ) == "Hello!",                  "V.GetProperty( 'str1' ) should return value 'Hello!'" )
    Expect( V.HasProperty( "str1", "Hello!" ) is True,            "V.HasProperty( 'str1', 'Hello!' ) should be True" )
    Expect( V.HasProperty( "str1", "Hello" ) is False,            "V.HasProperty( 'str1', 'Hello' ) should be False" )
    Expect( V.HasProperties() is True,                            "Vertex should contain properties" )
    Expect( V.GetProperties().get("str1") == "Hello!",            "V.GetProperties() should return a dict containing 'str1':'Hello!'" )

    # Overwrite string property
    V.SetProperty( "str1", "Hi again" )
    Expect( V.GetProperty( "str1" ) == "Hi again",                "V.GetProperty( 'str1' ) should return value 'Hi again'" )

    # Remove string property
    V.RemoveProperty( "str1" )
    Expect( V.HasProperty( "str1" ) is False,                     "Vertex should not contain property 'str1' after removal" )

    # Get the number of properties when done (should be the same as what we started with)
    num_end = V.NumProperties()
    Expect( num_start == num_end,                                 "Number of properties should be the same as what we started with" )

    # Close vertex
    graph.CloseVertex( V )



def TEST_PropertyDictSyntax():
    """
    Verify that vertices can use dict syntax for property set/get
    """
    # Get the node we will use for testing properties
    V = graph.OpenVertex( "P" )

    # Get the number of properties before start
    num_start = V.NumProperties()

    # Set using subscript syntax
    V['x'] = 10
    V['y'] = 33.33
    V['z'] = "This is a string"

    # Get using subscript syntax
    Expect( V['x'] == 10,                             "V['x'] should be 10" )
    Expect( float_eq( V['y'], 33.33 ),                "V['y'] should be 33.33" )
    Expect( V['z'] == "This is a string",             "V['z'] should be 'This is a string'" )

    # Test items()
    Expect( type( list(V.items()) ) is list,                "V.items() should return a list" )
    Expect( len( list(V.items()) ) == 3,                    "V.items() should contain 3 items" )
    D = dict( list(V.items()) )
    Expect( D['x'] == 10,                             "V.items() should contain ('x', 10)" )
    Expect( float_eq( D['y'], 33.33 ),                "V.items() should contain ('y', 33.33)" )
    Expect( D['z'] == "This is a string",             "V.items() should contain ('z', 'This is a string')" )

    # Test keys()
    Expect( type( list(V.keys()) ) is list,                 "V.keys() should return a list" )
    Expect( len( list(V.keys()) ) == 3,                     "V.keys() should contain 3 keys" )
    Expect( "x" in list(V.keys()),                          "V.keys() should contain 'x'" )
    Expect( "y" in list(V.keys()),                          "V.keys() should contain 'y'" )
    Expect( "z" in list(V.keys()),                          "V.keys() should contain 'z'" )

    # Test values()
    Expect( type( list(V.values()) ) is list,               "V.values() should return a list" )
    Expect( len( list(V.values()) ) == 3,                   "V.values() should contain 3 values" )
    Expect( 10 in list(V.values()),                         "V.values() should contain 10" )
    Expect( "This is a string" in list(V.values()),         "V.vakues() should contain 'This is a string'" )

    # Remove using subscript syntax
    del V['x']
    del V['y']
    del V['z']

    # Get the number of properties when done (should be the same as what we started with)
    num_end = V.NumProperties()
    Expect( num_start == num_end,                     "Number of properties should be the same as what we started with" )

    # Close vertex
    graph.CloseVertex( V )







def TEST_VertexDegrees():
    """
    Verify that vertices that are connected to other vertices have the correct degree
    """
    # Get the nodes we will use for testing
    create_and_check_new_vertex( "Node1", "node" )
    create_and_check_new_vertex( "Node2", "node" )
    create_and_check_new_vertex( "Node3", "node" )
    create_and_check_new_vertex( "Node4", "node" )
    create_and_check_new_vertex( "Node5", "node" )
    V1 = graph.OpenVertex( "Node1" )
    V2 = graph.OpenVertex( "Node2" )
    V3 = graph.OpenVertex( "Node3" )
    V4 = graph.OpenVertex( "Node4" )
    V5 = graph.OpenVertex( "Node5" )

    # Make some connections
    graph.Connect( V1, "to", V2 )
    graph.Connect( V1, "to", V3 )
    graph.Connect( V1, "to", V4 )
    graph.Connect( V1, "to", V5 )
    # Test
    Expect( V1.indegree == 0,                       "V1.indegree == 0" )
    Expect( V1.outdegree == 4,                      "V1.outdegree == 4" )
    Expect( V1.degree == 4,                         "V1.degree == 4" )
    Expect( V2.indegree == 1,                       "V2.indegree == 1" )
    Expect( V2.outdegree == 0,                      "V2.outdegree == 0" )
    Expect( V2.degree == 1,                         "V2.degree == 1" )
    Expect( V3.indegree == 1,                       "V3.indegree == 1" )
    Expect( V3.outdegree == 0,                      "V3.outdegree == 0" )
    Expect( V3.degree == 1,                         "V3.degree == 1" )
    Expect( V4.indegree == 1,                       "V4.indegree == 1" )
    Expect( V4.outdegree == 0,                      "V4.outdegree == 0" )
    Expect( V4.degree == 1,                         "V4.degree == 1" )
    Expect( V5.indegree == 1,                       "V5.indegree == 1" )
    Expect( V5.outdegree == 0,                      "V5.outdegree == 0" )
    Expect( V5.degree == 1,                         "V5.degree == 1" )

    # Make more connections
    graph.Connect( V2, "to", V3 )
    graph.Connect( V2, "to", V4 )
    graph.Connect( V2, "to", V5 )
    graph.Connect( V3, "to", V4 )
    graph.Connect( V3, "to", V5 )
    graph.Connect( V4, "to", V5 )
    # Test
    Expect( V1.indegree == 0,                       "V1.indegree == 0" )
    Expect( V1.outdegree == 4,                      "V1.outdegree == 4" )
    Expect( V1.degree == 4,                         "V1.degree == 4" )
    Expect( V2.indegree == 1,                       "V2.indegree == 1" )
    Expect( V2.outdegree == 3,                      "V2.outdegree == 3" )
    Expect( V2.degree == 4,                         "V2.degree == 4" )
    Expect( V3.indegree == 2,                       "V3.indegree == 2" )
    Expect( V3.outdegree == 2,                      "V3.outdegree == 2" )
    Expect( V3.degree == 4,                         "V3.degree == 4" )
    Expect( V4.indegree == 3,                       "V4.indegree == 3" )
    Expect( V4.outdegree == 1,                      "V4.outdegree == 1" )
    Expect( V4.degree == 4,                         "V4.degree == 4" )
    Expect( V5.indegree == 4,                       "V5.indegree == 4" )
    Expect( V5.outdegree == 0,                      "V5.outdegree == 0" )
    Expect( V5.degree == 4,                         "V5.degree == 4" )

    # Close vertices
    graph.CloseVertex( V1 )
    graph.CloseVertex( V2 )
    graph.CloseVertex( V3 )
    graph.CloseVertex( V4 )
    graph.CloseVertex( V5 )



def TEST_ManyProperties():
    """
    Test a lot of properties in a single vertex
    """
    # Start with no properties
    V = graph.OpenVertex( "P" )
    V.RemoveProperties()
    Expect( V.NumProperties() == 0,                   "Vertex should have no properties" )

    # Set many properties
    N = 100000
    for n in range( N ):
        name = "property_%d" % n
        value = n ** 2
        V[name] = value

    # Test values
    Expect( V.NumProperties() == N,                   "Vertex should have %d properties" % N )
    for n in range( N ):
        name = "property_%d" % n
        value = n ** 2
        Expect( V.HasProperty( name, value ),           "V.HasProperty( %s, %d ) should be True" % (name, value) )

    # Remove all properties
    Expect( V.RemoveProperties() == N,                "V.RemoveProperties() should return %d" % N )

    # Close vertex
    graph.CloseVertex( V )



def TEST_AllStringPropertyValueLengths():
    """
    Test large strings as property values
    """
    # Start with no properties
    V = graph.OpenVertex( "P" )
    V.RemoveProperties()
    Expect( V.NumProperties() == 0,                   "Vertex should have no properties" )

    for n in range( (2**16-1) - 64):
        value = "x" * n
        V.SetProperty( "large", value )
        Expect( V.HasProperty( "large", value ),        "Vertex string value should match what we just set" )

    # Close vertex
    graph.CloseVertex( V )



def TEST_PropertyValueMatch():
    """
    Test matching logic for HasProperty()
    """
    # Start with no properties
    V = graph.OpenVertex( "P" )
    V.RemoveProperties()
    Expect( V.NumProperties() == 0,                   "Vertex should have no properties" )

    # Set and test integer property
    V['x'] = 10
    Expect( V.HasProperty( 'x', 10 ),                 "x == 10" )
    Expect( V.HasProperty( 'x', (V_EQ,  10)),         "x == 10" )
    Expect( V.HasProperty( 'x', (V_NEQ, 7)),          "x != 7" )
    Expect( V.HasProperty( 'x', (V_GTE, 10)),         "x >= 10" )
    Expect( V.HasProperty( 'x', (V_GT,   9)),         "x > 9" )
    Expect( V.HasProperty( 'x', (V_LT,  11)),         "x < 11" )
    Expect( V.HasProperty( 'x', (V_LTE, 10)),         "x <= 10" )
    Expect( not V.HasProperty( 'x', 11),              "NOT x == 11" )
    Expect( not V.HasProperty( 'x', (V_EQ,  11)),     "NOT x == 11" )
    Expect( not V.HasProperty( 'x', (V_NEQ, 10)),     "NOT x != 10" )
    Expect( not V.HasProperty( 'x', (V_GTE, 11)),     "NOT x >= 11" )
    Expect( not V.HasProperty( 'x', (V_GT,  10)),     "NOT x > 10" )
    Expect( not V.HasProperty( 'x', (V_LT,  10)),     "NOT x < 10" )
    Expect( not V.HasProperty( 'x', (V_LTE, 9)),      "NOT x <= 9" )

    # Set and test float property
    V['pi'] = math.pi
    Expect( V.HasProperty( 'pi', math.pi ),             "pi == 3.14159..." )
    Expect( V.HasProperty( 'pi', (V_EQ,  math.pi)),     "pi == 3.14159..." )
    Expect( V.HasProperty( 'pi', (V_NEQ, 3.14)),        "pi != 3.14" )
    Expect( V.HasProperty( 'pi', (V_GTE, math.pi)),     "pi >= 3.14159..." )
    Expect( V.HasProperty( 'pi', (V_GT,  3.14)),        "pi > 3.14" )
    Expect( V.HasProperty( 'pi', (V_LT,  3.15)),        "pi < 3.15" )
    Expect( V.HasProperty( 'pi', (V_LTE, math.pi)),     "pi <= 3.14159..." )
    Expect( not V.HasProperty( 'pi', 3.15 ),            "NOT pi == 3.15" )
    Expect( not V.HasProperty( 'pi', (V_EQ,  3.15)),    "NOT pi == 3.15" )
    Expect( not V.HasProperty( 'pi', (V_NEQ, math.pi)), "NOT pi != 3.14159..." )
    Expect( not V.HasProperty( 'pi', (V_GTE, 3.15)),    "NOT pi >= 3.15" )
    Expect( not V.HasProperty( 'pi', (V_GT,  math.pi)), "NOT pi > 3.14159..." )
    Expect( not V.HasProperty( 'pi', (V_LT,  math.pi)), "NOT pi < 3.14159..." )
    Expect( not V.HasProperty( 'pi', (V_LTE, 3.14)),    "NOT pi <= 3.14" )

    # Set and test string property
    text = "Once upon a time..."
    V['str'] = text
    Expect( V.HasProperty( "str", text ),                     "str == text" )
    Expect( V.HasProperty( "str", (V_EQ, text) ),             "str == text" )
    Expect( V.HasProperty( "str", "O*" ),                     "PREFIX str == 'O*'" )
    Expect( V.HasProperty( "str", "Once upon*" ),             "PREFIX str == 'Once upon*'" )
    Expect( V.HasProperty( "str", (V_NEQ, "Hello") ),         "str != 'Hello'" )
    Expect( V.HasProperty( "str", (V_NEQ, "Once upon") ),     "str != 'Once upon'" )
    Expect( not V.HasProperty( "str", "Hello" ),              "NOT str == 'Hello'" )
    Expect( not V.HasProperty( "str", (V_EQ, "Hello") ),      "NOT str == 'Hello'" )
    Expect( not V.HasProperty( "str", "A*" ),                 "NOT PREFIX str == 'A*'" )
    Expect( not V.HasProperty( "str", "upon*" ),              "NOT PREFIX str == 'upon*'" )
    Expect( not V.HasProperty( "str", (V_NEQ, text) ),        "NOT str != text" )

    # Close vertex
    graph.CloseVertex( V )




def Run():
    """
    Run the tests in this module
    """
    global graph
    if not pyvgx.initialized():
        pyvgx.initialize( "vxroot/test_pyvgx_Vertex" )
    graph = pyvgx.Graph( "TEST_pyvgx_Vertex" )
    graph.Truncate()
    graph.Save()

    # Run all tests in this module
    if RunTests( [__name__] ) is False:
        raise TestFailed( "TEST FAILED" )







if __name__ == "__main__":
    """
    Standalone test of pyvgx.Vertex
    """
    Run()
