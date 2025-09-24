from pytest.pytest import RunTests, Expect, TestFailed
import time
from pyvgx import *
import pyvgx
import types
import threading

graph = None



def TEST_vxvertex_object():
    """
    Core vxvertex_object
    test_level=501
    """
    try:
        pyvgx.selftest( force=True, testroot="vgxtest", library="vgx", names=["vxvertex_object.c"] )
    except:
        Expect( False )



def TEST_vxdurable_commit():
    """
    Core vxdurable_commit
    test_level=501
    """
    try:
        pyvgx.selftest( force=True, testroot="vgxtest", library="vgx", names=["vxdurable_commit.c"] )
    except:
        Expect( False )



def TEST_Vertex_methods():
    """
    pyvgx.Vertex
    Verify all methods exist
    test_level=3101
    """
    if "vertex" in graph:
        graph.DeleteVertex( "vertex" )

    V = graph.NewVertex( "vertex" )
    
    methods = [
        V.Writable, V.Readable, V.Readonly,
        V.SetRank, V.GetRank,
        V.Neighborhood, V.Adjacent, V.Aggregate, V.ArcValue, V.Degree, V.Inarcs, V.Outarcs, V.Initials, V.Terminals, V.Neighbors,
        V.SetProperty, V.IncProperty, V.HasProperty, V.GetProperty, V.RemoveProperty, V.SetProperties, V.HasProperties, V.NumProperties, V.GetProperties, V.RemoveProperties,
        V.items, V.keys, V.values,
        V.SetVector, V.HasVector, V.GetVector, V.RemoveVector,
        V.SetExpiration, V.GetExpiration, V.IsExpired, V.ClearExpiration,
        V.Commit, V.IsVirtual, V.AsDict, V.Descriptor,
        V.DebugVector, V.Debug
    ]

    for method in methods:
        Expect( type(method) is types.BuiltinMethodType )



def TEST_Vertex_attributes_exist():
    """
    pyvgx.Vertex
    Verify all attributes exist
    test_level=3101
    """
    if "vertex" in graph:
        graph.DeleteVertex( "vertex" )

    V = graph.NewVertex( "vertex" )

    Expect( type( V.id ) is str )
    Expect( type( V.internalid ) is str )
    Expect( type( V.type ) is str )
    Expect( type( V.deg ) is int )
    Expect( type( V.ideg ) is int )
    Expect( type( V.odeg ) is int )
    Expect( type( V.vector ) is list )
    Expect( type( V.properties ) is dict )
    Expect( type( V.tmc ) is int )
    Expect( type( V.tmm ) is int )
    Expect( type( V.tmx ) is int )
    Expect( type( V.c1 ) is float )
    Expect( type( V.c0 ) is float )
    Expect( type( V.virtual ) is bool )
    Expect( type( V.address ) is int )
    Expect( type( V.index ) is int )
    Expect( type( V.bitindex ) is int )
    Expect( type( V.bitvector ) is int )
    Expect( type( V.op ) is int )
    Expect( type( V.refc ) is int )
    Expect( type( V.bidx ) is int )
    Expect( type( V.oidx ) is int )
    Expect( type( V.handle ) is int )
    Expect( type( V.enum ) is int )
    Expect( type( V.descriptor ) is int )
    Expect( type( V.readers ) is int )



def TEST_Vertex_constructor():
    """
    pyvgx.Vertex
    Verify all methods exist
    test_level=3101
    """
    if "vertex" in graph:
        graph.DeleteVertex( "vertex" )

    Expect( "vertex" not in graph )

    try:
        Vertex()
        Expect( False )
    except:
        pass

    try:
        Vertex( graph )
        Expect( False )
    except:
        pass

    # Default mode is 'a' so fail here
    try:
        Vertex( graph, "vertex" )
        Expect( False )
    except:
        Expect( "vertex" not in graph )

    # Default mode is 'a' so fail here
    try:
        Vertex( graph, "vertex", "v_type" )
        Expect( False )
    except:
        Expect( "vertex" not in graph )

    # Create new vertex
    V = Vertex( graph, "vertex", "v_type", "w" )
    Expect( "vertex" in graph )

    # Vertex type not permitted in 'a' mode
    try:
        Vertex( graph, "vertex", "v_type", "a" )
        Expect( False )
    except ValueError:
        pass
    except:
        Expect( False )

    # Open existing writable
    V = Vertex( graph, "vertex", mode="a" )
    Expect( V.Writable() )
    graph.CloseVertex( V )

    # Vertex type not permitted in 'r' mode
    try:
        Vertex( graph, "vertex", "v_type", "r" )
        Expect( False )
    except ValueError:
        pass
    except:
        Expect( False )

    # Open existing readonly
    V = Vertex( graph, "vertex", mode="r" )
    Expect( V.Readonly() )
    graph.CloseVertex( V )



def TEST_Vertex_attibutes_set():
    """
    pyvgx.Vertex
    Verify attributes for a populated vertex
    test_level=3101
    """
    if "vertex" in graph:
        graph.DeleteVertex( "vertex" )

    now = int( graph.ts )
    V = graph.NewVertex( "vertex", "node" )

    graph.Connect( V, "to", "other" )
    V.SetVector( [('a',1)] )
    V.SetProperty( 'x', 1 )
    V.SetProperty( 'y', 3.0 )
    V.SetProperty( 'z', 'string' )
    V.SetExpiration( now + 100 )
    V.c1 = 5.5
    V.c0 = 1000

    Expect( V.id == "vertex" )
    Expect( V.internalid == strhash128( "vertex" ) )
    Expect( V.type == 'node' )
    Expect( V.deg == 1 )
    Expect( V.ideg == 0 )
    Expect( V.odeg == 1 )
    Expect( V.vector == [('a',1.0)] )
    Expect( V.properties == {'x':1, 'y':3.0, 'z':'string'} )
    Expect( V.tmc >= now )
    Expect( V.tmm >= V.TMC )
    Expect( V.tmx == now + 100 )
    Expect( V.c1 == 5.5 )
    Expect( V.c0 == 1000.0 )
    Expect( V.virtual == False )
    Expect( V.address > 0 )
    index = (V.address-32) // 192
    Expect( V.index == index, "V.index={}, got {} for vertex at address {}".format( index, V.index, V.address ) )
    Expect( V.bitindex == V.index >> 6, "" )
    Expect( popcnt( V.bitvector ) == 1, "" )
    Expect( V.op > 0 )
    Expect( V.refc > 0 )
    Expect( V.bidx >= 0 )
    Expect( V.oidx >= 0 )
    Expect( V.handle > V.enum )
    Expect( (V.handle & 0x7FFFFFFF) + 1 == V.enum )
    Expect( V.descriptor > 0 )
    Expect( V.readers == 0 )

    graph.CloseVertex( V )
    V = graph.OpenVertex( "vertex", mode="r" )
    Expect( V.readers == 1 )

    graph.CloseVertex( V )



def Run( name ):
    global graph
    graph = pyvgx.Graph( name )
    RunTests( [__name__] )
    graph.Close()
    del graph
