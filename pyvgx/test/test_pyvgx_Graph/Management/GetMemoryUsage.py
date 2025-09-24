from pytest.pytest import RunTests, Expect, TestFailed
from pyvgx import *
import pyvgx
import random

graph = None



def TEST_GetMemoryUsage():
    """
    pyvgx.Graph.GetMemoryUsage()
    test_level=3101
    """
    g = Graph( "memusage" )

    info = g.GetMemoryUsage()

    Expect( type(info) is dict,              "Memory info should be dict"  )
    
    Expect( 'vgx.vertex.object' in info )
    Expect( 'vgx.vertex.arcvector' in info )
    Expect( 'vgx.vertex.property' in info )
    Expect( 'vgx.string' in info )
    Expect( 'vgx.index.global' in info )
    Expect( 'vgx.index.type' in info )
    Expect( 'vgx.codec.vertextype' in info )
    Expect( 'vgx.codec.relationship' in info )
    Expect( 'vgx.codec.vertexprop' in info )
    Expect( 'vgx.codec.dimension' in info )
    Expect( 'vgx.vector.internal' in info )
    Expect( 'vgx.vector.external' in info )
    Expect( 'vgx.vector.dimension' in info )
    Expect( 'vgx.event.schedule' in info )
    Expect( 'vgx.ephemeral.string' in info )
    Expect( 'vgx.ephemeral.vector' in info )
    Expect( 'vgx.ephemeral.vertexmap' in info )
    Expect( 'vgx.runtime' in info )
    Expect( 'system.process' in info )
    Expect( 'system.available' in info )
    Expect( 'system.global' in info )

    baseline_vertex = info['vgx.vertex.object']
    baseline_arcvector = info['vgx.vertex.arcvector']
    baseline_property = info['vgx.vertex.property']
    baseline_string = info['vgx.string']
    baseline_index = info['vgx.index.global']
    baseline_typeindex = info['vgx.index.type']
    baseline_codec_vxtype = info['vgx.codec.vertextype']
    baseline_codec_rel = info['vgx.codec.relationship']
    baseline_codec_vxprop = info['vgx.codec.vertexprop']
    baseline_codec_dim = info['vgx.codec.dimension']
    baseline_vector_int = info['vgx.vector.internal']
    baseline_vector_ext = info['vgx.vector.external']
    baseline_vector_dim = info['vgx.vector.dimension']
    baseline_event = info['vgx.event.schedule']
    baseline_eph_string = info['vgx.ephemeral.string']
    baseline_eph_vector = info['vgx.ephemeral.vector']
    baseline_eph_vxmap = info['vgx.ephemeral.vertexmap']
    baseline_runtime = info['vgx.runtime']
    baseline_process = info['system.process']
    baseline_available = info['system.available']
    baseline_global = info['system.global']

    Expect( type(baseline_vertex) is int )
    Expect( type(baseline_arcvector) is int )
    Expect( type(baseline_property) is int )
    Expect( type(baseline_string) is int )
    Expect( type(baseline_index) is int )
    Expect( type(baseline_typeindex) is int )
    Expect( type(baseline_codec_vxtype) is int )
    Expect( type(baseline_codec_rel) is int )
    Expect( type(baseline_codec_vxprop) is int )
    Expect( type(baseline_codec_dim) is int )
    Expect( type(baseline_vector_int) is int )
    Expect( type(baseline_vector_ext) is int )
    Expect( type(baseline_vector_dim) is int )
    Expect( type(baseline_event) is int )
    Expect( type(baseline_eph_string) is int )
    Expect( type(baseline_eph_vector) is int )
    Expect( type(baseline_eph_vxmap) is int )
    Expect( type(baseline_runtime) is int )
    Expect( type(baseline_process) is int )
    Expect( type(baseline_available) is int )
    Expect( type(baseline_global) is int )

    Expect( g.GetMemoryUsage( 'vgx.vertex.object' ) == baseline_vertex )
    Expect( g.GetMemoryUsage( 'vgx.vertex.arcvector' ) == baseline_arcvector )
    Expect( g.GetMemoryUsage( 'vgx.vertex.property' ) == baseline_property )
    Expect( g.GetMemoryUsage( 'vgx.string' ) == baseline_string )
    Expect( g.GetMemoryUsage( 'vgx.index.global' ) == baseline_index )
    Expect( g.GetMemoryUsage( 'vgx.index.type' ) == baseline_typeindex )
    Expect( g.GetMemoryUsage( 'vgx.codec.vertextype' ) == baseline_codec_vxtype )
    Expect( g.GetMemoryUsage( 'vgx.codec.relationship' ) == baseline_codec_rel )
    Expect( g.GetMemoryUsage( 'vgx.codec.vertexprop' ) == baseline_codec_vxprop )
    Expect( g.GetMemoryUsage( 'vgx.codec.dimension' ) == baseline_codec_dim )
    Expect( g.GetMemoryUsage( 'vgx.vector.internal' ) == baseline_vector_int )
    Expect( g.GetMemoryUsage( 'vgx.vector.external' ) == baseline_vector_ext )
    Expect( g.GetMemoryUsage( 'vgx.vector.dimension' ) == baseline_vector_dim )
    Expect( g.GetMemoryUsage( 'vgx.event.schedule' ) == baseline_event )
    Expect( g.GetMemoryUsage( 'vgx.ephemeral.string' ) == baseline_eph_string )
    Expect( g.GetMemoryUsage( 'vgx.ephemeral.vector' ) == baseline_eph_vector )
    Expect( g.GetMemoryUsage( 'vgx.ephemeral.vertexmap' ) == baseline_eph_vxmap )
    Expect( g.GetMemoryUsage( 'system.global' ) == baseline_global )

    vgx_other_mem = g.GetMemoryUsage( 'vgx.runtime' )
    process_mem = g.GetMemoryUsage( 'system.process' )
    available_mem = g.GetMemoryUsage( 'system.available' )

    Expect( vgx_other_mem < process_mem and vgx_other_mem > 0 )
    Expect( process_mem < baseline_global and process_mem > 0 )
    Expect( available_mem < baseline_global and available_mem > 0 )

    for key in [ "", "v", "s", "vgx.", "system.", "vgx.vertex.something", "system.something", "nothing", ".", None, 123 ]:
        try:
            g.GetMemoryUsage( key )
            Expect( False, "key '%s' should be unknown" % key )
        except:
            pass


    N = 100000

    # Add vertices
    for n in range( N ):
        g.CreateVertex( str(n) )

    info = g.GetMemoryUsage()
    Expect( info['vgx.vertex.object'] > baseline_vertex,            "vertex allocator should increase" )
    Expect( info['vgx.vertex.arcvector'] == baseline_arcvector,     "arcvector allocator should remain" )
    baseline_vertex = info['vgx.vertex.object'] # update

    # Add arcvectors
    for n in range( N-3 ):
        a = n+1
        b = n+2
        c = n+3
        g.Connect( str(n), "r1", str(a) )
        g.Connect( str(n), "r2", str(b) )
        g.Connect( str(n), "r3", str(c) )

    info = g.GetMemoryUsage()
    Expect( info['vgx.vertex.object'] == baseline_vertex,           "vertex allocator should remain" )
    Expect( info['vgx.vertex.arcvector'] > baseline_arcvector,      "arcvector allocator should increase" )
    baseline_arcvector = info['vgx.vertex.arcvector'] # update

    # Add properties
    for n in range( N ):
        V = g.NewVertex( str(n) )
        V['x'] = n

    info = g.GetMemoryUsage()
    Expect( info['vgx.vertex.object'] == baseline_vertex,           "vertex allocator should remain" )
    Expect( info['vgx.vertex.arcvector'] == baseline_arcvector,     "arcvector allocator should remain" )
    Expect( info['vgx.vertex.property'] > baseline_property,        "property allocator should increase" )
    baseline_prooperty = info['vgx.vertex.property'] # update


    Expect( info['system.process'] > baseline_process,              "process memory should increase" )
    Expect( info['system.global'] == baseline_global,               "system memory is a constant" )

    del V

    g.Erase()



def Run( name ):
    global graph
    graph = pyvgx.Graph( name )
    RunTests( [__name__] )
    graph.Close()
    del graph

