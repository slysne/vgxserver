###############################################################################
# 
# VGX Server
# Distributed engine for plugin-based graph and vector search
# 
# Module:  pyvgx
# File:    _vertex_test_support.py
# Author:  Stian Lysne <...>
# 
# Copyright © 2025 Rakuten, Inc.
# 
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# 
#     http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
# 
###############################################################################

from pytest.pytest import Expect
from pyvgx import *
import pyvgx
import time
import hashlib
import re
from pytest.threads import Worker







###############################################################################
# _VerifyVertex
#
###############################################################################
def _VerifyVertex( graph, V, name, typename=None, man="REAL", sus="ACTIVE", mode="w" ):
    """
    _VerifyVertex
    """
    TICK = graph.ts

    # Determine the expected internalid
    if len(name) == 32 and re.match( "[a-z0-9]{32}", name ):
        # use smart id
        internalid = name
    else:
        internalid = pyvgx.strhash128( name )

    # Make sure vertex evaluates to True
    Expect( V,                              "Vertex should evaluate to True" )

    # Verify vertex access
    if mode in ['w', 'a']:
        Expect( V.Writable(),               "Vertex should be acquired writable" )
        Expect( not V.Readonly(),           "Vertex should not be readonly" )
    elif mode == 'r':
        Expect( not V.Writable(),           "Vertex should not be acquired writable" )
        Expect( V.Readonly(),               "Vertex should be readonly" )
    Expect( V.Readable(),                   "Vertex should be readable" )

    # Verify vertex attributes
    Expect( V is not None,                  "Should be a vertex, not None" )
    Expect( V.identifier == name,           "V.identifier should be '%s', got '%s'" % (name, V.identifier) )
    Expect( V.id == V.identifier,           "V.id should equal V.identifier, got '%s'" % V.id )
    Expect( V.internalid == internalid,     "V.internalid should be '%s', got '%s'" % (internalid, V.internalid) )

    if man == "REAL":
        Expect( V.outdegree == 0,           "V.outdegree should be 0, got '%d'" % V.outdegree )
        Expect( V.indegree == 0,            "V.indegree should be 0, got '%d'" % V.indegree )
        Expect( V.degree == 0,              "V.degree should be 0, got '%d'" % V.degree )
    elif man == "VIRTUAL":
        Expect( V.outdegree == 0,           "V.outdegree should be 0, got '%d'" % V.outdegree )

    Expect( type(V.descriptor) is int,     "V.descriptor should be a long, got '%s'" % type(V.descriptor) )

    if typename is None:
        Expect( V.type == "__vertex__",     "V.type should be '__vertex__', got '%s'" % V.type )
    else:
        Expect( V.type == typename,         "V.type should be '%s', got '%s'" % (typename, V.type) )

    Expect( V.manifestation == man,         "V.manifestation should be '%s', got '%s'" % (man, V.manifestation) )
    Expect( V.man == V.manifestation,       "V.man should equal V.manifestation, got '%s'" % V.man )
    Expect( type( V.vector ) is list,       "V.vector should be list, got '%s'" % V.vector )
    Expect( len( V.vector ) == 0,           "V.vector should be empty, got '%s'" % V.vector )
    Expect( type( V.properties ) is dict,   "V.properties should be dict, got '%s'" % V.properties )
    Expect( len( V.properties ) == 0,       "V.properties should be empty, got '%s'" % V.properties )
    Expect( type( V.TMC ) is int,          "V.TMC should be long, got '%s'" % type( V.TMC ) )
    tmc = V.TMC
    Expect( tmc > 0,                        "V.TMC should be greater than zero" )
    Expect( tmc < T_NEVER,                  "V.TMC should be greater than zero" )
    Expect( abs(tmc-TICK) < 2,              "V.TMC should match graph current time (V.TMC=%d  TICK=%f)" % (tmc, TICK) )
    Expect( V.TMM in [tmc, tmc+1],          "V.TMM should equal V.TMC=%d(+1), got '%d'" % (tmc, V.TMM) )
    Expect( V.TMX > tmc,                    "V.TMX should be in the future, got '%d'" % V.TMX )

    # Verify vertex properties empty
    Expect( V.NumProperties() == 0,         "V.NumProperties() should be 0, got '%d'" % V.NumProperties() )
    Expect( V.HasProperties() is False,     "V should not have properties" )
    p = V.GetProperties()
    Expect( type( p ) is dict,              "V.GetProperties() should return dict, got '%s'" % p )
    Expect( len( p ) == 0,                  "V.GetProperties() should be empty, got '%s'" % p )
    n = V.RemoveProperties()
    Expect( n == 0,                         "V.RemoveProperties() should return 0, got '%d'" % n )
    Expect( type( list(V.items()) ) is list,      "V.items() should be list, got '%s'" % list(V.items()) )
    Expect( len( list(V.items()) ) == 0,          "V.items() should be empty, got '%s'" % list(V.items()) )
    Expect( type( list(V.keys()) ) is list,       "V.keys() should be list, got '%s'" % list(V.keys()) )
    Expect( len( list(V.keys()) ) == 0,           "V.keys() should be empty, got '%s'" % list(V.keys()) )
    Expect( type( list(V.values()) ) is list,     "V.values() should be list, got '%s'" % list(V.values()) )
    Expect( len( list(V.values()) ) == 0,         "V.values() should be empty, got '%s'" % list(V.values()) )

    # Verify vertex vector is empty
    v = V.GetVector()
    Expect( type( v ) is pyvgx.Vector,      "V.GetVector() should return a pyvgx.Vector instance, got '%s'" % type( v ) )
    Expect( len( v ) == 0,                  "V.GetVector() should return NULL-vector, got length=%d" % len( v ) )
    Expect( V.RemoveVector() is None,       "V.RemoveVector() should work and return None" )

    # Verify vertex descriptor
    desc = V.Descriptor()
    dpat = r"\[1\] \[%s\] \[\[%s %s\] \[IDLE NORMAL - WRITABLE LOCKED\]\] \[\[- -\] \[- -\] \[GLOBAL\] \[0\]\] \[\d+\]" % (V.type, man, sus)
    Expect( re.match( dpat, desc ),         "V.Descriptor() should match: %s, got '%s'" % (dpat, desc) )

    # Verify vertex as dict
    D = V.AsDict()
    Expect( type( D ) is dict,                      "V.AsDict() should return dict, got '%s'" % D )
    Expect( D.get('id') == name,                    "V.AsDict()['id'] should return V.id, got '%s'" % D.get('id') )
    Expect( D.get('internalid') == internalid,      "V.AsDict()['internalid'] should return V.internalid, got '%s'" % D.get('internalid') )
    Expect( D.get('odeg') == V.outdegree,           "V.AsDict()['odeg'] should return V.odeg, got '%s'" % D.get('odeg') )
    Expect( D.get('odeg') == V.odeg,                "V.AsDict()['odeg'] should return V.odeg, got '%s'" % D.get('odeg') )
    Expect( D.get('ideg') == V.indegree,            "V.AsDict()['ideg'] should return V.ideg, got '%s'" % D.get('ideg') )
    Expect( D.get('ideg') == V.ideg,                "V.AsDict()['ideg'] should return V.ideg, got '%s'" % D.get('ideg') )
    Expect( D.get('deg') == V.degree,               "V.AsDict()['degree'] should return V.deg, got '%s'" % D.get('deg') )
    Expect( D.get('deg') == V.deg,                  "V.AsDict()['deg'] should return V.deg, got '%s'" % D.get('deg') )
    Expect( D.get('descriptor') == V.descriptor,    "V.AsDict()['descriptor'] should return V.descriptor, got '%s'" % D.get('descrioptor') )
    Expect( D.get('type') == V.type,                "V.AsDict()['type'] should return V.type, got '%s'" % D.get('type') )
    Expect( D.get('man') == V.man,                  "V.AsDict()['man'] should return V.man, got '%s'" % D.get('man') )
    Expect( D.get('vector') == V.vector,            "V.AsDict()['vector'] should return V.vector, got '%s'" % D.get('vector') )
    Expect( D.get('properties') == V.properties,    "V.AsDict()['properties'] should return V.properties, got '%s'" % D.get('properties') )
    Expect( D.get('tmc') == tmc,                    "V.AsDict()['tmc'] should return V.tmc, got '%s'" % D.get('tmc') )
    Expect( D.get('tmm') == V.TMM,                  "V.AsDict()['tmm'] should return V.tmm, got '%s'" % D.get('tmm') )
    Expect( D.get('tmx') == V.TMX,                  "V.AsDict()['tmx'] should return V.tmx, got '%s'" % D.get('tmx') )
    Expect( D.get('rank').get('c1') == V.c1,        "V.AsDict()['rank']['c1'] should return V.c1, got '%s'" % D.get('rank').get('c1') )
    Expect( D.get('rank').get('c0') == V.c0,        "V.AsDict()['rank']['c0'] should return V.c0, got '%s'" % D.get('rank').get('c0') )
    Expect( D.get('virtual') == V.virtual,          "V.AsDict()['virtual'] should return V.virtual, got '%s'" % D.get('virtual') )
    Expect( D.get('address') == V.address,          "V.AsDict()['address'] should return V.address, got '%s'" % D.get('address') )
    Expect( D.get('index') == V.index,              "V.AsDict()['index'] should return V.index, got '%s'" % D.get('index') )
    Expect( D.get('bitindex') == V.bitindex,        "V.AsDict()['bitindex'] should return V.bitindex, got '%s'" % D.get('bitindex') )
    Expect( D.get('bitvector') == V.bitvector,      "V.AsDict()['bitvector'] should return V.bitvector, got '%s'" % D.get('bitvector') )
    Expect( D.get('op') == V.op,                    "V.AsDict()['op'] should return V.op, got '%s'" % D.get('op') )

    # Verify presence in graph
    Expect( graph.HasVertex( name ),        "Vertex should exist in graph" )
    Expect( graph.HasVertex( internalid ),  "Vertex should exist in graph" )




###############################################################################
# _VerifyNewVertex
#
###############################################################################
def _VerifyNewVertex( graph, V, name, typename=None ):
    """
    _VerifyNewVertex
    """
    _VerifyVertex( graph, V, name, typename=typename )




###############################################################################
# _NewVertex_or_CreateVertex
#
###############################################################################
def _NewVertex_or_CreateVertex( graph, method_name ):
    """
    pyvgx.Graph.NewVertex()
    OR
    pyvgx.Graph.CreateVertex()
    """
    if method_name == "NewVertex":
        mode = 1
        name_A = "A_New"
        name_B = "B_New"
        name_C = "C_New"
    elif method_name == "CreateVertex":
        mode = 2
        name_A = "A_Create"
        name_B = "B_Create"
        name_C = "C_Create"
    else:
        raise Exception( "unexpected method name: %s" % method_name )

    # New typeless vertex A
    if mode == 1:
        A = graph.NewVertex( name_A )
    else:
        Expect( graph.CreateVertex( name_A ) == 1,    "Should create a new vertex" )
        A = graph.OpenVertex( name_A )
    _VerifyNewVertex( graph, A, name_A )
    graph.CloseVertex( A )
    del A

    # New typed vertex B
    if mode == 1:
        B = graph.NewVertex( name_B, type="B_type" )
    else:
        Expect( graph.CreateVertex( name_B, type="B_type" ) == 1,    "Should create a new vertex" )
        B = graph.OpenVertex( name_B )
    _VerifyNewVertex( graph, B, name_B, "B_type" )
    graph.CloseVertex( B )
    del B

    # New typed vertex C with timeout
    if mode == 1:
        C = graph.NewVertex( name_C, type="C_type", timeout=100 )
    else:
        Expect( graph.CreateVertex( name_C, type="C_type" ) == 1,    "Should create a new vertex" )
        C = graph.OpenVertex( name_C )
    _VerifyNewVertex( graph, C, name_C, "C_type" )
    graph.CloseVertex( C )
    del C

    # Re-open typeless vertex A
    if mode == 1:
        A2 = graph.NewVertex( name_A )
    else:
        Expect( graph.CreateVertex( name_A ) == 0,    "Should not create a new vertex" )
        A2 = graph.OpenVertex( name_A )
    _VerifyNewVertex( graph, A2, name_A )
    graph.CloseVertex( A2 )
    del A2

    # Fail to open typeless vertex with typed call
    try:
        if mode == 1:
            graph.NewVertex( name_A, type="A_type" )
        else:
            graph.CreateVertex( name_A, type="A_type" )
        Expect( False,  "Should not be able to open typeless vertex with typed call" )
    except pyvgx.VertexError as ex:
        Expect( str(ex).startswith( "Type mismatch" ), "Should raise exception stating type mismatch" )
    except:
        Expect( False,  "Incorrect exception for type mismatch" )

    # Re-open typed vertex B with untyped call
    if mode == 1:
        B2 = graph.NewVertex( name_B )
    else:
        Expect( graph.CreateVertex( name_B ) == 0,    "Should not create a new vertex" )
        B2 = graph.OpenVertex( name_B )
    _VerifyNewVertex( graph, B2, name_B, "B_type" )
    graph.CloseVertex( B2 )
    del B2

    # Re-open typed vertex B with typed call
    if mode == 1:
        B3 = graph.NewVertex( name_B, type="B_type" )
    else:
        Expect( graph.CreateVertex( name_B ) == 0,    "Should not create a new vertex" )
        B3 = graph.OpenVertex( name_B )
    _VerifyNewVertex( graph, B3, name_B, "B_type" )
    graph.CloseVertex( B3 )
    del B3

    # Fail to open typed vertex with different type in call
    try:
        if mode == 1:
            graph.NewVertex( name_B, type="X_type" )
        else:
            graph.CreateVertex( name_B, type="X_type" )
        Expect( False,  "Should not be able to open typed vertex with different type in call" )
    except pyvgx.VertexError as ex:
        Expect( str(ex).startswith( "Type mismatch" ), "Should raise exception stating type mismatch" )
    except:
        Expect( False,  "Incorrect exception for type mismatch" )

    # Re-open typed vertex C with typed call and timeout
    if mode == 1:
        C2 = graph.NewVertex( name_C, type="C_type", timeout=0 )
    else:
        Expect( graph.CreateVertex( name_C ) == 0,    "Should not create a new vertex" )
        C2 = graph.OpenVertex( name_C )
    _VerifyNewVertex( graph, C2, name_C, "C_type" )
    graph.CloseVertex( C2 )
    del C2

    # Re-open 
    worker = Worker( "w1" )
    VERTICES = {}
    def open_vertex( name ):
        V = graph.OpenVertex( name, "w" )
        VERTICES[ name ] = V

    def close_vertex( name ):
        V = VERTICES.pop( name )
        graph.CloseVertex( V )



    if mode == 1:
        worker.perform( open_vertex, name_C )
        time.sleep( 0.1 )
        try:
            C3 = graph.NewVertex( name_C )
            Expect( False,                              "Should not be able to open locked vertex" )
        except pyvgx.AccessError:
            pass
        except Exception as ex:
            Expect( False,                              "Unexpected exception: %s" % ex )

        try:
            C4 = graph.NewVertex( name_C, timeout=100 )
            Expect( False,                              "Should not be able to open locked vertex with timeout" )
        except pyvgx.AccessError:
            pass
        except Exception as ex:
            Expect( False,                              "Unexpected exception: %s" % ex )

        worker.perform( close_vertex, name_C )
        time.sleep( 0.1 )

        C5 = graph.NewVertex( name_C )
        _VerifyNewVertex( graph, C5, name_C, "C_type" )

        graph.CloseVertex( C5 )
        del C5
