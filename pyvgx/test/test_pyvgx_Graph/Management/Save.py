###############################################################################
# 
# VGX Server
# Distributed engine for plugin-based graph and vector search
# 
# Module:  pyvgx.test
# File:    Save.py
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

from pyvgxtest.pyvgxtest import RunTests, Expect, TestFailed
from pyvgx import *
import pyvgx
import random

graph = None
SYSROOT = None




###############################################################################
# rstr
#
###############################################################################
def rstr(n):
    """
    """
    return "".join( [ chr(random.randint(97,122)) for _ in range(n) ] )




###############################################################################
# TEST_vxarcvector_serialization
#
###############################################################################
def TEST_vxarcvector_serialization():
    """
    Core vxarcvector_serialization
    test_level=501
    """
    try:
        pyvgx.selftest( force=True, testroot="vgxtest", library="vgx", names=["vxarcvector_serialization.c"] )
    except:
        Expect( False )




###############################################################################
# TEST_vxdurable_serialization
#
###############################################################################
def TEST_vxdurable_serialization():
    """
    Core vxdurable_serialization
    test_level=501
    """
    try:
        pyvgx.selftest( force=True, testroot="vgxtest", library="vgx", names=["vxdurable_serialization.c"] )
    except:
        Expect( False )




###############################################################################
# TEST_Save
#
###############################################################################
def TEST_Save():
    """
    pyvgx.Graph.Save()
    Basic save
    t_nominal=6
    test_level=3101
    """
    if not system.IsInitialized():
        system.Initialize( SYSROOT, euclidean=False )


    g = Graph( "persisted" )
    g.Truncate()

    # Save empty graph
    g.Save( 60000 )

    # Save simple graph with one connection
    g.CreateVertex( "A", type="node" )
    g.Connect( "A", "to", "B" )
    g.Save( 60000 )
    
    # Save graph where vertex has properties
    A = g.OpenVertex( "A" )
    A.SetProperty( "name", "A" )
    A.SetProperty( "number", 1 )
    A.SetProperty( "value", 3.0 )
    g.CloseVertex( A )
    g.Save( 60000 )

    # Save graph where vertex has virtual (disk) properties
    A = g.OpenVertex( "A" )
    DISK_X = "This is a small string"
    DISK_Y = "This is a big random string: {}".format( rstr(10000) )
    DISK_Z = "This is a huge random string: {}".format( rstr(1000000) )
    A.SetProperty( "disk_x", DISK_X, True )
    A.SetProperty( "*disk_y", DISK_Y, True )
    A.SetProperty( "*disk_z", DISK_Z )
    g.CloseVertex( A )
    g.Save( 60000 )

    # Check
    Expect( g.order == 2 and g.size == 1,           "two connected vertices"  )

    # Empty graph
    g.Truncate()
    Expect( g.order == 0 and g.size == 0,           "empty graph" )
    g.Close()
    del g

    # Unload and restore
    system.Unload()
    system.Initialize( SYSROOT, attach=None, euclidean=False )

    g = Graph( "persisted" )

    # Verify
    Expect( g.order == 2 and g.size == 1,           "two connected vertices" )
    Expect( "A" in g and "B" in g,                  "both vertices restored" )
    Expect( g.Adjacent( "A", "to", "B" ),           "connection restored" )
    Expect( g["A"].type == "node",                  "A type restored" )
    Expect( g["B"].type == "__vertex__",            "B type restored" )
    Expect( g["A"].IsVirtual() == False,            "A restored as real" )
    Expect( g["B"].IsVirtual() == True,             "B restored as virtual" )
    Expect( g["A"]["name"] == "A",                  "string property restored" )
    Expect( g["A"]["number"] == 1,                  "integer property restored" )
    Expect( g["A"]["value"] == 3.0,                 "float property restored" )
    Expect( g["A"]["disk_x"] == DISK_X,             "disk_x property restored" )
    Expect( g["A"]["disk_y"] == DISK_Y,             "disk_y property restored" )
    Expect( g["A"]["disk_z"] == DISK_Z,             "disk_z property restored" )

    g.Erase()
    system.Unload()




###############################################################################
# TEST_PersistedReadonly
#
###############################################################################
def TEST_PersistedReadonly():
    """
    Test Persisted Readonly
    t_nominal=5
    test_level=3101
    """
    # Create a new graph with one vertex, set readonly and then save and unload

    if not system.IsInitialized():
        system.Initialize( SYSROOT, euclidean=False )

    g = Graph( "persisted_readonly" )
    Expect( g.IsGraphReadonly() is False,           "Graph is not readonly" )

    g.CreateVertex( "X" )
    g.SetGraphReadonly( 60000 )

    g.Save( 60000 )
    g.Close()
    del g

    system.Unload()

    Expect( system.IsInitialized() == False,         "system should be reset after unload" )

    # Initialize and load graph
    system.Initialize( SYSROOT, attach=None, euclidean=False )

    g = Graph( "persisted_readonly" )
    Expect( g.IsGraphReadonly() is True,    "Graph is readonly" )

    Expect( g.EventBacklog().split()[-1] == "<PAUSED>",    "Event processor should be paused" )

    try:
        g.CreateVertex( "Y" )
        Expect( False, "should not be able to create vertex for readonly graph" )
    except:
        pass

    Expect( "X" in g,                       "X should exist" )
    Expect( "Y" not in g,                   "X should not exist" )


    g.ClearGraphReadonly()
    Expect( g.IsGraphReadonly() is False,   "Graph is not readonly" )
    
    Expect( g.EventBacklog().split()[-1] == "<RUNNING>",    "Event processor should be running" )

    g.CreateVertex( "Y" )

    Expect( "Y" in g,                       "Y should exist" )

    g.Erase()
    system.Unload()




###############################################################################
# TEST_Save_large_data
#
###############################################################################
def TEST_Save_large_data():
    """
    pyvgx.Graph.Save()
    Save and restore large data
    t_nominal=127
    test_level=3102
    """
    if not system.IsInitialized():
        system.Initialize( SYSROOT, euclidean=False )

    g=Graph( "large_persist" )
    g.Truncate()

    SEED = 1000

    I = 600000
    N = 1200000
    T = 10
    C = 10
    R = 5
    Vmin = -1000000000
    Vmax = 1000000000

    MODS = [ M_INT, M_FLT, M_INT|M_FWDONLY ]

    name_prop_fmt = "Node name: {}"
    virtual_prop_fmt_small = "This is prop for: {}"
    virtual_prop_fmt_large = "This is {} prop for: {{}}".format( rstr(2000) )

    # Populate
    random.seed( SEED )
    for n in range( I ):
        if not n % 10000:
            print(n, end=' ')
        init = "node_%d" % random.randint( 0, N )
        tp = "tp_%d" % random.randint( 0, T )
        if init in g:
            V = g.OpenVertex( init )
        else:
            V = g.NewVertex( init, type=tp )
        V[ 'value' ] = random.random()
        V[ 'number' ] = random.randint( 0, N )
        V[ 'string' ] = str( random.randint(0,100) ** random.randint(0,100) )
        V[ 'name' ] = name_prop_fmt.format( init )
        V.SetProperty( "disk_prop_small", virtual_prop_fmt_small.format( init ), True )
        V.SetProperty( "disk_prop_large", virtual_prop_fmt_large.format( init ), True )
        V.SetVector( [ (init,1), (tp,0.5) ] )

        for i in range( random.randint( 0, C ) ):
            mod = MODS[ random.randint(0,2) ]
            fwd_term = "_fwdonly" if mod & M_FWDONLY else ""
            term = "node_{}{}".format( random.randint( 0, N ), fwd_term )
            if random.random() > 0.3:
                if term not in g:
                    g.CreateVertex( term, type="term" )
            rel = "rel_%d" % random.randint( 0, R )
            val = random.randint( Vmin, Vmax )
            g.Connect( init, (rel,mod,val), term )
        if random.random() > 0.95:
            node1 = "node_{}".format( random.randint( 0, N ) )
            node2 = "node_{}_fwdonly".format( random.randint( 0, N ) )
            if node1 in g:
                g.DeleteVertex( node1 )
            if node2 in g:
                g.DeleteVertex( node2 )
        if random.random() > 0.98:
            node1 = "node_{}".format( random.randint( 0, N ) )
            node2 = "node_{}_fwdonly".format( random.randint( 0, N ) )
            if node1 in g:
                g.Disconnect( node1 )
            if node2 in g:
                g.Disconnect( node2 )
        g.CloseVertex( V )
    print()

    still_open = g.GetOpenVertices()
    if len( still_open ) > 0:
        LogWarning( "Open vertices remain after building graph: %s" % still_open )
        LogWarning( "Closing all vertices manually" )
        n_closed = g.CloseAll()
        LogWarning( "Closed %d vertices" % n_closed )

    # Capture state
    STATE = {
        'order'                 : g.order,
        'size'                  : g.size,
        # Encoding is enumeration
        'VertexTypes'           : set(["%s:e=%d:rc=%d" % (name, val[0], val[1]) for name, val in list(g.VertexTypes().items())]),
        'Relationships'         : set(["%s:e=%d:rc=%d" % (name, val[0], val[1]) for name, val in list(g.Relationships().items())]),
        'PropertyKeys'          : set(["%s:e=%d:rc=%d" % (name, val[0], val[1]) for name, val in list(g.PropertyKeys().items())]),
        # Encoding is address so skip that
        'PropertyStringValues'  : set(["%s:rc=%d" % (name, val[1]) for name, val in list(g.PropertyStringValues().items())])
    }

    VERTICES = {}
    order = g.order
    n = 0
    for name in g.Vertices():
        n += 1
        if not n % 10000:
            print("%d/%d" % (n, order), end=' ')
        V = g[ name ]
        d = V.AsDict()
        for key in ["allocator", "address", "index", "bitindex", "bitvector", "op", "readers" ]:
            del d[ key ]

        VERTICES[ name ] = {
            'AsDict'        : d,
            'Neighborhood'  : set( g.Neighborhood( V, fields=F_AARC ) )
        }
        # zzzzz
        if not V.Readonly():
            LogError( "VERTEX LOCK BALANCE IS MESSED UP!!! %s " % name )
            print(VERTICES[ name ])
        # zzzzz
        g.CloseVertex( V )
    print()

    still_open = g.GetOpenVertices()
    if len( still_open ) > 0:
        LogWarning( "Open vertices remain after capturing graph data: %s" % still_open )
        LogWarning( "Closing all vertices manually" )
        n_closed = g.CloseAll()
        LogWarning( "Closed %d vertices" % n_closed )

    # Save and erase from memory
    g.Save( 60000 )
    g.Truncate()
    g.Close()
    system.DeleteGraph( "large_persist", timeout=2000 )
    system.Unload()

    # Restore
    system.Initialize( SYSROOT, attach=None, euclidean=False )
    g = Graph( "large_persist" )

    # Verify
    Expect( g.order == STATE['order'],                  "g.order" )
    Expect( g.size == STATE['size'],                    "g.size" )
    
    VertexTypes           = set(["%s:e=%d:rc=%d" % (name, val[0], val[1]) for name, val in list(g.VertexTypes().items())])
    Relationships         = set(["%s:e=%d:rc=%d" % (name, val[0], val[1]) for name, val in list(g.Relationships().items())])
    PropertyKeys          = set(["%s:e=%d:rc=%d" % (name, val[0], val[1]) for name, val in list(g.PropertyKeys().items())])
    PropertyStringValues  = set(["%s:rc=%d" % (name, val[1]) for name, val in list(g.PropertyStringValues().items())])

    Expect( VertexTypes == STATE['VertexTypes'],                    "VertexTypes, diff=%s" % VertexTypes.symmetric_difference(STATE['VertexTypes']) )
    Expect( Relationships == STATE['Relationships'],                "Relationships, diff=%s" % Relationships.symmetric_difference(STATE['Relationships']) )
    Expect( PropertyKeys == STATE['PropertyKeys'],                  "PropertyKeys, diff=%s" % PropertyKeys.symmetric_difference(STATE['PropertyKeys']) )
    Expect( PropertyStringValues == STATE['PropertyStringValues'],  "PropertyStringValues, diff=%s" % PropertyStringValues.symmetric_difference(STATE['PropertyStringValues']) )

    Expect( set(g.Vertices()) == set(VERTICES),         "Vertices" )
    for name, data in list(VERTICES.items()):
        Expect( name in g,                              "vertex exists" )
        V = g[ name ]
        d = V.AsDict()
        for key in ["allocator", "address", "index", "bitindex", "bitvector", "op", "readers" ]:
            del d[ key ]
        if d != data['AsDict']:
            Expect( False,                              "vertex data mismatch, expected '%s', got '%s'" % (data['AsDict'], d) )
        if V.HasProperties():
            try:
                Expect( V['name'] == name_prop_fmt.format( name ),                          "'name' property" )
                Expect( V['disk_prop_small'] == virtual_prop_fmt_small.format( name ),      "'disk_prop_small' property" )
                Expect( V['disk_prop_large'] == virtual_prop_fmt_large.format( name ),      "'disk_prop_large' property" )
            except:
                Expect( False )
        result = set( g.Neighborhood( V, fields=F_AARC ) )
        Expect( result == data['Neighborhood'],         "vertex neighborhood" )
        g.CloseVertex( V )

    # OK!
    g.Erase()
    system.Unload()




###############################################################################
# TEST_Reinit
#
###############################################################################
def TEST_Reinit():
    """
    Reinitialize graph if unloaded
    test_level=3101
    """
    if not system.IsInitialized():
        system.Initialize( SYSROOT, euclidean=False )





###############################################################################
# Run
#
###############################################################################
def Run( name ):
    """
    """
    global graph
    global SYSROOT
    graph = pyvgx.Graph( name )
    SYSROOT = system.Root()
    RunTests( [__name__] )
    graph = pyvgx.Graph( name )
    graph.Close()
    del graph
