###############################################################################
# 
# VGX Server
# Distributed engine for plugin-based graph and vector search
# 
# Module:  pyvgxtest
# File:    CreateVertex.py
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
from . import _vertex_test_support as Support
from pyvgx import *
import pyvgx
import time
import itertools

graph = None




###############################################################################
# TEST_CreateVertex
#
###############################################################################
def TEST_CreateVertex():
    """
    pyvgx.Graph.CreateVertex()
    test_level=3101
    """
    Support._NewVertex_or_CreateVertex( graph, "CreateVertex" )




###############################################################################
# TEST_CreateVertex_with_attr
#
###############################################################################
def TEST_CreateVertex_with_attr():
    """
    pyvgx.Graph.CreateVertex()
    with type, lifespan, properties
    t_nominal=17
    test_level=3101
    """
    g = graph
    g.Truncate()
    node10 = "gone_in_ten_seconds"
    node12 = "gone_in_twelve_seconds"
    node14 = "gone_in_fourteen_seconds"
    node16 = "gone_in_sixteen_seconds"
    t0 = g.ts
    g.CreateVertex( node10, type="node", lifespan=10, properties={ 'x':10 } )
    g.CreateVertex( properties={ 'x':12 }, type="node", lifespan=12, id=node12 )
    g.CreateVertex( properties={ 'x':14 }, lifespan=14, id=node14, type="node" )
    g.CreateVertex( node16, "node", 16, { 'x':16 } )

    Expect( node10 in g and node12 in g and node14 in g and node16 in g )

    t10 = t0 + 10
    t12 = t0 + 12
    t14 = t0 + 14
    t16 = t0 + 16
    while g.order > 0:
        order = g.order
        ts = g.ts
        if order == 3:
            Expect( node12 in g and node14 in g and node16 in g,    "order=3, %s should be deleted" % node10 )
            Expect( ts > t10,                                       "t > t10, got t=%f t10=%f" % (ts, t10) )
        elif order == 2:
            Expect( node14 in g and node16 in g,                    "order=2, %s should be deleted" % [node10, node12] )
            Expect( ts > t12,                                       "t > t12, got t=%f t12=%f" % (ts, t12) )
        elif order == 1:
            Expect( node16 in g,                                    "order=1, %s should be deleted" % [node10, node12, node14] )
            Expect( ts > t14,                                       "t > t14, got t=%f t14=%f" % (ts, t14) )
        time.sleep( 0.1 )

    ts = g.ts
    Expect( ts > t16,                                               "t > t16, got t=%f t16=%f" % (ts, t16) )




###############################################################################
# TEST_CreateVertex_utf8
#
###############################################################################
def TEST_CreateVertex_utf8():
    """
    pyvgx.Graph.CreateVertex() with various names
    test_level=3101
    """
    g = graph
    g.Truncate()


    # Names cannot include codepoints in the range 0x0 - 0x1f, or 0xd800 - 0xdfff.
    for n in itertools.chain( range(0,0x20), range(0xd800,0xe000) ):
        c = chr( n )
        names = [
            "%s" % c,               # single char
            "node_%s" % c,          # char as last
            "%s_node" % c,          # char as first
            "%s%s" % ("x"*100, c),  # char as last, long id
            "%s%s" % (c, "x"*100)   # char as first, long id
            ]
        for name in names:
            try:
                g.CreateVertex( name )
                Expect( False,      "Should not create vertex with illegal name, codepoint %d" % n )
            except pyvgx.VertexError:
                pass
            except UnicodeEncodeError:
                pass
            except Exception as ex:
                Expect( False,      "Unexpeced exception %s" % str(ex) )



    # Invalid UTF-8 sequence should raise exception
    seq = [
        (True,  bytes( [0x7f] )),
        (False, bytes( [0x80] )),
        (False, bytes( [0x81] )),
        (False, bytes( [0xfe] )),
        (False, bytes( [0xff] )),
        (False, bytes( [0xc1, 0x80] )),
        (False, bytes( [0xc1, 0xff] )),
        (False, bytes( [0xc2, 0x7f] )),
        (False, bytes( [0xc2] )),
        (True,  bytes( [0xc2, 0x80] )),
        (True,  bytes( [0xc2, 0xbf] )),
        (False, bytes( [0xc2, 0xc0] )),
        (True,  bytes( [0xc3, 0x80] )),
        (True,  bytes( [0xdf, 0x80] )),
        (True,  bytes( [0xdf, 0xbf] )),
        (False, bytes( [0xdf, 0xc0] )),
        (False, bytes( [0xe0, 0x80] )),
        (False, bytes( [0xe0, 0x80, 0x80] )),
        (False, bytes( [0xe0, 0x9f, 0x80] )),
        (False, bytes( [0xe0, 0xa0] )),
        (True,  bytes( [0xe0, 0xa0, 0x80] )),
        (False, bytes( [0xe0, 0xa0, 0xc0] )),
        (True,  bytes( [0xe1, 0x80, 0x80] )),
        (True,  bytes( [0xe1, 0xbf, 0xbf] )),
        (True,  bytes( [0xe2, 0x80, 0x80] )),
        (True,  bytes( [0xe2, 0xbf, 0xbf] )),
        (True,  bytes( [0xed, 0x9f, 0xbf] )),
        (False, bytes( [0xed, 0xa0, 0x80] )),
        (False, bytes( [0xed, 0xbf, 0xbf] )),
        (True,  bytes( [0xee, 0x80, 0x80] )),
        (True,  bytes( [0xef, 0xbf, 0xbf] )),
        (False, bytes( [0xf0, 0x80, 0x80, 0x80] )),
        (False, bytes( [0xf0, 0x80, 0xbf, 0xbf] )),
        (False, bytes( [0xf0, 0x8f, 0xbf, 0xbf] )),
        (True,  bytes( [0xf0, 0x90, 0x80, 0x80] )),
        (True,  bytes( [0xf0, 0x90, 0x80, 0xbf] )),
        (True,  bytes( [0xf0, 0x90, 0xbf, 0xbf] )),
        (True,  bytes( [0xf0, 0xbf, 0xbf, 0xbf] )),
        (True,  bytes( [0xf1, 0x80, 0x80, 0x80] )),
        (True,  bytes( [0xf4, 0x8f, 0xbf, 0xbf] )),
        (False, bytes( [0xf4, 0x90, 0x80, 0x80] ))
    ]

    Expect( g.order == 0,           "Graph should be empty, order=%d" % g.order )
    for valid, utf8 in seq:
        if valid:
            # Sanity check
            try:
                name = utf8.decode()
            except:
                Expect( False )
            # Create vertex
            try:
                Expect( g.CreateVertex( name ) == 1,        "Should create vertex with valid name '%s'" % name )
                Expect( g.CreateVertex( utf8 ) == 0,        "Vertex should already exist" )
            except Exception as ex:
                Expect( False, "exception: %s" % str(ex) )
        # INVALID
        else:
            # Sanity check
            try:
                utf8.decode()
                Expect( False, "sequence invalid" )
            except UnicodeError:
                pass
            # Fail to create vertex
            try:
                g.CreateVertex( utf8 )
                Expect( False, "Should not be able to create vertex with invalid name" )
            except UnicodeError:
                pass
    g.Truncate()


    # All valid codepoints
    Expect( g.order == 0,           "Graph should be empty, order=%d" % g.order )
    N = 0
    for n in itertools.chain( range(0x20,0xd800), range(0xe000, 0x110000) ):
        c = chr( n )
        names = [
            "%s" % c,               # single char
            "node_%s" % c,          # char as last
            "%s_node" % c,          # char as first
            "%s_%s" % ("x"*100, c), # char as last, long id
            "%s_%s" % (c, "x"*100)  # char as first, long id
            ]
        for name in names:
            # Create vertex using unicode string
            Expect( g.CreateVertex( name ) == 1,            "Should create vertex '%s', codepoint %d" % (name, n) )
            N += 1
            # Create vertex using UTF-8 encoded string, should already be created above
            name_utf8 = name.encode()
            Expect( g.CreateVertex( name_utf8 ) == 0,       "Vertex '%s' should already be created" % name_utf8 )

    Expect( g.order == N,           "Graph order should be %d, got %d" % (N, g.order) )

    # Get all vertices
    V = g.Vertices()
    Expect( len(V) == N,            "All %d vertices should be returned, got %d" % (N, len(V)) )
    
    # Check all vertex names, establish arc from ROOT to each
    S = set( V )
    for n in itertools.chain( range(0x20,0xd800), range(0xe000, 0x110000) ):
        c = chr( n )
        names = [
            "%s" % c,               # single char
            "node_%s" % c,          # char as last
            "%s_node" % c,          # char as first
            "%s_%s" % ("x"*100, c), # char as last, long id
            "%s_%s" % (c, "x"*100)  # char as first, long id
            ]
        for name in names:
            Expect( name in S,      "Vertex %s should be returned set" % name )
            Expect( name in g,      "Vertex %s should exist in graph" % name )
            name_utf8 = name.encode()
            Expect( name_utf8 in g, "Vertex %s should exist in graph" % name_utf8 )
            Expect( g.Connect( "ROOT", "to", name ) == 1,        "New arc" )
            Expect( g.Connect( "ROOT", "to", name_utf8 ) == 0,   "Arc should already exist" )

    # Check ROOT neighborhood
    result_1 = g.Neighborhood( "ROOT" )
    Expect( len(result_1) == N,     "All %d vertices should be returned, got %d" % (N, len(result_1)) )
    Expect( set(result_1) == S,     "ROOT neighborhood should equal vertex set" )

    ROOT = g.OpenVertex( "ROOT" )
    result_2 = ROOT.Neighbors()
    result_3 = ROOT.Terminals()
    Expect( result_2 == result_1,   "ROOT.Neighbors() should match graph query" )
    Expect( result_3 == result_1,   "ROOT.Terminals() should match graph query" )

    del V
    del S
    del result_1
    del result_2
    del result_3
    del ROOT

    g.Truncate()
    Expect( g.order == 0,           "Graph should be empty, order=%d" % g.order )












###############################################################################
# Run
#
###############################################################################
def Run( name ):
    """
    Run the tests in this module
    """
    global graph
    graph = pyvgx.Graph( name )
    RunTests( [__name__] )
    graph.Close()
    del graph
