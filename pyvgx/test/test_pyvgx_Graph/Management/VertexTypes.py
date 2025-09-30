###############################################################################
# 
# VGX Server
# Distributed engine for plugin-based graph and vector search
# 
# Module:  pyvgx.test
# File:    VertexTypes.py
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

graph = None







###############################################################################
# TEST_VertexTypes
#
###############################################################################
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








###############################################################################
# Run
#
###############################################################################
def Run( name ):
    """
    """
    global graph
    graph = pyvgx.Graph( name )
    RunTests( [__name__] )
    graph.Close()
    del graph
