###############################################################################
# 
# VGX Server
# Distributed engine for plugin-based graph and vector search
# 
# Module:  pyvgx
# File:    GetTypeEnum.py
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

from pytest.pytest import RunTests, Expect, TestFailed
from pyvgx import *
import pyvgx

graph = None




###############################################################################
# TEST_GetTypeEnum
#
###############################################################################
def TEST_GetTypeEnum():
    """
    pyvgx.Vertex.GetTypeEnum()
    test_level=3101
    """
    g = graph
    g.Truncate()

    # Create typeless vertex
    A = g.NewVertex( "vertex" )

    # Create typed vertex
    B = g.NewVertex( "node_0", type="node" )

    tA = A.GetTypeEnum()
    tB = B.GetTypeEnum()

    Expect( type(tA) is int,            "type enum should be int, got %s" % type(tA) )

    Expect( tA != tB,                   "type enumerations should be different, got %d" % tA )
    
    A.SetType( "node" )
    t = A.GetTypeEnum()
    Expect( t == tB,                    "type enumerations should be the same, got %s" % [t, tB] )

    B.SetType( None )
    t = B.GetTypeEnum()
    Expect( t == tA,                    "type enumeration should be %d, got %d" % (tA, t) )

    del A
    del B




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
