###############################################################################
# 
# VGX Server
# Distributed engine for plugin-based graph and vector search
# 
# Module:  pyvgx
# File:    HasProperty.py
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



def TEST_HasProperty():
    """
    pyvgx.Vertex.HasProperty()
    Basic
    test_level=3101
    """
    if "vertex" in graph:
        graph.DeleteVertex( "vertex" )

    V = graph.NewVertex( "vertex" )

    Expect( V.HasProperty( "x" ) is False,                  "no x" )
    Expect( "x" not in V,                                   "no x" )
    Expect( V.HasProperty( "y" ) is False,                  "no y" )
    Expect( "y" not in V,                                   "no y" )

    V['x'] = 1234
    Expect( V.HasProperty( "x" ) is True,                   "has x" )
    Expect( "x" in V,                                       "has x" )
    Expect( V.HasProperty( "y" ) is False,                  "no y" )
    Expect( "y" not in V,                                   "no y" )

    V['y'] = "onetwo"
    Expect( V.HasProperty( "x" ) is True,                   "has x" )
    Expect( "x" in V,                                       "has x" )
    Expect( V.HasProperty( "y" ) is True,                   "has y" )
    Expect( "y" in V,                                       "has y" )






def Run( name ):
    global graph
    graph = pyvgx.Graph( name )
    RunTests( [__name__] )
    graph.Close()
    del graph
