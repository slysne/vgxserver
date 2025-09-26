###############################################################################
# 
# VGX Server
# Distributed engine for plugin-based graph and vector search
# 
# Module:  pyvgx
# File:    GetProperty.py
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



def TEST_GetProperty():
    """
    pyvgx.Vertex.GetProperty()
    Basic
    test_level=3101
    """
    if "vertex" in graph:
        graph.DeleteVertex( "vertex" )

    V = graph.NewVertex( "vertex" )

    Expect( V.GetProperty( "x" ) is None,                   "property does not exist" )
    Expect( V.GetProperty( "x", 100 ) == 100,               "default integer" )
    Expect( V.GetProperty( "x", "string" ) == "string",     "default string" )

    V['x'] = 1234
    Expect( V.GetProperty( "x" ) == 1234,                   "integer" )
    Expect( V.GetProperty( "x", 100 ) == 1234,              "integer" )
    Expect( V.GetProperty( "x", "string" ) == 1234,         "integer" )

    V['y'] = "onetwo"
    Expect( V.GetProperty( "y" ) == "onetwo",               "string" )
    Expect( V.GetProperty( "y", 100 ) == "onetwo",          "string" )
    Expect( V.GetProperty( "y", "string" ) == "onetwo",     "string" )




def Run( name ):
    global graph
    graph = pyvgx.Graph( name )
    RunTests( [__name__] )
    graph.Close()
    del graph
