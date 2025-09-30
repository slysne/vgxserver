###############################################################################
# 
# VGX Server
# Distributed engine for plugin-based graph and vector search
# 
# Module:  pyvgxtest
# File:    NumProperties.py
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
# TEST_NumProperties
#
###############################################################################
def TEST_NumProperties():
    """
    pyvgx.Vertex.NumProperties()
    Basic
    test_level=3101
    """
    if "vertex" in graph:
        graph.DeleteVertex( "vertex" )

    V = graph.NewVertex( "vertex" )

    Expect( V.NumProperties() == 0,                         "no properties" )

    V['x'] = 1
    Expect( V.NumProperties() == 1,                         "one property" )

    V['y'] = 1
    Expect( V.NumProperties() == 2,                         "two properties" )
    
    del V['x']
    Expect( V.NumProperties() == 1,                         "one property" )

    del V['y']
    Expect( V.NumProperties() == 0,                         "no properties" )





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
