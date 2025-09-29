###############################################################################
# 
# VGX Server
# Distributed engine for plugin-based graph and vector search
# 
# Module:  pyvgx
# File:    CloseVertex.py
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

graph = None




###############################################################################
# TEST_CloseVertex
#
###############################################################################
def TEST_CloseVertex():
    """
    pyvgx.Graph.CloseVertex()
    test_level=3101
    """
    # Reset
    graph.Truncate()

    # Open, then close and check
    A = graph.NewVertex( "A" )
    Support._VerifyNewVertex( graph, A, "A" )
    Expect( graph.CloseVertex( A ) is True,       "Vertex should be closed" )
    Expect( A.Writable() is False,                "Vertex should not be writable" )
    Expect( A.Readable() is False,                "Vertex should not be readable" )
    Expect( A.Readonly() is False,                "Vertex should not be readonly" )
    try:
        A.id
        Except( False,  "Vertex should not be accessible after close" )
    except pyvgx.AccessError as ex:
        Expect( str(ex).startswith( "Vertex is not accessible" ),  "Exception message should state that vertex is not accessible" )
    except:
        Except( False,  "Should not raise this exception" )




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
