###############################################################################
# 
# VGX Server
# Distributed engine for plugin-based graph and vector search
# 
# Module:  pyvgx.test
# File:    Close.py
# Author:  Stian Lysne slysne.dev@gmail.com
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
# TEST_Close
#
###############################################################################
def TEST_Close():
    """
    pyvgx.Vertex.Close()
    Call method
    test_level=3101
    """
    vertexid = "vertex"

    if vertexid in graph:
        graph.DeleteVertex( vertexid )

    # Open
    V = graph.NewVertex( vertexid )
    # Check
    Expect( V.id == vertexid,                     "Vertex id should be %s" % vertexid )
    # Prime cached attributes
    V_id = V.id
    V_internalid = V.internalid
    V_address = V.address
    V_enum = V.enum
    # Close
    V.Close()
    # Verify not accessible
    try:
        V.type
        Expect( False,  "Vertex should not be accessible after close" )
    except pyvgx.AccessError as ex:
        Expect( str(ex).startswith( "Vertex is not accessible" ),  "Exception message should state that vertex is not accessible" )
    except:
        Expect( False,  "Should not raise this exception" )
    # Verify access to cached attributes
    try:
        assert V.id == V_id
        assert V.internalid == V_internalid
        assert V.address == V_address
        assert V.enum == V_enum
    except pyvgx.AccessError as ex:
        Expect( False,  "Certain vertex attributes should still be accessible after close" )
    except:
        Expect( False,  "Should not raise this exception" )




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
