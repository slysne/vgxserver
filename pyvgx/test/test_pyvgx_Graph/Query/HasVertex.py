###############################################################################
# 
# VGX Server
# Distributed engine for plugin-based graph and vector search
# 
# Module:  pyvgxtest
# File:    HasVertex.py
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
# TEST_vxquery_inspect
#
###############################################################################
def TEST_vxquery_inspect():
    """
    Core vxquery_inspect
    test_level=501
    """
    try:
        pyvgx.selftest( force=True, testroot="vgxtest", library="vgx", names=["vxquery_inspect.c" ] )
    except:
        Expect( False )




###############################################################################
# TEST_HasVertex
#
###############################################################################
def TEST_HasVertex():
    """
    pyvgx.Graph.HasVertex()
    test_level=3101
    """
    node = "node_TEST_HasVertex"

    Expect( graph.HasVertex( node ) == False,           "node does not exist" )
    graph.CreateVertex( node )
    Expect( graph.HasVertex( node ) == True,            "node exists" )
    graph.DeleteVertex( node )
    Expect( graph.HasVertex( node ) == False,           "node does not exist" )




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
