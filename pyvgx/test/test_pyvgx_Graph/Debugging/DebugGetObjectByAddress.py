###############################################################################
# 
# VGX Server
# Distributed engine for plugin-based graph and vector search
# 
# Module:  pyvgx
# File:    DebugGetObjectByAddress.py
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





###############################################################################
# TEST_DebugGetObjectByAddress
#
###############################################################################
def TEST_DebugGetObjectByAddress():
    """
    pyvgx.Graph.DebugGetObjectByAddress()
    test_level=3101
    """
    graph = Graph( "debug" )
    graph.Truncate()

    for n in range( 100000 ):
        V = graph.NewVertex( "vertex_%d" % n )
        graph.CloseVertex( V )

    for n in range( 100 ):
        vertices = graph.Vertices( hits=100, sortby=S_RANDOM, result=R_DICT, fields=F_ID, select=".address" )
        for v in vertices:
            id = v['id']
            prop = v['properties']
            addr = prop['.address']
            objrepr = graph.DebugGetObjectByAddress( addr )
            Expect( objrepr.startswith( "<object at 0x%x" % addr ) )


    graph.Erase()




###############################################################################
# Run
#
###############################################################################
def Run( name ):
    """
    """
    RunTests( [__name__] )
