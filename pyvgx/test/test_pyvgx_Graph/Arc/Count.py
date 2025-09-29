###############################################################################
# 
# VGX Server
# Distributed engine for plugin-based graph and vector search
# 
# Module:  pyvgx
# File:    Count.py
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
import time
from pyvgx import *
import pyvgx

graph = None




###############################################################################
# TEST_Count
#
###############################################################################
def TEST_Count():
    """
    pyvgx.Graph.Count()
    test_level=3101
    """
    graph.Truncate()

    A = graph.NewVertex( "A" )
    B = graph.NewVertex( "B" )

    val = 0
    for n in range(1, 100):
        val += 1
        Expect( graph.Count( A, "to", B ) == val,                       "inc 1 ==> count = %d" % val )
        Expect( graph.ArcValue( A, ("to", D_OUT, M_CNT), B ) == val,    "arc value = %d" % val )

    Expect( graph.Count( A, "to", B, 0 ) == val,                        "inc 0 ==> count = %d" % val )
    Expect( graph.ArcValue( A, ("to", D_OUT, M_CNT), B ) == val,        "arc value = %d" % val )

    for n in range( 1, 200 ):
        if val > 0:
            val -= 1
        Expect( graph.Count( A, "to", B, -1 ) == val,                   "inc -1 ==> count = %d" % val )
        Expect( graph.ArcValue( A, ("to", D_OUT, M_CNT), B ) == val,    "arc value = %d" % val )

    Expect( graph.ArcValue( A, ("to", D_OUT, M_CNT), B ) == 0,          "arc value = 0" )
    
    val = 0
    inc = 0
    for n in range(33):
        inc = n - 2*inc
        val += inc
        if val < 0:
            val = 0
        elif val > 2**32:
            val = 2**32
        Expect( graph.Count( A, "to", B, inc ) == val,              "value after inc=%d should be %d" % (inc, val) )

    graph.CloseVertex(A)
    graph.CloseVertex(B)
    graph.DeleteVertex( "A" )
    graph.DeleteVertex( "B" )



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
    graph.Truncate()
    graph.Close()
    del graph
