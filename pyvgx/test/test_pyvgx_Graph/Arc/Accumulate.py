###############################################################################
# 
# VGX Server
# Distributed engine for plugin-based graph and vector search
# 
# Module:  pyvgx.test
# File:    Accumulate.py
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
import time
from pyvgx import *
import pyvgx
import random

graph = None



###############################################################################
# float_eq
#
###############################################################################
def float_eq( a, b ):
    """
    """
    if b:
        return abs( 1 - ( a / b ) ) < 1e-5
    else:
        return abs( a ) < 1e-5



###############################################################################
# TEST_Accumulate
#
###############################################################################
def TEST_Accumulate():
    """
    pyvgx.Graph.Accumulate()
    test_level=3101
    """
    graph.Truncate()

    A = graph.NewVertex( "A" )
    B = graph.NewVertex( "B" )

    random.seed( 1000 )

    val = 0.0
    for n in range( 1000 ):
        inc = 100 * (random.random()-0.5)
        val += inc
        acc = graph.Accumulate( A, "to", B, inc )
        Expect( float_eq( acc, val ), "accumulated value should be %f, got %f" % (val, acc) )


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
