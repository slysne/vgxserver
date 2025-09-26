###############################################################################
# 
# VGX Server
# Distributed engine for plugin-based graph and vector search
# 
# Module:  pyvgx
# File:    Size.py
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



def TEST_Size():
    """
    pyvgx.Graph.Size()
    test_level=3101
    """
    g = Graph( "vertices" )
    g.Truncate()

    N = 100
    i = 0
    for n in range( N ):
        g.Connect( "node_%d" % n, "to", "node_%d" % (n+1) )
        i += 1
        Expect( g.Size() == i,                      "%d arcs" % i )
        
        g.Connect( "node_%d" % n, ("to",M_INT,n), "node_%d" % (n+1) )
        i += 1
        Expect( g.Size() == i,                      "%d arcs" % i )

    g.Erase()



def Run( name ):
    global graph
    graph = pyvgx.Graph( name )
    RunTests( [__name__] )
    graph.Close()
    del graph
