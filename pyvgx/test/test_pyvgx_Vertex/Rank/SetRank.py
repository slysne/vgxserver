###############################################################################
# 
# VGX Server
# Distributed engine for plugin-based graph and vector search
# 
# Module:  pyvgx.test
# File:    SetRank.py
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
# TEST_SetRank
#
###############################################################################
def TEST_SetRank():
    """
    pyvgx.Vertex.SetRank()
    Basic
    test_level=3101
    """
    if "vertex" in graph:
        graph.DeleteVertex( "vertex" )

    V = graph.NewVertex( "vertex" )

    c1, c0 = V.SetRank()
    Expect( c1 == 1.0 )
    Expect( c0 == 0.0 )

    c1, c0 = V.SetRank( 2 )
    Expect( c1 == 2.0 )
    Expect( c0 == 0.0 )

    c1, c0 = V.SetRank( c0=1024 )
    Expect( c1 == 2.0 )
    Expect( c0 == 1024.0 )

    c1, c0 = V.SetRank( c1=4.0 )
    Expect( c1 == 4.0 )
    Expect( c0 == 1024.0 )

    c1, c0 = V.SetRank( c1=8.0, c0=512 )
    Expect( c1 == 8.0 )
    Expect( c0 == 512.0 )

    c1, c0 = V.SetRank()
    Expect( c1 == 8.0 )
    Expect( c0 == 512.0 )




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
