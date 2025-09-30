###############################################################################
# 
# VGX Server
# Distributed engine for plugin-based graph and vector search
# 
# Module:  pyvgxtest
# File:    ClearExpiration.py
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
import time

graph = None




###############################################################################
# TEST_ClearExpiration
#
###############################################################################
def TEST_ClearExpiration():
    """
    pyvgx.Vertex.ClearExpiration()
    Basic
    test_level=3101
    """
    if "vertex" in graph:
        graph.DeleteVertex( "vertex" )

    V = graph.NewVertex( "vertex" )

    Expect( V.TMX == T_NEVER,               "no expiration" )

    tmx = int( time.time() ) + 60

    V.SetExpiration( tmx )
    Expect( V.TMX == tmx,                   "expiration set" )

    V.ClearExpiration()
    Expect( V.TMX == T_NEVER,               "no expiration" )




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
