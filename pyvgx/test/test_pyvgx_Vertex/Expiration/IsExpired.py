###############################################################################
# 
# VGX Server
# Distributed engine for plugin-based graph and vector search
# 
# Module:  pyvgx
# File:    IsExpired.py
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
import time

graph = None



def TEST_IsExpired():
    """
    pyvgx.Vertex.IsExpired()
    Basic
    t_nominal=18
    test_level=3101
    """
    if "vertex" in graph:
        graph.DeleteVertex( "vertex" )

    V = graph.NewVertex( "vertex" )

    Expect( V.IsExpired() == False,         "no expiration" )

    # Should expire
    tmx = int(graph.ts + 2)
    V.SetExpiration( tmx )
    Expect( V.IsExpired() == False,         "not expired (yet)" )
    time.sleep( 4 )
    Expect( V.IsExpired() == True,          "expired" )
    graph.CloseVertex( V )
    del V
    time.sleep( 2 )
    Expect( "vertex" not in graph,          "vertex should no longer exist" )

    # Should expire but we can resurrect by clearing expiration before closing vertex
    V = graph.NewVertex( "vertex" )
    tmx = int(graph.ts + 2)
    V.SetExpiration( tmx )
    Expect( V.IsExpired() == False,         "not expired (yet)" )
    time.sleep( 4 )
    Expect( V.IsExpired() == True,          "expired" )
    # "save" it from expiring
    V.ClearExpiration()
    Expect( V.IsExpired() == False,         "no expiration" )
    graph.CloseVertex( V )
    del V
    time.sleep( 2 )
    Expect( "vertex" in graph,              "vertex should remain in graph with no expiration" )

    # Should expire but we can resurrect by setting expiration to a later time
    V = graph.OpenVertex( "vertex", "a" )
    tmx = int(graph.ts + 2)
    V.SetExpiration( tmx )
    Expect( V.IsExpired() == False,         "not expired (yet)" )
    time.sleep( 4 )
    Expect( V.IsExpired() == True,          "expired" )
    # "save" it from expiring
    tmx = int(graph.ts + 30)
    V.SetExpiration( tmx )
    Expect( V.IsExpired() == False,         "expires in future" )
    graph.CloseVertex( V )
    del V
    time.sleep( 2 )
    Expect( "vertex" in graph,              "vertex should remain in graph, expires later" )

    # Delete
    graph.DeleteVertex( "vertex" )
    Expect( "vertex" not in graph,          "vertex has been deleted" )






def Run( name ):
    global graph
    graph = pyvgx.Graph( name )
    RunTests( [__name__] )
    graph.Close()
    del graph
