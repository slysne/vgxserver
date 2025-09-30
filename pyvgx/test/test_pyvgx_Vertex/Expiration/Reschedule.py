###############################################################################
# 
# VGX Server
# Distributed engine for plugin-based graph and vector search
# 
# Module:  pyvgx.test
# File:    Reschedule.py
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
import random

graph = None




###############################################################################
# TEST_Reschedule_vertex
#
###############################################################################
def TEST_Reschedule_vertex():
    """
    pyvgx.Vertex.SetExpiration()
    Reschedule for a different expiration time
    t_nominal=20
    test_level=3101
    """

    # Reschedule earlier
    #
    #
    A_id = "reschedule_node_earlier"
    A = graph.NewVertex( A_id )

    # Initialize expiration 1 minute from now
    TTL = 60
    now = int( graph.ts )
    tmx = now + TTL
    A.SetExpiration( tmx )
    graph.CloseVertex( A )

    # Wait
    time.sleep( 2 )
    A = graph.OpenVertex( A_id )
    Expect( A.GetExpiration() == tmx )

    # Change our minds
    TTL = 5
    now = int( graph.ts )
    tmx = now + TTL
    A.SetExpiration( tmx )
    graph.CloseVertex( A )

    # Wait
    t0 = graph.ts
    waited = 0
    while A_id in graph:
        time.sleep( 0.5 )
        waited = graph.ts - t0
        Expect( waited < TTL + 3 )  # No more than 3 seconds leniency


    # Reschedule later
    #
    #
    B_id = "reschedule_node_later"
    B = graph.NewVertex( B_id )
    
    # Initialize expiration 5 seconds from now
    TTL = 3
    now = int( graph.ts )
    tmx = now + TTL
    B.SetExpiration( tmx )
    graph.CloseVertex( B )

    # Wait
    time.sleep( 1 )
    B = graph.OpenVertex( B_id )
    Expect( B.GetExpiration() == tmx )

    # Change our minds
    TTL = 10
    now = int( graph.ts )
    tmx = now + TTL
    B.SetExpiration( tmx )
    graph.CloseVertex( B )

    # Wait
    t0 = graph.ts
    waited = 0
    while B_id in graph:
        time.sleep( 0.5 )
        waited = graph.ts - t0
        Expect( waited < TTL + 3 )  # No more than 3 seconds leniency




###############################################################################
# TEST_Reschedule_down_small
#
###############################################################################
def TEST_Reschedule_down_small():
    """
    pyvgx.Vertex.SetExpiration()
    Reschedule from long to short TTL, all contained within the smallest partition
    t_nominal=15
    test_level=3102
    """
    graph.Truncate()
    Expect( graph.order == 0 and graph.size == 0 )

    small_thres = int( graph.EventParam()['short']['insertion_threshold'] - 1 )

    # Reschedule many down to 10 seconds TTL
    MIN_TTL = 10
    N = 10000
    for node in range( N ):
        V = graph.NewVertex( str(node) )
        now = int( graph.ts )
        for x in range( small_thres, MIN_TTL, -1 ):
            V.SetExpiration( now + x )
        graph.CloseVertex( V )

    # Wait
    t0 = graph.ts
    waited = 0
    while graph.order > 0:
        print(graph.EventBacklog(), graph)
        time.sleep( 0.333 )
        waited = graph.ts - t0
        Expect( waited < MIN_TTL + 5,   "Graph should be empty after %d seconds, %d out of %d vertices still remain" % (waited, graph.order, N) )

    time.sleep( 2 )
    print(graph.EventBacklog(), graph)




###############################################################################
# TEST_Reschedule_down_medium
#
###############################################################################
def TEST_Reschedule_down_medium():
    """
    pyvgx.Vertex.SetExpiration()
    Reschedule from long to short TTL, from medium to small
    t_nominal=27
    test_level=3102
    """
    graph.Truncate()
    Expect( graph.order == 0 and graph.size == 0 )

    medium_thres = int( graph.EventParam()['medium']['insertion_threshold'] - 1 )

    # Reschedule many down to 10 seconds TTL
    MIN_TTL = 10
    N = 10000
    for node in range( N ):
        V = graph.NewVertex( str(node) )
        now = int( graph.ts )
        for x in range( medium_thres, MIN_TTL, -1 ):
            V.SetExpiration( now + x )
        graph.CloseVertex( V )

    # Wait
    t0 = graph.ts
    waited = 0
    while graph.order > 0:
        print(graph.EventBacklog(), graph)
        time.sleep( 0.333 )
        waited = graph.ts - t0
        Expect( waited < MIN_TTL + 5,   "Graph should be empty after %d seconds, %d out of %d vertices still remain" % (waited, graph.order, N) )

    time.sleep( 2 )
    print(graph.EventBacklog(), graph)


    

###############################################################################
# TEST_Reschedule_down_large
#
###############################################################################
def TEST_Reschedule_down_large():
    """
    pyvgx.Vertex.SetExpiration()
    Reschedule from long to short TTL, from large via medium to small
    t_nominal=28
    test_level=3102
    """
    graph.Truncate()
    Expect( graph.order == 0 and graph.size == 0 )

    large_ttl = int( graph.EventParam()['medium']['insertion_threshold'] + 1000 )

    # Reschedule many down to 10 seconds TTL
    MIN_TTL = 10
    N = 10000
    for node in range( N ):
        V = graph.NewVertex( str(node) )
        now = int( graph.ts )
        for x in range( large_ttl, MIN_TTL, -1 ):
            V.SetExpiration( now + x )
        graph.CloseVertex( V )

    # Wait
    t0 = graph.ts
    waited = 0
    while graph.order > 0:
        print(graph.EventBacklog(), graph)
        time.sleep( 0.333 )
        waited = graph.ts - t0
        Expect( waited < MIN_TTL + 5,   "Graph should be empty after %d seconds, %d out of %d vertices still remain" % (waited, graph.order, N) )

    time.sleep( 2 )
    print(graph.EventBacklog(), graph)




###############################################################################
# TEST_Reschedule_up_down
#
###############################################################################
def TEST_Reschedule_up_down():
    """
    pyvgx.Vertex.SetExpiration()
    Reschedule from short to long to short TTL
    t_nominal=43
    test_level=3102
    """
    graph.Truncate()
    Expect( graph.order == 0 and graph.size == 0 )

    large_ttl = int( graph.EventParam()['medium']['insertion_threshold'] + 1000 )

    # Reschedule many down to 10 seconds TTL
    MIN_TTL = 10
    N = 10000
    for node in range( N ):
        name = str(node)
        V = graph.NewVertex( name )
        now = int( graph.ts )
        for x in range( 1, large_ttl ):
            V.SetExpiration( now + x )
        graph.CloseVertex( V )
        Expect( name in graph )
        V = graph.OpenVertex( name )
        for x in range( large_ttl, MIN_TTL, -1 ):
            V.SetExpiration( now + x )
        graph.CloseVertex( V )
        Expect( name in graph )

    # Wait
    t0 = graph.ts
    waited = 0
    while graph.order > 0:
        print(graph.EventBacklog(), graph)
        time.sleep( 0.333 )
        waited = graph.ts - t0
        Expect( waited < MIN_TTL + 5,   "Graph should be empty after %d seconds, %d out of %d vertices still remain" % (waited, graph.order, N) )

    time.sleep( 2 )
    print(graph.EventBacklog(), graph)




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
