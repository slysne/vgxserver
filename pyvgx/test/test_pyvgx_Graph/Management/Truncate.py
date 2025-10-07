###############################################################################
# 
# VGX Server
# Distributed engine for plugin-based graph and vector search
# 
# Module:  pyvgx.test
# File:    Truncate.py
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
import random
import time

graph = None




###############################################################################
# TEST_Truncate_no_arcs
#
###############################################################################
def TEST_Truncate_no_arcs():
    """
    pyvgx.Graph.Truncate()
    Basic truncation of different type, no arcs
    test_level=3101
    """
    g = Graph( "vertices" )

    g.Truncate()
    Expect( g.order == 0 and g.size == 0,                  "empty graph" )

    N = 1000


    TYPES = [ None, 'something', 'another', 'this', 'that', 'thing', 'node' ]
    for tp in TYPES:
        for n in range( N ):
            g.CreateVertex( "node_%s_%d" % (tp, n) )
    Expect( g.order == len(TYPES)*N,                        "graph populated" )
    count = g.order
    Expect( g.Truncate() == count,                          "all nodes removed" )
    Expect( g.order == 0 and g.size == 0,                  "empty graph" )

    i = 0
    indexed = {}
    for tp in TYPES:
        indexed[ tp ] = 0
        i += 1
        for n in range( N+1 ):
            if tp is None:
                name = "typeless_%d" % n
                g.CreateVertex( name )
            else:
                name = "type_%s_%d" % (tp, n)
                g.CreateVertex( name, type=tp )
            indexed[ tp ] += 1

    Expect( sum( indexed.values() ) == g.order,             "all nodes" )

    Expect( g.Truncate( "nonexist" ) == 0,                  "none of this type" )

    Expect( sum( indexed.values() ) == g.order,             "all nodes" )

    for tp in TYPES:
        Expect( g.Truncate( tp ) == indexed[ tp ],          "truncated all of type %s" % tp )
        indexed[ tp ] = 0
        Expect( sum( indexed.values() ) == g.order,         "remaining count = %d" % g.order )
        
    Expect( g.order == 0,                                   "empty graph" )




###############################################################################
# TEST_Truncate_arcs_regular_and_forward
#
###############################################################################
def TEST_Truncate_arcs_regular_and_forward():
    """
    pyvgx.Graph.Truncate()
    Truncation with regular and forward-only arcs
    test_level=3101
    """
    g = Graph( "vertices" )
    g.Truncate()

    # initial
    g.CreateVertex( "A" )

    for mod in [M_STAT, M_STAT|M_FWDONLY]:

        # terminals
        for n in range(3):
            term = "T{}".format(n)
            g.CreateVertex( term, type="term" )

        # connect
        for n in range(3):
            term = "T{}".format(n)
            g.Connect( "A", ("to", M_STAT), term )

        # check
        Expect( g.size == 3 and g.order == 4,           "size=3 and order=4" )
        Expect( len( g.Neighborhood( "A" ) ) == 3,      "A has three neighbors" )

        # truncate terminals
        Expect( g.Truncate( "term" ) == 3,              "truncate term nodes" )

        # check
        Expect( g.size == 0 and g.order == 1,           "size=0 and order=1" )
        Expect( len( g.Neighborhood( "A" ) ) == 0,      "A has no neighbors" )




###############################################################################
# TEST_Truncate_with_arcs
#
###############################################################################
def TEST_Truncate_with_arcs():
    """
    pyvgx.Graph.Truncate()
    Truncation with arcs between types
    test_level=3101
    """
    random.seed( 1000 )

    g = Graph( "vertices" )
    g.Truncate()

    TYPES = [ "person", "city", "food" ]

    P = 1000
    C = 50
    F = 20

    # persons
    for p in range( P ):
        person = "person_{}".format(p)
        g.CreateVertex( person, "person" )
    Expect( g.Order("person") == P,             "{} person nodes".format(P) )

    # cities
    for c in range( C ):
        city = "city_{}".format(c)
        g.CreateVertex( city, "city" )
    Expect( g.Order("city") == C,               "{} city nodes".format(C) )

    # foods
    for f in range( F ):
        food = "food_{}".format(f)
        g.CreateVertex( food, "food" )
    Expect( g.Order("food") == F,               "{} food nodes".format(F) )

    # person visits city
    # NOTE: forward-only arcs
    for person in g.VerticesType( "person" ):
        city = "city_{}".format( (random.randint(0,C-1)) )
        g.Connect( person, ("visits", M_STAT|M_FWDONLY), city )

    # city famous for food
    for city in g.VerticesType( "city" ):
        food = "food_{}".format( (random.randint(0,F-1)) )
        g.Connect( city, "famous_for", food )
        g.Connect( city, "in_country", "virtual_country" )

    # Verify order
    Expect( g.order == P + C + F + 1,           "all nodes" )

    # Verify size
    Expect( g.size == P + 2*C,                  "all arcs" )

    # Virtualize all cities (not erased when forward-only inarcs exist)
    Expect( g.Truncate( "city" ) == 0,          "cities virtualized, not removed" )

    # Verify size
    Expect( g.size == P,                        "only person to city" )

    ## NOTE: Strange effect after truncation of nodes connecting to virtual nodes.
    ##       The virtual nodes are not automatically removed by the truncation.
    ##       They need to be opened and closed in order to go away.
    ## TODO: Fix this in a future release
    g.CloseVertex( g.OpenVertex( "virtual_country", "r" ) )
    1
    ##

    Expect( "virtual_country" not in g,         "virtual node removed" )

    # all persons and foods remain
    Expect( g.Order("person") == P,             "{} person nodes".format(P) )
    Expect( g.Order("city") == C,               "{} city nodes".format(C) )
    Expect( g.Order("food") == F,               "{} food nodes".format(F) )

    # Verify order
    Expect( g.order == P + C + F,               "total order remains" )

    # Erase persons
    Expect( g.Truncate( "person" ) == P,        "erased persons" )

    ## NOTE: Same reason as above
    g.CloseVertices( g.OpenVertices( [c for c in g.VerticesType("city") if c in g] ) )

    # Verify order
    Expect( g.order == F,                       "only food remains" )

    # Verify size
    Expect( g.size == 0,                        "no arcs" )

    # Erase
    Expect( g.Truncate() == F,                  "removed all" )
    Expect( g.order == 0,                       "empty graph" )




###############################################################################
# TEST_Truncate_repeat
#
###############################################################################
def TEST_Truncate_repeat():
    """
    pyvgx.Graph.Truncate()
    Abuse truncate
    t_nominal=30
    test_level=3102
    """

    g = Graph( "truncated" )

    for pause in [1.0, 0.5, 0.2, 0.1, 0.05, 0.02, 0.01, 0.005, 0.002, 0.001, 0.001, 0.001, 0, 0, 0, 0.001, 0.01, 0.1, 1.0]:
        g.CreateVertex( "X" )
        Expect( g.Truncate() == 1 )
        time.sleep( pause )

        g.CreateVertex( "Y" )
        g.SetGraphReadonly( 60000 )
        g.ClearGraphReadonly()
        Expect( g.Truncate() == 1 )
        time.sleep( pause )

        g.CreateVertex( "Z" )
        g.EventDisable()
        g.SetGraphReadonly( 60000 )
        g.ClearGraphReadonly()
        g.EventEnable()
        Expect( g.Truncate() == 1 )
        time.sleep( pause )

    Expect( g.order == 0,                       "empty graph" )

    g.Close()
    del g




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
