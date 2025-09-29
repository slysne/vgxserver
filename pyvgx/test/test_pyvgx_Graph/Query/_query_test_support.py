###############################################################################
# 
# VGX Server
# Distributed engine for plugin-based graph and vector search
# 
# Module:  pyvgx
# File:    _query_test_support.py
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

from pyvgxtest.pyvgxtest import Expect
from pyvgx import *
import pyvgx
import time
import re
import os, sys




###############################################################################
# __build_fanout
#
###############################################################################
def __build_fanout( graph, initial="root", remaining_levels=[1,2], fanout_factor=4, modifiers=[M_INT,M_FLT], relationships=[] ):
    """
    """
    r"""


    level_<x>_<y>.level = x
    level_<x>_<y>.number = y
    level_<x>_<y>.parent = level_<x-1>_<>.number
    level_<x>_<y>.name = "level_<x>_<y>"
    level_<x>_<y>.short = "n<x><yyyy>"

    (root)  -[to_level_1,M_INT,0]-> (level_1_0)  -[to_level_2,M_INT,0]-> (level_2_0)
                                                \-[to_level_2,M_FLT,0]-> (level_2_0)
                                                ...
                                                ...
                                                \-[to_level_2,M_INT,7]-> (level_2,7)
                                                \-[to_level_2,M_FLT,7]-> (level_2,7)
           \-[to_level_1,M_FLT,0]-> (level_1_0)

    .
    .
    .

    (root)  -[to_level_1,M_INT,3]-> (level_1_3)
           \-[to_level_1,M_FLT,3]-> (level_1_3)





    NOTE: Default relationship "to_level_<n>" is included by default.
          Additional relationships will be added by repeating all insertions
          made for the default relationship.

    """
    if not remaining_levels:
        return
    level, remaining_levels = remaining_levels[0], remaining_levels[1:]
    fanout = level * fanout_factor
    R = graph.OpenVertex( initial )

    for fx in range( fanout ):
        terminal = "level_%d_%d" % (level, fx)
        if not terminal in graph:
            T = graph.NewVertex( terminal, type="level_%d" % level )
            T.SetProperty( "level", level )
            T.SetProperty( "number", fx )
            T.SetProperty( "name", terminal )
            T.SetProperty( "short", "n%d%04d" % (level, fx) )
            __build_fanout( graph, T.id, remaining_levels, fanout_factor, modifiers, relationships )
        # Default relationship
        rel = "to_level_%d" % level
        for mod in modifiers:
            graph.Connect( initial, (rel, mod, fx), terminal )
        # Additional relationships
        for rel in relationships:
            for mod in modifiers:
                graph.Connect( initial, (rel, mod, fx), terminal )


    graph.CloseVertex( R )




###############################################################################
# _new_root_node
#
###############################################################################
def _new_root_node( graph, id ):
    """
    """
    R = graph.NewVertex( id, type="ROOT" )
    R.SetProperty( "level", 0 )
    R.SetProperty( "number", 0 )
    R.SetProperty( "name", id )
    R.SetProperty( "short", "n%d%04d" % (0,0) )
    return R




###############################################################################
# NewFanout
#
###############################################################################
def NewFanout( name, root_node, fanout_factor=10, levels=3, modifiers=[M_INT], relationships=[] ):
    """
    NewFanout
    """
    g = Graph( name )
    g.ClearGraphReadonly()
    g.EventEnable()
    g.Truncate()
    remaining_levels = list(range(1,levels+1))
    R = _new_root_node( g, root_node )
    __build_fanout( g, R.id, remaining_levels=remaining_levels, fanout_factor=fanout_factor, modifiers=modifiers, relationships=relationships )
    return g




###############################################################################
# AppendFanout
#
###############################################################################
def AppendFanout( name, root_node, fanout_factor=10, levels=3, modifiers=[M_INT], relationships=[] ):
    """
    AppendFanout
    """
    g = Graph( name )
    g.ClearGraphReadonly()
    g.EventEnable()
    remaining_levels = list(range(1,levels+1))
    R = _new_root_node( g, root_node )
    __build_fanout( g, R.id, remaining_levels=remaining_levels, fanout_factor=fanout_factor, modifiers=modifiers, relationships=relationships )
    return g




###############################################################################
# NewGeoGraph
#
###############################################################################
def NewGeoGraph( name ):
    """
    Build graph of states and cities
    """

    datadir = os.path.join( os.path.dirname(__file__), "data" )
    cities = os.path.join( datadir, "cities.tsv" )
    states = os.path.join( datadir, "states.tsv" )

    f_cities = open( cities, "r" )
    f_states = open( states, "r" )

    g = Graph( name )
    g.ClearGraphReadonly()
    g.EventEnable()

    COUNTRY = "USA"
    U = g.NewVertex( COUNTRY, type="country" )

    for line in f_states:
        if line.strip():
            STATE, Name = line.strip().split("\t")
            S = g.NewVertex( STATE, type="state" )
            S[ 'name' ] = Name
            g.Connect( U, "state", S )
            g.CloseVertex( S )
    f_states.close()

    for line in f_cities:
        if line.strip():
            Name, STATE, lat_s, lon_s = line.strip().split("\t")
            lat = float( lat_s )
            lon = float( lon_s )
            node = "%s_%s" % (Name.replace("'",""), STATE)
            C = g.NewVertex( node, type="city" )
            S = g.OpenVertex( STATE )
            g.Connect( S, "city", C )
            g.Connect( U, "city", C )
            C[ 'state' ] = S[ 'name' ]
            C[ 'lat' ] = lat
            C[ 'lon' ] = lon
            g.CloseVertex( S )
    f_cities.close()

    g.CloseVertex( U )

    return g




###############################################################################
# AssertCleanStart
#
###############################################################################
def AssertCleanStart():
    """
    Fail if not clean
    """
    n = system.WritableVertices()
    Expect( n == 0,         "Unexpected writable vertex count: %d" % n )
