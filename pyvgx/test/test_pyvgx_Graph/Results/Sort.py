###############################################################################
# 
# VGX Server
# Distributed engine for plugin-based graph and vector search
# 
# Module:  pyvgx.test
# File:    Sort.py
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
from ..Query._query_test_support import *
from pyvgx import *
import pyvgx
from math import *
import operator

graph = None



###############################################################################
# is_sorted_asc
#
###############################################################################
def is_sorted_asc( L ):
    """
    """
    return all( a <= b for a, b in zip(L, L[1:]) )


###############################################################################
# is_sorted_desc
#
###############################################################################
def is_sorted_desc( L ):
    """
    """
    return all( a >= b for a, b in zip(L, L[1:]) )



###############################################################################
# TEST_Neighborhood_sorting
#
###############################################################################
def TEST_Neighborhood_sorting():
    """
    All sort orders for pyvgx.Graph.Neighborhood()
    t_nominal=22
    test_level=3101
    """
    root = "sort_root"
    params_STR = { 'id':root, 'result':R_STR }
    params_LIST = { 'id':root, 'result':R_LIST }
    params_DICT = { 'id':root, 'result':R_DICT }
    levels = 1
    for fanout_factor in [1,10,100,1000,10000]:
        for modifiers in [ [M_INT], [M_INT,M_UINT], [M_FLT], [M_INT,M_FLT], [M_INT,M_UINT,M_FLT] ]:
            g = NewFanout( "fanout", root, fanout_factor=fanout_factor, levels=levels, modifiers=modifiers )

            # Add various arcs to root's terminals from other roots to give terminals a variable number of incident arcs
            for side in range( int( sqrt( fanout_factor ) ) ):
                AppendFanout( "fanout", "side_%d" % side, fanout_factor=side, levels=2, modifiers=modifiers )

            for READONLY in [False, True]:
                if READONLY is True:
                    g.SetGraphReadonly( 60000 )
                for H_x in [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 100, -1]:
                    params_STR['hits'] = H_x
                    params_LIST['hits'] = H_x
                    params_DICT['hits'] = H_x
                    size = fanout_factor * len( modifiers )
                    if H_x > size or H_x < 0:
                        hit_arcs = size
                    else:
                        hit_arcs = H_x
                    if H_x > fanout_factor or H_x < 0:
                        hit_fanout = fanout_factor
                    else:
                        hit_fanout = H_x

                    # S_NONE
                    result = g.Neighborhood( sortby=S_NONE, **params_STR )
                    Expect( len(result) == hit_arcs,                "all arcs" )

                    # S_VAL
                    values = g.Neighborhood( sortby=S_VAL, fields=F_VAL, **params_LIST )
                    values = [x[0] for x in values]
                    Expect( len(values) == hit_arcs,                "all arcs" )
                    Expect( is_sorted_desc( values ),           "values sorted descending" )
                    values_rank = g.Neighborhood( sortby=S_RANK, rank="next.arc.value", fields=F_VAL, **params_LIST )
                    values_rank = [x[0] for x in values_rank]
                    Expect( len(values_rank) == hit_arcs,           "all arcs" )
                    Expect( is_sorted_desc( values_rank ),      "values sorted descending" )

                    for mod in modifiers:
                        values = g.Neighborhood( sortby=S_VAL|S_ASC, arc=("*",D_OUT,mod), fields=F_VAL, **params_LIST )
                        values = [x[0] for x in values]
                        Expect( len( values ) == hit_fanout,     "all arcs of mod = %d" % mod )
                        Expect( is_sorted_asc( values ),            "values sorted ascending" )
                        values_rank = g.Neighborhood( sortby=S_RANK|S_ASC, rank="next.arc.value", arc=("*",D_OUT,mod), fields=F_VAL, **params_LIST )
                        values_rank = [x[0] for x in values_rank]
                        Expect( len( values_rank ) == hit_fanout,    "all arcs of mod = %d" % mod )
                        Expect( is_sorted_asc( values_rank ),           "values sorted ascending" )

                        values = g.Neighborhood( sortby=S_VAL, arc=("*",D_OUT,mod), fields=F_VAL, **params_LIST )
                        values = [x[0] for x in values]
                        Expect( len( values ) == hit_fanout,     "all arcs of mod = %d" % mod )
                        Expect( is_sorted_desc( values ),           "values sorted descending" )
                        values_rank = g.Neighborhood( sortby=S_RANK, rank="next.arc.value", arc=("*",D_OUT,mod), fields=F_VAL, **params_LIST )
                        values_rank = [x[0] for x in values_rank]
                        Expect( len( values_rank ) == hit_fanout,    "all arcs of mod = %d" % mod )
                        Expect( is_sorted_desc( values_rank ),          "values sorted descending" )

                        values = g.Neighborhood( sortby=S_VAL|S_DESC, arc=("*",D_OUT,mod), fields=F_VAL, **params_LIST )
                        values = [x[0] for x in values]
                        Expect( len( values ) == hit_fanout,     "all arcs of mod = %d" % mod )
                        Expect( is_sorted_desc( values ),           "values sorted descending" )
                        values_rank = g.Neighborhood( sortby=S_RANK|S_DESC, rank="next.arc.value", arc=("*",D_OUT,mod), fields=F_VAL, **params_LIST )
                        values_rank = [x[0] for x in values_rank]
                        Expect( len( values_rank ) == hit_fanout,    "all arcs of mod = %d" % mod )
                        Expect( is_sorted_desc( values_rank ),          "values sorted descending" )
                    
                    values = g.Neighborhood( sortby=S_VAL, arc=("*",D_OUT), fields=F_VAL, **params_LIST )
                    values = [x[0] for x in values]
                    Expect( len(values) == hit_arcs,                    "all arcs" )
                    Expect( is_sorted_desc( values ),               "values sorted descending" )
                    values_rank = g.Neighborhood( sortby=S_RANK, rank="next.arc.value", arc=("*",D_OUT), fields=F_VAL, **params_LIST )
                    values_rank = [x[0] for x in values_rank]
                    Expect( len(values_rank) == hit_arcs,               "all arcs" )
                    Expect( is_sorted_desc( values_rank ),          "values sorted descending" )

                    # S_ADDR
                    str_addresses = g.Neighborhood( sortby=S_ADDR, fields=F_ADDR, **params_STR )
                    addresses = [int(x) for x in str_addresses]
                    Expect( len(addresses) == hit_arcs,                 "all arcs" )
                    Expect( is_sorted_asc( addresses ),             "addresses sorted ascending" )
                    str_addresses_rank = g.Neighborhood( sortby=S_RANK, rank="next", fields=F_ADDR, **params_STR )
                    addresses_rank = [int(x) for x in str_addresses_rank]
                    Expect( len(addresses_rank) == hit_arcs,            "all arcs" )
                    Expect( is_sorted_desc( addresses_rank ),       "addresses sorted descending" )

                    # S_OBID
                    internalids = g.Neighborhood( sortby=S_OBID, fields=F_OBID, **params_STR )
                    Expect( len(internalids) == hit_arcs,               "all arcs" )
                    Expect( is_sorted_asc( internalids ),           "internalids sorted ascending" )

                    # S_ANCHOR_OBID
                    internalids = g.Neighborhood( sortby=S_ANCHOR_OBID, fields=F_ANCHOR_OBID, **params_STR )
                    Expect( len(internalids) == hit_arcs,               "all arcs" )
                    Expect( is_sorted_asc( internalids ),           "internalids sorted ascending" )

                    # S_ID
                    names = g.Neighborhood( sortby=S_ID, fields=F_ID, **params_STR )
                    Expect( len(names) == hit_arcs,                     "all arcs" )
                    Expect( is_sorted_asc( names ),                 "names sorted ascending" )

                    # S_ANCHOR
                    names = g.Neighborhood( sortby=S_ANCHOR, fields=F_ANCHOR, **params_STR )
                    Expect( len(names) == hit_arcs,                     "all arcs" )
                    Expect( is_sorted_asc( names ),                 "names sorted ascending" )

                    # S_DEG
                    degrees = g.Neighborhood( sortby=S_DEG, fields=F_DEG, **params_LIST )
                    degrees = [x[0] for x in degrees]
                    Expect( len(degrees) == hit_arcs,                   "all arcs" )
                    Expect( is_sorted_desc( degrees ),              "degrees sorted descending" )
                    degrees_rank = g.Neighborhood( sortby=S_RANK, rank="next.deg", fields=F_DEG, **params_LIST )
                    degrees_rank = [x[0] for x in degrees_rank]
                    Expect( len(degrees_rank) == hit_arcs,              "all arcs" )
                    Expect( is_sorted_desc( degrees_rank ),         "degrees sorted descending" )

                    # S_IDEG
                    indegrees = g.Neighborhood( sortby=S_IDEG, fields=F_IDEG, **params_LIST )
                    indegrees = [x[0] for x in indegrees]
                    Expect( len(indegrees) == hit_arcs,                 "all arcs" )
                    Expect( is_sorted_desc( indegrees ),            "indegrees sorted descending" )
                    indegrees_rank = g.Neighborhood( sortby=S_RANK, rank="next.ideg", fields=F_IDEG, **params_LIST )
                    indegrees_rank = [x[0] for x in indegrees_rank]
                    Expect( len(indegrees_rank) == hit_arcs,            "all arcs" )
                    Expect( is_sorted_desc( indegrees_rank ),       "indegrees sorted descending" )

                    # S_ODEG
                    outdegrees = g.Neighborhood( sortby=S_ODEG, fields=F_ODEG, **params_LIST )
                    outdegrees = [x[0] for x in outdegrees]
                    Expect( len(outdegrees) == hit_arcs,                "all arcs" )
                    Expect( is_sorted_desc( outdegrees ),           "outdegrees sorted descending" )
                    outdegrees_rank = g.Neighborhood( sortby=S_RANK, rank="next.odeg", fields=F_ODEG, **params_LIST )
                    outdegrees_rank = [x[0] for x in outdegrees_rank]
                    Expect( len(outdegrees_rank) == hit_arcs,           "all arcs" )
                    Expect( is_sorted_desc( outdegrees_rank ),      "outdegrees sorted descending" )

                    # TODO: Current test graph has no vectors, add vectors to test sim sorting
                    # S_SIM
                    # S_HAM

                    # S_RANK
                    result = g.Neighborhood( sortby=S_RANK, rank="next[ 'short' ]", select="short", fields=F_PROP, **params_DICT )
                    property_values = [ x['properties']['short'] for x in result ]
                    Expect( is_sorted_desc( property_values ),      "string properties sorted descending" )
                    result = g.Neighborhood( sortby=S_RANK, rank="next[ 'number' ]", select="number", fields=F_PROP, **params_DICT )
                    property_values = [ x['properties']['number'] for x in result ]
                    Expect( is_sorted_desc( property_values ),      "numeric properties sorted descending" )

                    # S_TMC
                    times = g.Neighborhood( sortby=S_TMC, fields=F_TMC, **params_LIST )
                    times = [x[0] for x in times]
                    Expect( is_sorted_asc( times ),                 "TMC sorted ascending" )
                    times_rank = g.Neighborhood( sortby=S_RANK, rank="next.tmc", fields=F_TMC, **params_LIST )
                    times_rank = [x[0] for x in times_rank]
                    Expect( is_sorted_desc( times_rank ),           "TMC sorted descending" )


                    # S_TMM
                    times = g.Neighborhood( sortby=S_TMM, fields=F_TMM, **params_LIST )
                    times = [x[0] for x in times]
                    Expect( is_sorted_asc( times ),                 "TMM sorted ascending" )
                    times_rank = g.Neighborhood( sortby=S_RANK, rank="next.tmm", fields=F_TMM, **params_LIST )
                    times_rank = [x[0] for x in times_rank]
                    Expect( is_sorted_desc( times_rank ),           "TMM sorted descending" )


                    # TODO: No expiration for vertices in this graph
                    # S_TMX

                    # S_NATIVE
                    result = g.Neighborhood( sortby=S_NATIVE, **params_STR )
                    Expect( len(result) == hit_arcs,                "all arcs" )

                    # S_RANDOM
                    if H_x > 10 or H_x < 0:
                        result1 = g.Neighborhood( sortby=S_RANDOM, fields=F_ID, **params_STR )
                        result2 = g.Neighborhood( sortby=S_RANDOM, fields=F_ID, **params_STR )
                        result3 = g.Neighborhood( sortby=S_RANDOM, fields=F_ID, **params_STR )
                        result4 = g.Neighborhood( sortby=S_RANK, rank="random()", fields=F_ID, **params_STR )
                        result5 = g.Neighborhood( sortby=S_RANK, rank="random()", fields=F_ID, **params_STR )
                        result6 = g.Neighborhood( sortby=S_RANK, rank="random()", fields=F_ID, **params_STR )
                        Expect( len(result1) == hit_arcs,               "all arcs" )
                        Expect( len(result2) == hit_arcs,               "all arcs" )
                        Expect( len(result3) == hit_arcs,               "all arcs" )
                        Expect( len(result4) == hit_arcs,               "all arcs" )
                        Expect( len(result5) == hit_arcs,               "all arcs" )
                        Expect( len(result6) == hit_arcs,               "all arcs" )

                        if fanout_factor > 1:
                            Expect( result1 != result2,                 "random order" )
                            Expect( result1 != result3,                 "random order" )
                            Expect( result1 != result4,                 "random order" )
                            Expect( result1 != result5,                 "random order" )
                            Expect( result1 != result6,                 "random order" )
                            Expect( result2 != result3,                 "random order" )
                            Expect( result2 != result4,                 "random order" )
                            Expect( result2 != result5,                 "random order" )
                            Expect( result2 != result6,                 "random order" )
                            Expect( result3 != result4,                 "random order" )
                            Expect( result3 != result5,                 "random order" )
                            Expect( result3 != result6,                 "random order" )
                            Expect( result4 != result5,                 "random order" )
                            Expect( result4 != result6,                 "random order" )
                            Expect( result5 != result6,                 "random order" )
                if READONLY is True:
                    g.ClearGraphReadonly()
    
    g.DebugCheckAllocators()
    g.ClearGraphReadonly()
    g.Truncate()


 

###############################################################################
# TEST_Global_sorting
#
###############################################################################
def TEST_Global_sorting():
    """
    All sort orders for pyvgx.Graph.Vertices() and pyvgx.Graph.Arcs()
    t_nominal=39
    test_level=3101
    """
    root = "sort_root"
    params_STR = { 'result':R_STR }
    params_LIST = { 'result':R_LIST }
    params_DICT = { 'result':R_DICT }
    levels = 1
    for fanout_factor in [1,10,100,1000,10000]:
        for modifiers in [ [M_INT], [M_INT,M_UINT], [M_FLT], [M_INT,M_FLT], [M_INT,M_UINT,M_FLT] ]:
            g = NewFanout( "fanout", root, fanout_factor=fanout_factor, levels=levels, modifiers=modifiers )

            # Add various arcs to root's terminals from other roots to give terminals a variable number of incident arcs
            for side in range( int( sqrt( fanout_factor ) ) ):
                AppendFanout( "fanout", "side_%d" % side, fanout_factor=side, levels=2, modifiers=modifiers )

            for READONLY in [False, True]:
                if READONLY is True:
                    g.SetGraphReadonly( 60000 )

                for H_x in [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 100, -1 ]:
                    params_STR['hits'] = H_x
                    params_LIST['hits'] = H_x
                    params_DICT['hits'] = H_x
                    if H_x > g.order or H_x < 0:
                        hit_vertices = g.order
                    else:
                        hit_vertices = H_x
                    if H_x > g.size or H_x < 0:
                        hit_arcs = g.size
                    else:
                        hit_arcs = H_x

                    # S_NONE
                    # Vertices
                    result = g.Vertices( sortby=S_NONE, **params_STR )
                    Expect( len(result) == hit_vertices,             "all vertices" )
                    # Arcs
                    result = g.Arcs( sortby=S_NONE, **params_STR )
                    Expect( len(result) == hit_arcs,              "all arcs" )


                    # S_ADDR
                    # Vertices
                    str_addresses = g.Vertices( sortby=S_ADDR, fields=F_ADDR, **params_STR )
                    addresses = [int(x) for x in str_addresses]
                    Expect( len(addresses) == hit_vertices,              "all vertices" )
                    Expect( is_sorted_asc( addresses ),             "addresses sorted ascending" )
                    str_addresses_rank = g.Vertices( sortby=S_RANK, rank="vertex", fields=F_ADDR, **params_STR )
                    addresses_rank = [int(x) for x in str_addresses_rank]
                    Expect( len(addresses_rank) == hit_vertices,         "all vertices" )
                    Expect( is_sorted_desc( addresses_rank ),       "addresses sorted descending" )
                    # Arcs
                    str_addresses = g.Arcs( sortby=S_ADDR, fields=F_ADDR, **params_STR )
                    addresses = [int(x) for x in str_addresses]
                    Expect( len(addresses) == hit_arcs,               "all arcs" )
                    Expect( is_sorted_asc( addresses ),             "addresses sorted ascending" )
                    str_addresses_rank = g.Arcs( sortby=S_RANK, rank="next", fields=F_ADDR, **params_STR )
                    addresses_rank = [int(x) for x in str_addresses_rank]
                    Expect( len(addresses_rank) == hit_arcs,          "all arcs" )
                    Expect( is_sorted_desc( addresses_rank ),       "addresses sorted descending" )


                    # S_OBID
                    # Vertices
                    internalids = g.Vertices( sortby=S_OBID, fields=F_OBID, **params_STR )
                    Expect( len(internalids) == hit_vertices,            "all vertices" )
                    Expect( is_sorted_asc( internalids ),           "internalids sorted ascending" )
                    # Arcs
                    internalids = g.Arcs( sortby=S_OBID, fields=F_OBID, **params_STR )
                    Expect( len(internalids) == hit_arcs,             "all arcs" )
                    Expect( is_sorted_asc( internalids ),           "internalids sorted ascending" )


                    # S_ANCHOR_OBID 
                    # Vertices (same as S_OBID in this context)
                    internalids = g.Vertices( sortby=S_ANCHOR_OBID, fields=F_OBID, **params_STR )
                    Expect( len(internalids) == hit_vertices,            "all vertices" )
                    Expect( is_sorted_asc( internalids ),           "internalids sorted ascending" )
                    # Arcs
                    internalids = g.Arcs( sortby=S_ANCHOR_OBID, fields=F_ANCHOR_OBID, **params_STR )
                    Expect( len(internalids) == hit_arcs,             "all arcs" )
                    Expect( is_sorted_asc( internalids ),           "internalids sorted ascending" )


                    # S_ID
                    # Vertices
                    names = g.Vertices( sortby=S_ID, fields=F_ID, **params_STR )
                    Expect( len(names) == hit_vertices,                  "all vertices" )
                    Expect( is_sorted_asc( names ),                 "names sorted ascending" )
                    # Arcs
                    names = g.Arcs( sortby=S_ID, fields=F_ID, **params_STR )
                    Expect( len(names) == hit_arcs,                   "all arcs" )
                    Expect( is_sorted_asc( names ),                 "names sorted ascending" )


                    # S_ANCHOR
                    # Vertices (same as S_ID in this context)
                    names = g.Vertices( sortby=S_ANCHOR, fields=F_ID, **params_STR )
                    Expect( len(names) == hit_vertices,                  "all vertices" )
                    Expect( is_sorted_asc( names ),                 "names sorted ascending" )
                    # Arcs
                    names = g.Arcs( sortby=S_ANCHOR, fields=F_ANCHOR, **params_STR )
                    Expect( len(names) == hit_arcs,                   "all arcs" )
                    Expect( is_sorted_asc( names ),                 "names sorted ascending" )


                    # S_DEG
                    # Vertices
                    degrees = g.Vertices( sortby=S_DEG, fields=F_DEG, **params_LIST )
                    degrees = [x[0] for x in degrees]
                    Expect( len(degrees) == hit_vertices,                "all vertices" )
                    Expect( is_sorted_desc( degrees ),              "degrees sorted descending" )
                    degrees_rank = g.Vertices( sortby=S_RANK, rank="vertex.deg", fields=F_DEG, **params_LIST )
                    degrees_rank = [x[0] for x in degrees_rank]
                    Expect( len(degrees_rank) == hit_vertices,           "all vertices" )
                    Expect( is_sorted_desc( degrees_rank ),         "degrees sorted descending" )
                    # Arcs
                    degrees = g.Arcs( sortby=S_DEG, fields=F_DEG, **params_LIST )
                    degrees = [x[0] for x in degrees]
                    Expect( len(degrees) == hit_arcs,                 "all arcs" )
                    Expect( is_sorted_desc( degrees ),              "degrees sorted descending" )
                    degrees_rank = g.Arcs( sortby=S_RANK, rank="next.deg", fields=F_DEG, **params_LIST )
                    degrees_rank = [x[0] for x in degrees_rank]
                    Expect( len(degrees_rank) == hit_arcs,            "all arcs" )
                    Expect( is_sorted_desc( degrees_rank ),         "degrees sorted descending" )


                    # S_IDEG
                    # Vertices
                    indegrees = g.Vertices( sortby=S_IDEG, fields=F_IDEG, **params_LIST )
                    indegrees = [x[0] for x in indegrees]
                    Expect( len(indegrees) == hit_vertices,              "all vertices" )
                    Expect( is_sorted_desc( indegrees ),            "indegrees sorted descending" )
                    indegrees_rank = g.Vertices( sortby=S_RANK, rank="vertex.ideg", fields=F_IDEG, **params_LIST )
                    indegrees_rank = [x[0] for x in indegrees_rank]
                    Expect( len(indegrees_rank) == hit_vertices,         "all vertices" )
                    Expect( is_sorted_desc( indegrees_rank ),       "indegrees sorted descending" )
                    # Arcs
                    indegrees = g.Arcs( sortby=S_IDEG, fields=F_IDEG, **params_LIST )
                    indegrees = [x[0] for x in indegrees]
                    Expect( len(indegrees) == hit_arcs,               "all arcs" )
                    Expect( is_sorted_desc( indegrees ),            "indegrees sorted descending" )
                    indegrees_rank = g.Arcs( sortby=S_RANK, rank="next.ideg", fields=F_IDEG, **params_LIST )
                    indegrees_rank = [x[0] for x in indegrees_rank]
                    Expect( len(indegrees_rank) == hit_arcs,          "all arcs" )
                    Expect( is_sorted_desc( indegrees_rank ),       "indegrees sorted descending" )


                    # S_ODEG
                    # Vertices
                    outdegrees = g.Vertices( sortby=S_ODEG, fields=F_ODEG, **params_LIST )
                    outdegrees = [x[0] for x in outdegrees]
                    Expect( len(outdegrees) == hit_vertices,             "all vertices" )
                    Expect( is_sorted_desc( outdegrees ),           "outdegrees sorted descending" )
                    outdegrees_rank = g.Vertices( sortby=S_RANK, rank="vertex.odeg", fields=F_ODEG, **params_LIST )
                    outdegrees_rank = [x[0] for x in outdegrees_rank]
                    Expect( len(outdegrees_rank) == hit_vertices,        "all vertices" )
                    Expect( is_sorted_desc( outdegrees_rank ),      "outdegrees sorted descending" )
                    # Arcs
                    outdegrees = g.Arcs( sortby=S_ODEG, fields=F_ODEG, **params_LIST )
                    outdegrees = [x[0] for x in outdegrees]
                    Expect( len(outdegrees) == hit_arcs,              "all arcs" )
                    Expect( is_sorted_desc( outdegrees ),           "outdegrees sorted descending" )
                    outdegrees_rank = g.Arcs( sortby=S_RANK, rank="next.odeg", fields=F_ODEG, **params_LIST )
                    outdegrees_rank = [x[0] for x in outdegrees_rank]
                    Expect( len(outdegrees_rank) == hit_arcs,         "all arcs" )
                    Expect( is_sorted_desc( outdegrees_rank ),      "outdegrees sorted descending" )


                    # TODO: Current test graph has no vectors, add vectors to test sim sorting
                    # S_SIM
                    # S_HAM

                    # S_RANK
                    # Vertices
                    result = g.Vertices( sortby=S_RANK, rank="vertex[ 'short' ]", select="short", fields=F_PROP, **params_DICT )
                    property_values = [ x['properties']['short'] for x in result ]
                    Expect( is_sorted_desc( property_values ),      "string properties sorted descending" )
                    result = g.Vertices( sortby=S_RANK, rank="vertex[ 'number' ]", select="number", fields=F_PROP, **params_DICT )
                    property_values = [ x['properties']['number'] for x in result ]
                    Expect( is_sorted_desc( property_values ),      "numeric properties sorted descending, got %s" % property_values )
                    # Arcs
                    result = g.Arcs( sortby=S_RANK, rank="next[ 'short' ]", select="short", fields=F_PROP, **params_DICT )
                    property_values = [ x['properties']['short'] for x in result ]
                    Expect( is_sorted_desc( property_values ),      "string properties sorted descending" )
                    result = g.Arcs( sortby=S_RANK, rank="next[ 'number' ]", select="number", fields=F_PROP, **params_DICT )
                    property_values = [ x['properties']['number'] for x in result ]
                    Expect( is_sorted_desc( property_values ),      "numeric properties sorted descending, got %s" % property_values )


                    # S_TMC
                    # Vertices
                    times = g.Vertices( sortby=S_TMC, fields=F_TMC, **params_LIST )
                    times = [x[0] for x in times]
                    Expect( is_sorted_asc( times ),                 "TMC sorted ascending" )
                    times_rank = g.Vertices( sortby=S_RANK, rank="vertex.tmc", fields=F_TMC, **params_LIST )
                    times_rank = [x[0] for x in times_rank]
                    Expect( is_sorted_desc( times_rank ),           "TMC sorted descending" )
                    # Arcs
                    times = g.Arcs( sortby=S_TMC, fields=F_TMC, **params_LIST )
                    times = [x[0] for x in times]
                    Expect( is_sorted_asc( times ),                 "TMC sorted ascending" )
                    times_rank = g.Arcs( sortby=S_RANK, rank="next.tmc", fields=F_TMC, **params_LIST )
                    times_rank = [x[0] for x in times_rank]
                    Expect( is_sorted_desc( times_rank ),           "TMC sorted descending" )


                    # S_TMM
                    # Vertices
                    times = g.Vertices( sortby=S_TMM, fields=F_TMM, **params_LIST )
                    times = [x[0] for x in times]
                    Expect( is_sorted_asc( times ),                 "TMM sorted ascending" )
                    times_rank = g.Vertices( sortby=S_RANK, rank="vertex.tmm", fields=F_TMM, **params_LIST )
                    times_rank = [x[0] for x in times_rank]
                    Expect( is_sorted_desc( times_rank ),           "TMM sorted descending" )
                    # Arcs
                    times = g.Arcs( sortby=S_TMM, fields=F_TMM, **params_LIST )
                    times = [x[0] for x in times]
                    Expect( is_sorted_asc( times ),                 "TMM sorted ascending" )
                    times_rank = g.Arcs( sortby=S_RANK, rank="next.tmm", fields=F_TMM, **params_LIST )
                    times_rank = [x[0] for x in times_rank]
                    Expect( is_sorted_desc( times_rank ),           "TMM sorted descending" )


                    # TODO: No expiration for vertices in this graph
                    # S_TMX

                    # S_NATIVE
                    # Vertices
                    result = g.Vertices( sortby=S_NATIVE, **params_STR )
                    Expect( len(result) == hit_vertices,                 "all vertices" )
                    # Arcs
                    result = g.Arcs( sortby=S_NATIVE, **params_STR )
                    Expect( len(result) == hit_arcs,                  "all size" )



                    # S_RANDOM
                    if H_x > 10 or H_x < 0:
                        # Vertices
                        result1 = g.Vertices( sortby=S_RANDOM, fields=F_ID, **params_STR )
                        result2 = g.Vertices( sortby=S_RANDOM, fields=F_ID, **params_STR )
                        result3 = g.Vertices( sortby=S_RANDOM, fields=F_ID, **params_STR )
                        result4 = g.Vertices( sortby=S_RANK, rank="random()", fields=F_ID, **params_STR )
                        result5 = g.Vertices( sortby=S_RANK, rank="random()", fields=F_ID, **params_STR )
                        result6 = g.Vertices( sortby=S_RANK, rank="random()", fields=F_ID, **params_STR )
                        Expect( len(result1) == hit_vertices,               "all vertices" )
                        Expect( len(result2) == hit_vertices,               "all vertices" )
                        Expect( len(result3) == hit_vertices,               "all vertices" )
                        Expect( len(result4) == hit_vertices,               "all vertices" )
                        Expect( len(result5) == hit_vertices,               "all vertices" )
                        Expect( len(result6) == hit_vertices,               "all vertices" )

                        if fanout_factor > 1:
                            Expect( result1 != result2,                 "random order" )
                            Expect( result1 != result3,                 "random order" )
                            Expect( result1 != result4,                 "random order" )
                            Expect( result1 != result5,                 "random order" )
                            Expect( result1 != result6,                 "random order" )
                            Expect( result2 != result3,                 "random order" )
                            Expect( result2 != result4,                 "random order" )
                            Expect( result2 != result5,                 "random order" )
                            Expect( result2 != result6,                 "random order" )
                            Expect( result3 != result4,                 "random order" )
                            Expect( result3 != result5,                 "random order" )
                            Expect( result3 != result6,                 "random order" )
                            Expect( result4 != result5,                 "random order" )
                            Expect( result4 != result6,                 "random order" )
                            Expect( result5 != result6,                 "random order" )

                        # Arcs
                        result1 = g.Arcs( sortby=S_RANDOM, fields=F_ID, **params_STR )
                        result2 = g.Arcs( sortby=S_RANDOM, fields=F_ID, **params_STR )
                        result3 = g.Arcs( sortby=S_RANDOM, fields=F_ID, **params_STR )
                        result4 = g.Arcs( sortby=S_RANK, rank="random()", fields=F_ID, **params_STR )
                        result5 = g.Arcs( sortby=S_RANK, rank="random()", fields=F_ID, **params_STR )
                        result6 = g.Arcs( sortby=S_RANK, rank="random()", fields=F_ID, **params_STR )
                        Expect( len(result1) == hit_arcs,                "all arcs" )
                        Expect( len(result2) == hit_arcs,                "all arcs" )
                        Expect( len(result3) == hit_arcs,                "all arcs" )
                        Expect( len(result4) == hit_arcs,                "all arcs" )
                        Expect( len(result5) == hit_arcs,                "all arcs" )
                        Expect( len(result6) == hit_arcs,                "all arcs" )

                        if fanout_factor > 1:
                            Expect( result1 != result2,                 "random order" )
                            Expect( result1 != result3,                 "random order" )
                            Expect( result1 != result4,                 "random order" )
                            Expect( result1 != result5,                 "random order" )
                            Expect( result1 != result6,                 "random order" )
                            Expect( result2 != result3,                 "random order" )
                            Expect( result2 != result4,                 "random order" )
                            Expect( result2 != result5,                 "random order" )
                            Expect( result2 != result6,                 "random order" )
                            Expect( result3 != result4,                 "random order" )
                            Expect( result3 != result5,                 "random order" )
                            Expect( result3 != result6,                 "random order" )
                            Expect( result4 != result5,                 "random order" )
                            Expect( result4 != result6,                 "random order" )
                            Expect( result5 != result6,                 "random order" )

                if READONLY is True:
                    g.ClearGraphReadonly()

    g.DebugCheckAllocators()
    g.ClearGraphReadonly()
    g.Truncate()


 









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
