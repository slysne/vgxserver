###############################################################################
# 
# VGX Server
# Distributed engine for plugin-based graph and vector search
# 
# Module:  pyvgx
# File:    Arcs.py
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
from . import _query_test_support as QuerySupport
from pyvgx import *
import pyvgx
from math import *
import time



###############################################################################
# TEST_Arcs_basic
#
###############################################################################
def TEST_Arcs_basic():
    """
    pyvgx.Graph.Arcs()
    Basic
    t_nominal=7
    test_level=3201
    """
    g = pyvgx.Graph( "arcs" )
    g.Truncate()

    ALL_ARCS_Q = g.NewArcsQuery()

    for N in range(1, 100):
        for a in range( N ):
            init = "node_%d" % a
            g.Connect( "root", "to", init )
            for b in range( N ):
                term = "node_%d" % b
                g.Connect( init, "to", term )

        # All hits
        #ALL_ARCS_Q.hits = -1
        Expect( len( ALL_ARCS_Q.Execute() ) == g.size,                    "all arcs" )

        # Zero hits
        #ALL_ARCS_Q.hits = 0
        result = ALL_ARCS_Q.Execute( hits=0 )
        Expect( len(result) == 0,                           "hits = 0, got %d" % len(result) )

        # One hit
        #ALL_ARCS_Q.hits = 1
        result = ALL_ARCS_Q.Execute( hits=1 )
        Expect( len(result) == 1,                           "hits = 1, got %d" % len(result) )

        for R_x in [R_SIMPLE, R_STR, R_LIST, R_DICT]:
            for F_x in [F_NONE, F_ANCHOR, F_AARC, F_ID, F_DEG, F_PROP]:
                ARCS_Q = g.NewArcsQuery( result=R_x, fields=F_x )
                # All hits
                result = ARCS_Q.Execute()
                Expect( len(result) == g.size,              "all arcs" )
                # 1 hit
                #ARCS_Q.hits = 1
                result = ARCS_Q.Execute( hits=1 )
                Expect( len(result) == 1,                   "hits = 1, got %d" % len(result) )
                # With sorting
                result = g.Arcs( result=R_x, fields=F_x, hits=1, sortby=S_DEG )
                Expect( len(result) == 1,                   "hits = 1, got %d" % len(result) )

    g.DebugCheckAllocators()




###############################################################################
# GetArcHeads
#
###############################################################################
def GetArcHeads( g, filter ):
    """
    """
    names = []
    ARCS_Q = g.NewArcsQuery( condition={ 'traverse':{ 'filter':filter } }, result=R_DICT )
    for arc in ARCS_Q.Execute():
        Expect( 'arc' in arc )
        Expect( 'id' in arc )
        names.append( arc['id'] )
    return names



###############################################################################
# TEST_Arcs_condition_filters
#
###############################################################################
def TEST_Arcs_condition_filters():
    """
    pyvgx.Graph.Arcs()
    Condition filters
    t_nominal=31
    test_level=3202
    """
    root = "root"
    levels = 2
    for fanout_factor in [1,8,24,96]:
        for modifiers in [ [M_INT], [M_INT,M_UINT], [M_FLT], [M_INT,M_FLT], [M_INT,M_UINT,M_FLT] ]:
            g = QuerySupport.NewFanout( "arcs", root, fanout_factor=fanout_factor, levels=levels, modifiers=modifiers )

            # Add some typeless vertices and virtual vertices
            for n in range( fanout_factor ):
                g.CreateVertex( "typeless_%d" % n )
            for v in range( fanout_factor ):
                g.Connect( "level_2_%d" % v, "to", "virtual_%d" % (v%100) )

            # Add various arcs to root's terminals from other roots to give terminals a variable number of incident arcs
            for side in range( int( sqrt( fanout_factor ) ) ):
                QuerySupport.AppendFanout( "arcs", "side_%d" % side, fanout_factor=side, levels=2, modifiers=modifiers )


            all_arcs = g.Arcs( result=R_DICT )
            Expect( len(all_arcs) == g.size,                    "all arcs" )


            # condition: virtual
            result = GetArcHeads( g, "!next.virtual" )
            for name in result:
                Expect( g[name].IsVirtual() == False,           "Only REAL vertices" )
                Expect( not name.startswith( "virtual_" ),      "Not virtual" )
            result = GetArcHeads( g, "next.virtual" )
            for name in result:
                Expect( g[name].IsVirtual() == True,            "Only VIRTUAL vertices" )
                Expect( name.startswith( "virtual_" ),          "virtual" )

            # condition: type
            result = GetArcHeads( g, "next.type == '__vertex__'" )
            for name in result:
                Expect( g[name].type == "__vertex__",           "Only typeless vertices" )
                Expect( name.startswith( "virtual_" ),          "virtual" )
            result = GetArcHeads( g, "next.type == 'level_1'" )
            for name in result:
                Expect( g[name].type == "level_1",              "Only level_1 vertices" )
                Expect( name.startswith( "level_1_" ),          "level_1" )
            result = GetArcHeads( g, "next.type == 'level_2'" )
            for name in result:
                Expect( g[name].type == "level_2",              "Only level_2 vertices" )
                Expect( name.startswith( "level_2_" ),          "level_2" )
            result = GetArcHeads( g, "next.type == 'ROOT'" )
            for name in result:
                Expect( g[name].type == "ROOT",                 "Only ROOT vertex" )

            # condition: degree
            for d in range(10):

                #code.interact(local=locals())

                result = GetArcHeads( g, "next.deg == %d" % d )
                for name in result:
                    Expect( g[name].degree == d,                "degree = %d" % d )
                result = GetArcHeads( g, "next.deg > %d" % d )
                for name in result:
                    Expect( g[name].degree > d,                 "degree > %d" % d )
                result = GetArcHeads( g, "next.deg < %d" % d )
                for name in result:
                    Expect( g[name].degree < d,                 "degree < %d" % d )
                result = GetArcHeads( g, "next.deg in range( %d, %d )" % (d, d+10) )
                for name in result:
                    dg = g[name].degree
                    Expect( dg >= d and dg <= d+10,             "degree in [%d, %d]" % (d,d+10) )

            # condition: indegree
            for d in range(10):
                result = GetArcHeads( g, "next.ideg > %d" % d )
                for name in result:
                    Expect( g[name].indegree > d,               "indegree > %d" % d )

            # condition: outdegree
            for d in range(10):
                result = GetArcHeads( g, "next.odeg > %d" % d )
                for name in result:
                    Expect( g[name].outdegree > d,              "outdegree > %d" % d )
                

            # TODO: Add vectors to graph to test similarity
            # condition: similarity

            # condition: id
            result = GetArcHeads( g, "next.id == 'nonexist'" )
            Expect( len( result ) == 0,                         "vertex does not exist" )

            for fx in range( fanout_factor ):
                prefix = "level_1"
                name = "%s_%d" % (prefix, fx)
                result = GetArcHeads( g, "next.id == '%s'" % name )
                Expect( len( result ) >= 1,                     "vertex %s exist" % name )
                Expect( result[0] == name,                      name )
                #----
                # TODO: This does not work at the moment
                #result = GetArcHeads( g, "next.internalid == '%s'" % pyvgx.strhash128(name) )
                #Expect( len( result ) == 1,                     "vertex %s by its internalid exist" % name )
                #Expect( result[0] == name,                      name )
                #----
                result = GetArcHeads( g, "next.id == '%s*'" % prefix )
                Expect( len( result ) >= fanout_factor,         "all with prefix %s" % prefix )

            # condition: abstime
            V = g.NewVertex( "vertex_created_now" )
            now = V.TMC
            g.Connect( root, "to", V )
            del V

            result = GetArcHeads( g, "next.tmc > T_MIN" )
            Expect( len(result) == g.size,                      "all arcs with terminal created after T_MIN" )

            result = GetArcHeads( g, "next.tmc < T_MAX" )
            Expect( len(result) == g.size,                      "all arcs with terminals created before T_MAX" )

            result = GetArcHeads( g, "next.tmc == %d" % now )
            Expect( "vertex_created_now" in result,             "includes just created vertex" )

            result = GetArcHeads( g, "next.tmc != %d" % now )
            Expect( "vertex_created_now" not in result,         "excludes just created vertex" )

            one = all_arcs[0]['id']
            tmm = g[one].TMM
            result = GetArcHeads( g, "next.tmm == %d" % tmm )
            Expect( one in result,                              "includes the modified vertex" )
            result = set( GetArcHeads( g, "next.tmm != %d" % tmm ) )
            Expect( one not in result,                          "excludes the modified vertex" )

            # condition: property
            result = GetArcHeads( g, "next['level'] == 1" )
            Expect( len(result) >= fanout_factor,               "all level_1" )

            result = GetArcHeads( g, "next['level'] >= 1" )
            n = fanout_factor + 2*fanout_factor
            Expect( len(result) >= n,                           "all level_1 and level_2" )

            result = GetArcHeads( g, "next['name'] == 'level_1*'" )
            Expect( len(result) >= fanout_factor,               "all level_1" )

            # condition: neighbor
            # we want nodes that have neighbors (either side) going OUT to something via rel "to_level_1", 
            # i.e. only level_1 nodes since roots are the only ones with this outbound relationship
            result = g.Arcs( result=R_DICT, condition={ 'neighbor':{ 'arc':("to_level_1",D_OUT) } } )
            for arc in result:
                name = arc['anchor']
                Expect( name.startswith( "level_1_" ),          "all level_1" )

            # we want nodes that have neighbors (either side) going OUT to something via rel "to_level_2",
            # i.e. both ROOT nodes and level_2 nodes since these nodes both have level_1 as neighbors
            # and level_1 goes out to level 2
            result = g.Arcs( result=R_DICT, condition={ 'neighbor':{ 'arc':("to_level_2",D_OUT) } } )
            for arc in result:
                name = arc['anchor']
                V = g[name]
                Expect( V.type in ["ROOT", "level_2"],          "root and all level_2" )

            # we want nodes that have neighbors (either side) arrived IN at via rel "to_level_1", 
            # i.e. both ROOT nodes and level_2 nodes since these nodes both have level_1 as neighbors
            # and level_1 is arrived IN at via "to_level_1"
            result = g.Arcs( result=R_DICT, condition={ 'neighbor':{ 'arc':("to_level_1",D_IN) } } )
            for arc in result:
                name = arc['anchor']
                V = g[name]
                Expect( V.type in ["ROOT", "level_2"],          "root and all level_2" )

            # condition: arc
            # we want nodes that have arcs going out via "to_level_1", i.e. root nodes
            result = g.Arcs( result=R_DICT, condition={ 'arc':("to_level_1",D_OUT) } )
            for arc in result:
                name = arc['anchor']
                V = g[name]
                Expect( V.type == "ROOT",                       "only roots" )

            # we want nodes that have arcs arriving in via "to_level_2", i.e. level 2 nodes
            result = g.Arcs( result=R_DICT, condition={ 'arc':("to_level_2",D_IN) } )
            for arc in result:
                name = arc['anchor']
                Expect( name.startswith( "level_2_" ),          "level 2 nodes" )


            g.CloseVertex(V)
    g.DebugCheckAllocators()
    g.Truncate()






###############################################################################
# Run
#
###############################################################################
def Run( name ):
    """
    """
    RunTests( [__name__] )
