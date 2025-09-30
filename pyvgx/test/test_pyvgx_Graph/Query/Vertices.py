###############################################################################
# 
# VGX Server
# Distributed engine for plugin-based graph and vector search
# 
# Module:  pyvgxtest
# File:    Vertices.py
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
import gc






###############################################################################
# TEST_Vertices_basic
#
###############################################################################
def TEST_Vertices_basic():
    """
    pyvgx.Graph.Vertices()
    Basic
    test_level=3101
    """
    QuerySupport.AssertCleanStart()
    Expect( system.WritableVertices() == 0 )
    graph_name = "vertices"
    g = pyvgx.Graph( graph_name )
    g.Truncate()
    for n in range(100):
        g.CreateVertex( "node_%d" % n )
    Expect( len(g.Vertices()) == g.order,               "all vertices" )

    result = g.Vertices( hits=0 )
    Expect( len(result) == 0,                           "hits = 0" )

    result = g.Vertices( hits=1 )
    Expect( len(result) == 1,                           "hits = 1" )

    g.DebugCheckAllocators()




###############################################################################
# TEST_Vertices_condition_filters
#
###############################################################################
def TEST_Vertices_condition_filters():
    """
    pyvgx.Graph.Vertices()
    Condition filters
    t_nominal=10
    test_level=3101
    """

    QuerySupport.AssertCleanStart()

    def INNER():
        graph_name = "vertices"
        root = "root"
        levels = 2

        for fanout_factor in [1,8,24,96]:
            for modifiers in [ [M_INT], [M_INT,M_UINT], [M_FLT], [M_INT,M_FLT], [M_INT,M_UINT,M_FLT] ]:
                g = QuerySupport.NewFanout( graph_name, root, fanout_factor=fanout_factor, levels=levels, modifiers=modifiers )

                ALL_VERTICES_Q = g.NewVerticesQuery()

                REAL_VERTICES_Q         = g.NewVerticesQuery( condition={'virtual':False} )
                VIRTUAL_VERTICES_Q      = g.NewVerticesQuery( condition={'virtual':True} )

                TYPELESS_VERTICES_Q     = g.NewVerticesQuery( condition={'type':None} )
                VERTEX_VERTICES_Q       = g.NewVerticesQuery( condition={'type':"__vertex__"} )
                LEVEL_1_VERTICES_Q      = g.NewVerticesQuery( condition={'type':"level_1"} )
                LEVEL_2_VERTICES_Q      = g.NewVerticesQuery( condition={'type':"level_2"} )
                ROOT_VERTICES_Q         = g.NewVerticesQuery( condition={'type':"ROOT"} )

                DEGREE_EQ_VERTICES_Q_LIST = []
                DEGREE_GT_VERTICES_Q_LIST = []
                DEGREE_LT_VERTICES_Q_LIST = []
                DEGREE_RANGE_VERTICES_Q_LIST = []
                INDEGREE_GT_VERTICES_Q_LIST = []
                OUTDEGREE_GT_VERTICES_Q_LIST = []
                for d in range(10):
                    D_EQ = g.NewVerticesQuery( condition={'degree':d} )
                    D_GT = g.NewVerticesQuery( condition={'degree':(V_GT,d)} )
                    D_LT = g.NewVerticesQuery( condition={'degree':(V_LT,d)} )
                    D_RA = g.NewVerticesQuery( condition={'degree':(V_RANGE,(d,d+10))} )
                    I_GT = g.NewVerticesQuery( condition={'indegree':(V_GT,d)} )
                    O_GT = g.NewVerticesQuery( condition={'outdegree':(V_GT,d)} )
                    DEGREE_EQ_VERTICES_Q_LIST.append( D_EQ )
                    DEGREE_GT_VERTICES_Q_LIST.append( D_GT )
                    DEGREE_LT_VERTICES_Q_LIST.append( D_LT )
                    DEGREE_RANGE_VERTICES_Q_LIST.append( D_RA )
                    INDEGREE_GT_VERTICES_Q_LIST.append( I_GT )
                    OUTDEGREE_GT_VERTICES_Q_LIST.append( O_GT )

                NONEXIST_VERTICES_Q             = g.NewVerticesQuery( condition={ 'id':'nonexist' } )

                AFTER_TMIN_VERTICES_Q           = g.NewVerticesQuery( condition={ 'abstime':{ 'created':( V_GT, T_MIN ) } } )
                BEFORE_TMAX_VERTICES_Q          = g.NewVerticesQuery( condition={ 'abstime':{ 'created':( V_LT, T_MAX ) } } )
                NOW_OR_BEFORE_NOW_VERTICES_Q    = g.NewVerticesQuery( condition={ 'reltime':{ 'created':( V_LTE, 0 ) } } )
                AFTER_NOW_VERTICES_Q            = g.NewVerticesQuery( condition={ 'reltime':{ 'created':( V_GT, 0 ) } } )

                PROP_LEVEL_1_VERTICES_Q         = g.NewVerticesQuery( condition={ 'property':{ 'level':1 } } )
                PROP_LEVEL_ANY_VERTICES_Q       = g.NewVerticesQuery( condition={ 'property':{ 'level':(V_GTE,1) } } )
                PROP_NAME_PREFIX_VERTICES_Q     = g.NewVerticesQuery( condition={ 'property':{ 'name':"level_1*" } } )

                NEIGHBOR_WITH_ARC_TO_LEVEL_1_VERTICES_Q       = g.NewVerticesQuery( condition={ 'neighbor':{ 'arc':("to_level_1",D_OUT) } } )
                NEIGHBOR_WITH_ARC_TO_LEVEL_2_VERTICES_Q       = g.NewVerticesQuery( condition={ 'neighbor':{ 'arc':("to_level_2",D_OUT) } } )
                NEIGHBOR_WITH_INARC_TO_LEVEL_1_VERTICES_Q     = g.NewVerticesQuery( condition={ 'neighbor':{ 'arc':("to_level_1",D_IN) } } )

                ARC_TO_LEVEL_1_VERTICES_Q       = g.NewVerticesQuery( condition={ 'arc':("to_level_1",D_OUT) } )
                INARC_TO_LEVEL_2_VERTICES_Q     = g.NewVerticesQuery( condition={ 'arc':("to_level_2",D_IN) } )


                # Add some typeless vertices and virtual vertices
                for n in range( fanout_factor ):
                    g.CreateVertex( "typeless_%d" % n )
                for v in range( fanout_factor ):
                    g.Connect( "level_2_%d" % v, "to", "virtual_%d" % (v%100) )

                # Add various arcs to root's terminals from other roots to give terminals a variable number of incident arcs
                for side in range( int( sqrt( fanout_factor ) ) ):
                    QuerySupport.AppendFanout( "vertices", "side_%d" % side, fanout_factor=side, levels=2, modifiers=modifiers )

                #ALL_VERTICES_Q.hits = -1;
                all_vertices = ALL_VERTICES_Q.Execute()
                Expect( len(all_vertices) == g.order,               "all vertices" )

                # condition: virtual
                for name in REAL_VERTICES_Q.Execute():
                    Expect( g[name].IsVirtual() == False,           "Only REAL vertices" )
                    Expect( not name.startswith( "virtual_" ),      "Not virtual" )
                for name in VIRTUAL_VERTICES_Q.Execute():
                    Expect( g[name].IsVirtual() == True,            "Only VIRTUAL vertices" )
                    Expect( name.startswith( "virtual_" ),          "virtual" )

                # condition: type
                for name in TYPELESS_VERTICES_Q.Execute():
                    Expect( g[name].type == "__vertex__",           "Only typeless vertices" )
                for name in VERTEX_VERTICES_Q.Execute():
                    Expect( g[name].type == "__vertex__",           "Only typeless vertices" )
                for name in LEVEL_1_VERTICES_Q.Execute():
                    Expect( g[name].type == "level_1",              "Only level_1 vertices" )
                    Expect( name.startswith( "level_1_" ),          "level_1" )
                for name in LEVEL_2_VERTICES_Q.Execute():
                    Expect( g[name].type == "level_2",              "Only level_2 vertices" )
                    Expect( name.startswith( "level_2_" ),          "level_2" )
                for name in ROOT_VERTICES_Q.Execute():
                    Expect( g[name].type == "ROOT",                 "Only ROOT vertex" )

                # condition: degree
                for d in range(10):
                    for name in DEGREE_EQ_VERTICES_Q_LIST[d].Execute():
                        Expect( g[name].degree == d,                "degree = %d" % d )
                    for name in DEGREE_GT_VERTICES_Q_LIST[d].Execute():
                        Expect( g[name].degree > d,                 "degree > %d" % d )
                    for name in DEGREE_LT_VERTICES_Q_LIST[d].Execute():
                        Expect( g[name].degree < d,                 "degree < %d" % d )
                    for name in DEGREE_RANGE_VERTICES_Q_LIST[d].Execute():
                        dg = g[name].degree
                        Expect( dg >= d and dg <= d+10,             "degree in [%d, %d]" % (d,d+10) )

                # condition: indegree
                for d in range(10):
                    for name in INDEGREE_GT_VERTICES_Q_LIST[d].Execute():
                        Expect( g[name].indegree > d,               "indegree > %d" % d )

                # condition: outdegree
                for d in range(10):
                    for name in OUTDEGREE_GT_VERTICES_Q_LIST[d].Execute():
                        Expect( g[name].outdegree > d,              "outdegree > %d" % d )
                    

                # TODO: Add vectors to graph to test similarity
                # condition: similarity

                # condition: id
                result = NONEXIST_VERTICES_Q.Execute()
                Expect( len( result ) == 0,                         "vertex does not exist" )

                for fx in range( fanout_factor ):
                    prefix = "level_1"
                    name = "%s_%d" % (prefix, fx)
                    result = g.Vertices( condition={ 'id':name } )
                    Expect( len( result ) == 1,                     "vertex %s exist" % name )
                    Expect( result[0] == name,                      name )
                    result = g.Vertices( condition={ 'id':pyvgx.strhash128(name) } )
                    Expect( len( result ) == 1,                     "vertex %s by its internalid exist" % name )
                    Expect( result[0] == name,                      name )
                    result = g.Vertices( condition={ 'id':"%s*" % prefix } )
                    if len( result ) != fanout_factor:
                        print("fanout_factor", fanout_factor)
                        print("modifiers", modifiers)
                        print("fx", fx)
                        print("len( result )", len( result ))
                        g.Save( 60000 )

                    Expect( len( result ) == fanout_factor,         "all with prefix %s" % prefix )

                # condition: abstime
                V = g.NewVertex( "vertex_created_now" )
                now = V.TMC
                del V

                result = AFTER_TMIN_VERTICES_Q.Execute()
                Expect( len(result) == g.order,                     "all vertices created after T_MIN" )

                result = BEFORE_TMAX_VERTICES_Q.Execute()
                Expect( len(result) == g.order,                     "all vertices created before T_MAX" )

                result = set( g.Vertices( condition={ 'abstime':{ 'created':( V_EQ, now) } } ) )
                Expect( "vertex_created_now" in result,             "includes just created vertex" )

                result = set( g.Vertices( condition={ 'abstime':{ 'created':( V_NEQ, now) } } ) )
                Expect( "vertex_created_now" not in result,         "excludes just created vertex" )

                one = all_vertices[0]
                tmm = g[one].TMM
                result = set( g.Vertices( condition={ 'abstime':{ 'modified':( V_EQ, tmm ) } } ) )
                Expect( one in result,                              "includes the modified vertex" )
                result = set( g.Vertices( condition={ 'abstime':{ 'modified':( V_NEQ, tmm ) } } ) )
                Expect( one not in result,                          "excludes the modified vertex" )

                # condition: reltime
                result = g.Vertices( condition={ 'reltime':{ 'created':( V_LTE, 0 ) } } )
                Expect( len(result) == g.order,                     "all vertices created now or before now" )

                result = g.Vertices( condition={ 'reltime':{ 'created':( V_GT, 0 ) } } )
                Expect( len(result) == 0,                           "no vertices created after now" )

                # condition: property
                result = PROP_LEVEL_1_VERTICES_Q.Execute()
                Expect( len(result) == fanout_factor,               "all level_1" )

                result = PROP_LEVEL_ANY_VERTICES_Q.Execute()
                n = fanout_factor + 2*fanout_factor
                Expect( len(result) == n,                           "all level_1 and level_2" )

                result = PROP_NAME_PREFIX_VERTICES_Q.Execute()
                Expect( len(result) == fanout_factor,               "all level_1" )

                # condition: neighbor
                # we want nodes that have neighbors (either side) going OUT to something via rel "to_level_1", 
                # i.e. only level_1 nodes since roots are the only ones with this outbound relationship
                for name in NEIGHBOR_WITH_ARC_TO_LEVEL_1_VERTICES_Q.Execute():
                    Expect( name.startswith( "level_1_" ),          "all level_1" )

                # we want nodes that have neighbors (either side) going OUT to something via rel "to_level_2",
                # i.e. both ROOT nodes and level_2 nodes since these nodes both have level_1 as neighbors
                # and level_1 goes out to level 2
                for name in NEIGHBOR_WITH_ARC_TO_LEVEL_2_VERTICES_Q.Execute():
                    V = g[name]
                    Expect( V.type in ["ROOT", "level_2"],          "root and all level_2" )

                # we want nodes that have neighbors (either side) arrived IN at via rel "to_level_1", 
                # i.e. both ROOT nodes and level_2 nodes since these nodes both have level_1 as neighbors
                # and level_1 is arrived IN at via "to_level_1"
                for name in NEIGHBOR_WITH_INARC_TO_LEVEL_1_VERTICES_Q.Execute():
                    V = g[name]
                    Expect( V.type in ["ROOT", "level_2"],          "root and all level_2" )

                # condition: arc
                # we want nodes that have arcs going out via "to_level_1", i.e. root nodes
                for name in ARC_TO_LEVEL_1_VERTICES_Q.Execute():
                    V = g[name]
                    Expect( V.type == "ROOT",                       "only roots" )

                # we want nodes that have arcs arriving in via "to_level_2", i.e. level 2 nodes
                for name in INARC_TO_LEVEL_2_VERTICES_Q.Execute():
                    Expect( name.startswith( "level_2_" ),          "level 2 nodes" )


                g.CloseVertex(V)
        return g

    g = INNER()
    gc.collect()
    g.Erase()




###############################################################################
# TEST_Vertices_vector_filters
#
###############################################################################
def TEST_Vertices_vector_filters():
    """
    pyvgx.Graph.Vertices()
    Vector filters
    test_level=3101
    """
    QuerySupport.AssertCleanStart()
    pass # TODO




###############################################################################
# TEST_Vertices_all_params
#
###############################################################################
def TEST_Vertices_all_params():
    """
    pyvgx.Graph.Vertices()
    All parameters
    t_nominal=61
    test_level=3102
    """
    QuerySupport.AssertCleanStart()
    graph_name = "vertices"
    g = Graph( graph_name )

    N = 10000

    for n in range( N ):
        name = "node_%d" % n
        V = g.NewVertex( name )
        V['number'] = n
        V['name'] = name
        g.Connect( "root", ("to",M_INT,n), V )
        g.CloseVertex( V )

    now = int( time.time() )
    g.Define( "all_params := !vertex.virtual && vertex.type == '*' && vertex.deg >= 0 && vertex.ideg >= 0 && vertex.odeg >= 0 && vertex.id == 'node*' && vertex.tmc > %d && vertex[ 'name' ] == 'node*'" % (now-1000) )
    

    def check_timing( result ):
        Expect( type(result) is dict )
        Expect( "time" in result )
        timing = result["time"]
        Expect( "total" in timing )
        Expect( "search" in timing )
        Expect( "result" in timing )
        t_total = timing['total']
        t_search = timing['search']
        t_result = timing['result']
        if t_total + 1e-6 < t_search + t_result:
            Expect( False, "t_total=%f t_search=%f t_result=%f, total should be >= %f" % (t_total, t_search, t_result, t_search+t_result) )

    def check_counts( result ):
        Expect( type(result) is dict )
        Expect( "counts" in result )
        counts = result['counts']
        Expect( "vertices" in counts )
        vertices = counts['vertices']
        Expect( type(vertices) is int )

    # don't crash
    for RES in [R_STR, R_LIST, R_DICT, R_STR|R_METAS, R_LIST|R_METAS, R_DICT|R_METAS]:
        for FLD in [F_ID, F_ALL]:
            for SORT in [S_NONE, S_ID, S_RANK, S_ADDR, S_NATIVE, S_DEG, S_RANDOM]:
                VERTICES_Q = g.NewVerticesQuery( 
                                    condition = {
                                        'virtual'   : False,
                                        'type'      : None,
                                        'degree'    : (V_GTE,0),
                                        'indegree'  : (V_GTE,0),
                                        'outdegree' : (V_GTE,0),
                                        #TODO: 'similarity'
                                        'id':'node*',
                                        'abstime'   : { 'created':(V_GT, T_MIN) },
                                        'reltime'   : { 'created':(V_GT, -1000) },
                                        'property'  : { 'name':'node*' },
                                        'adjacent'  : {
                                            'arc'       : ( "to", D_IN ),
                                            'neighbor'  : { 'id':'root' }
                                        }
                                    },
                                    vector=[ ("aaa",2), ("bbb",1), ("ccc",0.5) ],
                                    result=RES,
                                    fields=FLD,
                                    select="*",
                                    rank="vertex[ 'number' ]",
                                    sortby=SORT
                                    #timeout=1000
                                  )
                VERTICES_FILTER_Q = g.NewVerticesQuery(     
                                        condition = { 
                                            'filter'   : "all_params",
                                            'adjacent' : {
                                                'arc'      : ( "to", D_IN ),
                                                'neighbor' : {
                                                    'filter' : "prev.arc.type == 'to' && vertex.id == 'root'"
                                                }
                                            }
                                        },
                                        vector=[ ("aaa",2), ("bbb",1), ("ccc",0.5) ],
                                        result=RES,
                                        fields=FLD,
                                        select="*",
                                        rank="vertex[ 'number' ]",
                                        sortby=SORT,
                                        #timeout=1000
                                      )

                for hits in [0, 1, 50, -1]:
                    for offset in [0, 10]:
                        result_Q = VERTICES_Q.Execute( hits=hits, offset=offset, timeout=1000 )

                        result_imm = g.Vertices( 
                                        condition = {
                                            'virtual'   : False,
                                            'type'      : None,
                                            'degree'    : (V_GTE,0),
                                            'indegree'  : (V_GTE,0),
                                            'outdegree' : (V_GTE,0),
                                            #TODO: 'similarity'
                                            'id':'node*',
                                            'abstime'   : { 'created':(V_GT, T_MIN) },
                                            'reltime'   : { 'created':(V_GT, -1000) },
                                            'property'  : { 'name':'node*' },
                                            'adjacent'  : {
                                                'arc'       : ( "to", D_IN ),
                                                'neighbor'  : { 'id':'root' }
                                            }
                                        },
                                        vector=[ ("aaa",2), ("bbb",1), ("ccc",0.5) ],
                                        result=RES,
                                        fields=FLD,
                                        select="*",
                                        rank="vertex[ 'number' ]",
                                        sortby=SORT,
                                        offset=offset,
                                        hits=hits,
                                        timeout=1000
                                    )

                        result_filter_Q = VERTICES_FILTER_Q.Execute( hits=hits, offset=offset, timeout=1000 )

                        result_filter_imm = g.Vertices( 
                                        condition = { 
                                            'filter'   : "all_params",
                                            'adjacent' : {
                                                'arc'      : ( "to", D_IN ),
                                                'neighbor' : {
                                                    'filter' : "prev.arc.type == 'to' && vertex.id == 'root'"
                                                }
                                            }
                                        },
                                        vector=[ ("aaa",2), ("bbb",1), ("ccc",0.5) ],
                                        result=RES,
                                        fields=FLD,
                                        select="*",
                                        rank="vertex[ 'number' ]",
                                        sortby=SORT,
                                        offset=offset,
                                        hits=hits,
                                        timeout=1000
                                    )

                        # Check metas
                        if RES & R_TIMING:
                            check_timing( result_Q )
                            check_timing( result_imm )
                            check_timing( result_filter_Q )
                            check_timing( result_filter_imm )

                        if RES & R_COUNTS:
                            check_counts( result_Q )
                            check_counts( result_imm )
                            check_counts( result_filter_Q )
                            check_counts( result_filter_imm )

                        if hits < 0:
                            expect_count = N - offset
                        else:
                            expect_count = hits

                        if type( result_Q ) is dict:
                            result_Q = result_Q['vertices']
                        if type( result_imm ) is dict:
                            result_imm = result_imm['vertices']
                        if type( result_filter_Q ) is dict:
                            result_filter_Q = result_filter_Q['vertices']
                        if type( result_filter_imm ) is dict:
                            result_filter_imm = result_filter_imm['vertices']

                        # Memsort is the only stable sort
                        if SORT == S_ADDR:
                            if result_Q != result_filter_Q:
                                print(result_Q)
                                print(result_filter_Q)

                            Expect( result_Q == result_imm,                 "Query object vs immediate equivalent" )
                            Expect( result_filter_Q == result_filter_imm,   "Query object vs immediate equivalent" )
                            Expect( result_Q == result_filter_Q,            "Filter syntax equivalent" )

                        Expect( len(result_Q) == expect_count,            "%d nodes, got %d" % (expect_count, len(result_Q)) )

    del VERTICES_Q
    del VERTICES_FILTER_Q
    g.Erase()




###############################################################################
# TEST_Vertices_all_name_lengths
#
###############################################################################
def TEST_Vertices_all_name_lengths():
    """
    Test full range of lengths of vertex names and vertex properties
    t_nominal=1163
    test_level=3102
    """

    QuerySupport.AssertCleanStart()

    def INNER():
        graph_name = "vertices"
        g = Graph( graph_name )

        MIN = 1
        try:
            for n in list(range(MIN,2000)) + list(range(2000,20000,20)) + list(range(20000,130000,100)) + list(range(pyvgx.MAX_STRING-100,pyvgx.MAX_STRING+2)):
                name = "x" * n
                V = g.NewVertex( name, type="node" )
                V['name'] = name
                V['n'] = len( name )
                g.CloseVertex( V )
        except pyvgx.VertexError:
            # Magic limit for vertex name length is 131000
            Expect( len( name ) == pyvgx.MAX_STRING+1 )
            Expect( g.size == 0 )       # no arcs

        ORDER = g.order
        g.CloseAll()

        #
        #
        #
        # TEST TEST TEST
        try:
            op.Fence( 1000 * 5 * 60 )
        except Exception as fence_error:
            wc = system.WritableVertices()
            print( "System writable vertices: %d" % system.WritableVertices() )
            print( "Graph: SYSTEM Open: %s" % system.System().GetOpenVertices() )
            for name in system.Registry().keys():
                instance = Graph( name )
                LIST = instance.GetOpenVertices()
                print( "Graph: %s Open: %s" % (name, LIST) )
            Expect( False,  "Fence failed: %s" % fence_error )

        # Make sure we can save it
        g.Save( 60000 )
        g.Close()
        del g
        system.DeleteGraph( graph_name, timeout=2000 )

        # Make sure we can restore it
        g = Graph( graph_name )
        Expect( g.order == ORDER )              # same order
        Expect( g.size == 0 )                   # no arcs


        LONGEST_NAME_Q  = g.NewVerticesQuery( fields=F_ID, sortby=S_RANK|S_DESC, rank="strlen( vertex.id )" )
        SHORTEST_NAME_Q = g.NewVerticesQuery( fields=F_ID, sortby=S_RANK|S_ASC, rank="strlen( vertex.id )" )

        LONGEST_PROPERTY_Q  = g.NewVerticesQuery( fields=F_ID, sortby=S_RANK|S_DESC, rank="strlen( vertex['name'] )" )
        SHORTEST_PROPERTY_Q  = g.NewVerticesQuery( fields=F_ID, sortby=S_RANK|S_ASC, rank="strlen( vertex['name'] )" )

        # Queries
        for READONLY in [False, True]:
            if READONLY:
                g.SetGraphReadonly( 60000 )

            # Get longest name
            result = LONGEST_NAME_Q.Execute( hits=1 )
            sz = len( result[0] )
            Expect( sz == pyvgx.MAX_STRING,        "%d, got %d" % (pyvgx.MAX_STRING, sz) )

            # Get shortest name
            result = SHORTEST_NAME_Q.Execute( hits=1 )
            sz = len( result[0] )
            Expect( sz == MIN,        "%d, got %d" % (MIN, sz) )

            # Get longest property
            result = LONGEST_PROPERTY_Q.Execute( hits=1 )
            sz = len( result[0] )
            Expect( sz == pyvgx.MAX_STRING,        "%d, got %d" % (pyvgx.MAX_STRING, sz) )

            # Get shortest property
            result = SHORTEST_PROPERTY_Q.Execute( hits=1 )
            sz = len( result[0] )
            Expect( sz == MIN,        "%d, got %d" % (MIN, sz) )

            # Get single hit by placing specific constraint
            for n in list(range( 1, 129)) + [1000, 10000, pyvgx.MAX_STRING-1, pyvgx.MAX_STRING]:
                result = g.Vertices( hits=2, select=".id; name; n", result=R_DICT, condition={ 'filter':"strlen( vertex['name'] ) == vertex['n'] && vertex['name'] == vertex.id && vertex['n'] == %d" % n } )
                sz = len( result )
                Expect( sz == 1 ,           "1, got %d" % sz )
                P = result[0]['properties']
                sz_id = len( P[".id"] )
                sz_name = len( P["name"] )
                val_n = P["n"]
                Expect( sz_id == n,         "%d, got %d" % (n, sz_id) )
                Expect( sz_name == n,       "%d, got %d" % (n, sz_name) )
                Expect( val_n == n,         "%d, got %d" % (n, val_n) )

            # Return all, default field
            result = g.Vertices()
            sz = len( result )
            Expect( sz == ORDER,            "%d, got %d" % (ORDER, sz) )

            # Return all, all fields
            for R_x in [R_SIMPLE, R_STR, R_LIST, R_DICT]:
                result = g.Vertices( result=R_x, fields=F_ALL, condition={ 'filter':"strlen( vertex.id ) > %d" % (pyvgx.MAX_STRING-50) } )
                sz = len( result )
                Expect( sz == 50,      "%d, got %d" % (50, sz) )

            if READONLY:
                g.ClearGraphReadonly()
        return g

    g = INNER()
    gc.collect()
    g.Erase()





###############################################################################
# Run
#
###############################################################################
def Run( name ):
    """
    """
    RunTests( [__name__] )
