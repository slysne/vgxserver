###############################################################################
# 
# VGX Server
# Distributed engine for plugin-based graph and vector search
# 
# Module:  pyvgx
# File:    FilterExpressions.py
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
from ..Query._query_test_support import *
from pyvgx import *
import pyvgx
from math import *
import operator
import random
from pytest.threads import Worker
import threading

graph = None




###############################################################################
# TEST_FilterExpressions_local
#
###############################################################################
def TEST_FilterExpressions_local():
    """
    Local filters
    t_nominal=15
    test_level=3201
    """
    root = "local_filter_root"
    levels = 3
    for fanout_factor in [1,4,16,64]:
        for modifiers in [ [M_INT], [M_INT,M_UINT], [M_FLT], [M_INT,M_FLT], [M_INT,M_UINT,M_FLT] ]:
            g = NewFanout( "fanout", root, fanout_factor=fanout_factor, levels=levels, modifiers=modifiers )

            # Add various arcs to root's terminals from other roots to give terminals a variable number of incident arcs
            for side in range( int( sqrt( fanout_factor ) ) ):
                AppendFanout( "fanout", "side_%d" % side, fanout_factor=side, levels=2, modifiers=modifiers )

            print("%d %s %s" % (fanout_factor, modifiers, g))

            for H_x in [0, 1, 2, 10, -1]:
                for S_x in [S_NONE, S_VAL, S_ID, S_DEG, S_ADDR, S_RANK]:
                    print(fanout_factor, modifiers, H_x, S_x)
                    
                    params = { 'id':root, 'hits':H_x, 'sortby':S_x, 'fields':F_AARC }

                    if S_x == S_RANK:
                        params['rank'] = "sin( .arc.value )"

                    # Basic
                    r1 = g.Neighborhood( **params )
                    r2 = g.Neighborhood( filter="1", **params )
                    r3 = g.Neighborhood( filter="0", **params )
                    r4 = g.Neighborhood( filter="1", post="1", **params )
                    r5 = g.Neighborhood( filter="1", post="0", **params )
                    r6 = g.Neighborhood( filter="0", post="1", **params )
                    Expect( r1 == r2 )
                    Expect( len(r3) == 0 )
                    Expect( r1 == r4 )
                    Expect( r1 == r5 )
                    Expect( r6 == r3 )

                    # No traversal in local filters

                    #try:
                    #    r1 = g.Neighborhood( filter=".arc.value > 0", **params )
                    #    Expect( False, "traversal not allowed in local" )
                    #except:
                    #    Expect( True )


                    try:
                        r1 = g.Neighborhood( post=".arc.value > 0", **params )
                        Expect( False, "traversal not allowed in post-filter" )
                    except:
                        Expect( True )

                    # Deep
                    r1 = g.Neighborhood( **params )
                    r2 = g.Neighborhood( neighbor={'filter':"1"}, **params )
                    r3 = g.Neighborhood( neighbor={'filter':"0"}, **params )
                    r4 = g.Neighborhood( neighbor={'post':"1"}, **params )
                    r5 = g.Neighborhood( neighbor={'post':"0"}, **params )
                    r6 = g.Neighborhood( neighbor={'filter':"1", 'post':"1"}, **params )
                    r7 = g.Neighborhood( neighbor={'filter':"1", 'post':"0"}, **params )
                    r8 = g.Neighborhood( neighbor={'filter':"0", 'post':"1"}, **params )
                    r9 = g.Neighborhood( neighbor={'filter':"0", 'post':"0"}, **params )

                    Expect( r1 == r2 )
                    Expect( len(r3) == 0 )
                    Expect( r1 == r4 )
                    Expect( len(r5) == 0 )
                    Expect( r1 == r6 )
                    Expect( len(r7) == 0 )
                    Expect( len(r8) == 0 )
                    Expect( len(r9) == 0 )

                    # No traversal in local filters
                    try:
                        r1 = g.Neighborhood( neighbor={ 'filter':".arc.value > 0" }, **params )
                        Expect( False, "traversal not allowed in local" )
                    except:
                        Expect( True )
                    try:
                        r1 = g.Neighborhood( neighbor={ 'post':".arc.value > 0" }, **params )
                        Expect( False, "traversal not allowed in post-filter" )
                    except:
                        Expect( True )

                    # Deep collect
                    r0 = g.Neighborhood( neighbor={ 'collect':C_COLLECT }, **params )
                    r1 = g.Neighborhood( collect=C_NONE, neighbor={ 'collect':C_COLLECT }, **params )
                    r2 = g.Neighborhood( collect=C_NONE, neighbor={ 'filter':"1", 'collect':C_COLLECT }, **params )
                    r3 = g.Neighborhood( collect=C_NONE, neighbor={ 'filter':"0", 'collect':C_COLLECT }, **params )
                    r4 = g.Neighborhood( collect=C_NONE, neighbor={ 'filter':"1", 'collect':C_COLLECT, 'post':"1" }, **params )
                    r5 = g.Neighborhood( collect=C_NONE, neighbor={ 'filter':"1", 'collect':C_COLLECT, 'post':"0" }, **params )
                    r6 = g.Neighborhood( neighbor={ 'filter':"1", 'collect':C_COLLECT, 'post':"1" }, **params )
                    r7 = g.Neighborhood( neighbor={ 'filter':"1", 'collect':C_COLLECT, 'post':"0" }, **params )
                    Expect( r1 == r2 )
                    Expect( len(r3) == 0 )
                    Expect( r1 == r4 )
                    Expect( r1 == r5 )
                    Expect( r0 == r6 )
                    Expect( r5 == r7 )


    g.Erase()




###############################################################################
# TEST_FilterExpressions_mixed
#
###############################################################################
def TEST_FilterExpressions_mixed():
    """
    Mixed filter expressions for pyvgx.Graph.Neighborhood()
    t_nominal=11
    test_level=3202
    """
    root = "rank_root"
    levels = 3
    for fanout_factor in [1,5,25,125]:
        for modifiers in [ [M_INT], [M_INT,M_UINT], [M_FLT], [M_INT,M_FLT], [M_INT,M_UINT,M_FLT] ]:
            g = NewFanout( "fanout", root, fanout_factor=fanout_factor, levels=levels, modifiers=modifiers )

            # Add various arcs to root's terminals from other roots to give terminals a variable number of incident arcs
            for side in range( int( sqrt( fanout_factor ) ) ):
                AppendFanout( "fanout", "side_%d" % side, fanout_factor=side, levels=2, modifiers=modifiers )

            print("%d %s %s" % (fanout_factor, modifiers, g))

            size = fanout_factor * len( modifiers )
            result = g.Neighborhood( root )
            Expect( len(result) == size,                "all arcs" )

            # Filter arc to next node and filter on its ID
            for first in [(3,'level_1_3'), (21,'level_1_21'), (63,'level_1_63')]:
                n, node = first
                result_single = g.Neighborhood( root, filter="next.arc.value == %d && next.arc.mod == M_INT" % n, neighbor={ 'arc':D_OUT, 'filter':"vertex.id == '%s'" % node } )
                if node in g and M_INT in modifiers:
                    Expect( len( result_single ) == 1,              "single result" )
                    Expect( result_single[0] == node,               "exact node" )
                else:
                    Expect( len( result_single ) == 0,              "zero results" )
                result_mods = g.Neighborhood( root, filter="next.arc.value == %d" % n, neighbor={ 'arc':D_OUT, 'filter':"vertex.id == '%s'" % node } )
                if node in g:
                    Expect( len( result_mods ) == len( modifiers ), "all modifiers of arc" )
                    Expect( result_mods[0] == node,                 "exact node" )
                else:
                    Expect( len( result_mods ) == 0,                "zero results" )


            # Ensure next arc and previous arc agreement
            for n in range( 0, fanout_factor ):
                result_zero = g.Neighborhood( root, filter="next.arc.value == %d" % n, neighbor={ 'filter':"prev.arc.value != %d" % n } )
                Expect( len( result_zero ) == 0,                    "zero results" )
                result_hits = g.Neighborhood( root, filter="next.arc.value == %d" % n, neighbor={ 'filter':"prev.arc.value == %d" % n } )
                Expect( len( result_hits ) != 0,                    "nonzero results" )
                                
            # Deep
            result = g.Neighborhood(    root,
                                        fields=F_AARC,
                                        collect=C_NONE,
                                        filter="next.arc.value in {7,17} && next.arc.mod == M_INT",
                                        neighbor = {
                                            'filter'    : "vertex.id in {'level_1_7', 'level_1_17'}",
                                            'adjacent'  : {
                                                'arc'       : D_OUT,
                                                'filter'    : "next.arc.value in {11, 71, 111, 171, 211}",
                                                'neighbor'  : {
                                                    'filter'    : "vertex.id in {'level_2_11', 'level_2_71', 'level_2_111' ,'level_2_171', 'level_2_211'}",
                                                    'traverse'  : {
                                                        'filter'    : "next.arc.value == 300",
                                                        'collect'   : C_COLLECT,
                                                        'arc'       : D_OUT,
                                                        'neighbor'  : {
                                                            'filter'    : "vertex.id == 'level_3_300'"
                                                        }
                                                    }
                                                }
                                            }
                                        }

                                    )
            if 'level_3_300' in g and M_INT in modifiers:
                Expect( len( result ) > 0,          "one or more results" )
            else:
                Expect( len( result ) == 0,         "zero results" )
    g.Erase()




###############################################################################
# TEST_FilterExpressions_registers
#
###############################################################################
def TEST_FilterExpressions_registers():
    """
    Filter registers
    test_level=3202
    """
    g = Graph( "filter_registers" )

    for t in range( 1000 ):
        init = "trunk_%d" % t
        V = g.NewVertex( init )
        V['t'] = t
        g.CloseVertex( V )
        term = "trunk_%d" % (t+1)
        g.Connect( init, ("main",M_INT,t), term )
        for n in range( 10 ):
            node = "node_%d_%d" % (t, n)
            g.Connect( init, ("side",M_INT,n), node )


    for level in range( 500 ):
        print(level, end=' ')

        # One level
        result_true = g.Neighborhood( "trunk_%d" % level, 
            neighbor = {
                'adjacent'  : {
                    'arc'       :   D_OUT,
                    'filter'    :   "next.id == 'node_%d*'" % (level+1)
                }
            }
        )
        result_false = g.Neighborhood( "trunk_%d" % level, 
            neighbor = {
                'adjacent'  : {
                    'arc'       :   D_OUT,
                    'filter'    :   "next.id == 'node_%d*'" % (level)
                }
            }
        )
        Expect( len( result_true ) == 1 and result_true[0] == "trunk_%d" % (level+1) )
        Expect( len( result_false ) == 0 )

        # Two levels
        result_true = g.Neighborhood( "trunk_%d" % level, 
            neighbor = {
                'adjacent'  : {
                    'arc'       :   D_OUT,
                    'neighbor'  :   {
                        'adjacent'  : {
                            'arc'       :   D_OUT,
                            'filter'    :   "next.id == 'node_%d*'" % (level+2)
                        }
                    }
                }
            }
        )
        result_false = g.Neighborhood( "trunk_%d" % level, 
            neighbor = {
                'adjacent'  : {
                    'arc'       :   D_OUT,
                    'neighbor'  :   {
                        'adjacent'  : {
                            'arc'       :   D_OUT,
                            'filter'    :   "next.id == 'node_%d*'" % (level+1)
                        }
                    }
                }
            }
        )
        Expect( len( result_true ) == 1 and result_true[0] == "trunk_%d" % (level+1)  )
        Expect( len( result_false ) == 0 )

        # Three levels 
        result_true = g.Neighborhood( "trunk_%d" % level, 
            neighbor = {
                'adjacent'  : {
                    'arc'       :   D_OUT,
                    'neighbor'  :   {
                        'adjacent'  : {
                            'arc'       :   D_OUT,
                            'neighbor'  :   {
                                'indegree'  :   (V_EQ,1),
                                'adjacent'  : {
                                    'arc'       :   D_OUT,
                                    'filter'    :   "next.id == 'node_%d*'" % (level+3)
                                }
                            }
                        }
                    }
                }
            }
        )
        result_false = g.Neighborhood( "trunk_%d" % level, 
            neighbor = {
                'adjacent'  : {
                    'arc'       :   D_OUT,
                    'neighbor'  :   {
                        'adjacent'  : {
                            'arc'       :   D_OUT,
                            'neighbor'  :   {
                                'indegree'  :   (V_EQ,2),
                                'adjacent'  : {
                                    'arc'       :   D_OUT,
                                    'filter'    :   "next.id == 'node_%d*'" % (level+3)
                                }
                            }
                        }
                    }
                }
            }
        )
        Expect( len( result_true ) == 1 and result_true[0] == "trunk_%d" % (level+1) )
        Expect( len( result_false ) == 0 )

        # Four levels
        result_true = g.Neighborhood( "trunk_%d" % level, 
            # 1
            neighbor = {
                'filter'    :   "do( store( R1, 100 ) )",
                'adjacent'  : {
                    'arc'       :   D_OUT,
                    # 2
                    'neighbor'  :   {
                        'adjacent'  : {
                            'arc'       :   D_OUT,
                            # 3
                            'neighbor'  :   {
                                'property'  :   {
                                    't'         : level+3
                                },
                                'adjacent'  : {
                                    'arc'       :   D_OUT,
                                    # 4
                                    'neighbor'  :   {
                                        'indegree'  :   (V_EQ,1),
                                        'adjacent'  : {
                                            'arc'       :   D_OUT,
                                            'filter'    :   "load( R1 ) == 100 && next.id == 'node_%d*'" % (level+4),
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        )
        result_false = g.Neighborhood( "trunk_%d" % level, 
            # 1
            neighbor = {
                'filter'    :   "do( store( R1, 100 ) )",
                'adjacent'  :   {
                    'arc'       :   D_OUT,
                    # 2
                    'neighbor'  :   {
                        'adjacent'  : {
                            'arc'       :   D_OUT,
                            # 3
                            'neighbor'  :   {
                                'property'  :   {
                                    'nope'      : level+3
                                },
                                'adjacent'  : {
                                    'arc'       :   D_OUT,
                                    # 4
                                    'neighbor'  :   {
                                        'indegree'  :   (V_EQ,1),
                                        'adjacent'  : {
                                            'arc'       :   D_OUT,
                                            'filter'    :   "load( R1 ) == 99 && next.id == 'node_%d*'" % (level+4),
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        )
        Expect( len( result_true ) == 1 and result_true[0] == "trunk_%d" % (level+1) )
        Expect( len( result_false ) == 0 )

        # Five levels
        result_true = g.Neighborhood( "trunk_%d" % level, 
            filter = "next.id == 'trunk_%d'" % (level+1),
            # 1
            neighbor = {
                'property'  :   {
                    't'         : level+1
                },
                'filter'    :   "do( store( R1, vertex['t'] ) )",
                'adjacent'  : {
                    'arc'       :   D_OUT,
                    'filter'    :   "do( store( R2, next.arc.value ), store( R3, next.arc.type ) ) && next.id == 'trunk_%d'" % (level+2),
                    # 2
                    'neighbor'  :   {
                        'property'  :   {
                            't'         : level+2
                        },
                        'adjacent': {
                            'arc'       :   D_OUT,
                            'filter'    :   "next.id == 'trunk_%d' && do( store( R4, 123456 ) )" % (level+3),
                            # 3
                            'neighbor'  :   {
                                'adjacent' : {
                                    'arc'       :   D_OUT,
                                    'filter'    :   "load( R1 ) == %d && next.id == 'trunk_%d'" % (level+1, level+4),
                                    # 4
                                    'neighbor'  :   {
                                        'adjacent'  : {
                                            'arc'       :   D_OUT,
                                            'filter'    :   "next.id == 'trunk_%d'" % (level+5),
                                            # 5
                                            'neighbor'  :   {
                                                'adjacent'  : {
                                                    'arc'       :   D_OUT,
                                                    'filter'    :   "load( R2 ) == %d && load( R3 ) == prev.arc.type && load( R4 ) == 123456 && next.id == 'node_%d*'" % (level+1, level+5)
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        )
        result_false = g.Neighborhood( "trunk_%d" % level, 
            filter = "next.id == 'trunk_%d'" % (level+1),
            # 1
            neighbor = {
                'property'  :   {
                    't'         : level+1
                },
                'filter'    :   "do( store( R1, vertex['t'] ) )",
                'adjacent'  : {
                    'arc'       :   D_OUT,
                    'filter'    :   "next.id == 'trunk_%d'" % (level+2),
                    # 2
                    'neighbor'  :   {
                        'property'  :   {
                            't'         : level+2
                        },
                        'adjacent'  : {
                            'arc'       :   D_OUT,
                            'filter'    :   "next.id == 'trunk_%d'" % (level+3),
                            # 3
                            'neighbor'  :   {
                                'property'  :   {
                                    't'         : level+3
                                },
                                'adjacent'  : {
                                    'arc'       :   D_OUT,
                                    # 4
                                    'neighbor'  :   {
                                        'property'  :   {
                                            't'         : level+4
                                        },
                                        'adjacent'  : {
                                            'arc'       :   D_OUT,
                                            'filter'    :   "load( R2 ) == %d && next.id == 'trunk_%d'" % (level+1, level+5),
                                            # 5
                                            'neighbor'  :   {
                                                'adjacent'  : {
                                                    'arc'       :   D_OUT,
                                                    'filter'    :   "next.id == 'node_%d*'" % (level+5)
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }

        )
        Expect( len( result_true ) == 1 and result_true[0] == "trunk_%d" % (level+1) )
        Expect( len( result_false ) == 0 )
    
    print() 

    g.Erase()




###############################################################################
# TEST_FilterExpressions_collect
#
###############################################################################
def TEST_FilterExpressions_collect():
    """
    Stage, Commit, Collect
    t_nominal=88
    test_level=3202
    """
    root = "collect_root"
    levels = 3
    for fanout_factor in [1,4,16,64]:
        for modifiers in [ [M_INT], [M_INT,M_UINT], [M_FLT], [M_INT,M_FLT], [M_INT,M_UINT,M_FLT] ]:
            g = NewFanout( "fanout", root, fanout_factor=fanout_factor, levels=levels, modifiers=modifiers )

            # Add various arcs to root's terminals from other roots to give terminals a variable number of incident arcs
            for side in range( int( sqrt( fanout_factor ) ) ):
                AppendFanout( "fanout", "side_%d" % side, fanout_factor=side, levels=2, modifiers=modifiers )

            print("%d %s %s" % (fanout_factor, modifiers, g))

            for H_x in [0, 1, 2, 10, -1]:
                for S_x in [S_NONE, S_VAL, S_ID, S_DEG, S_ADDR, S_RANK]:
                    print(fanout_factor, modifiers, H_x, S_x)
                    
                    params = { 'id':root, 'hits':H_x, 'sortby':S_x, 'fields':F_AARC }

                    if S_x == S_RANK:
                        params['rank'] = "sin( .arc.value )"

                    # Collect
                    r1 = g.Neighborhood( **params )
                    r2 = g.Neighborhood( collect=C_NONE, filter="collect()", **params )
                    Expect( r1 == r2, "filter collect equivalent" )

                    # Stage / Commit
                    r1 = g.Neighborhood( **params )
                    r2 = g.Neighborhood( collect=C_NONE, filter="stage() && commit()", **params )
                    Expect( r1 == r2, "filter stage/commit equivalent" )

                    # Specify staging slot
                    for cx in [-1,0,1,2,3,4,5]:
                        r1 = g.Neighborhood( **params )
                        r2 = g.Neighborhood( collect=C_NONE, filter="stage(null,%d) && commit(%d)" % (cx,cx), **params )
                        Expect( r1 == r2, "filter stage/commit equivalent" )
                        # Staging slot mismatch
                        r0 = g.Neighborhood( collect=C_NONE, filter="stage(null,%d) && commit(%d)" % (cx,cx+1), **params )
                        Expect( len(r0) == 0, "nothing collected" )

                    # Override predicator value
                    r1 = g.Neighborhood( **params )
                    r2 = g.Neighborhood( collect=C_NONE, filter="collect()", **params )
                    r3 = g.Neighborhood( collect=C_NONE, filter="collect(.arc.value + 2*pi)", **params )
                    r4 = g.Neighborhood( collect=C_NONE, filter="stage(null) && commit()", **params )
                    r5 = g.Neighborhood( collect=C_NONE, filter="stage(.arc.value + 2*pi) && commit()", **params )
                    Expect( r1 == r2, "filter collect equivalent" )
                    Expect( r1 == r4, "filter stage/commit equivalent" )
                    Expect( len(r1) == len(r3), "filter collect with predicator override same result size" )
                    Expect( len(r1) == len(r5), "filter stage/commit with predicator override same result size" )
                    if r1:
                        Expect( r1 != r3, "filter collect with predicator override different results" )
                        Expect( r1 != r5, "filter stage/commit with predicator override different results" )

                    # Staging slot mismatch
                    r0 = g.Neighborhood( collect=C_NONE, filter="stage(null,%d) && commit(%d)" % (cx,cx+1), **params )
                    Expect( len(r0) == 0, "nothing collected" )

                    # Filter
                    for expr in [ "1", ".arc.value < 10", "sin( .arc.value ) > 0", "'5' in .id", ".arc.mod == M_FLT" ]:
                        print("    expr=%s" % expr)

                        r1 = g.Neighborhood( filter=expr, **params )
                        r2 = g.Neighborhood( collect=C_NONE, filter="collectif(%s)" % expr, **params )
                        r3 = g.Neighborhood( collect=C_NONE, filter="stageif(%s) && do(commit())" % expr, **params )
                        r4 = g.Neighborhood( collect=C_NONE, filter="do(stage()) && commitif(%s) && do(unstage())" % expr, **params )
                        Expect( r1 == r2, "filter collect equivalent" )
                        Expect( r1 == r3, "filter stageif/commit equivalent" )
                        Expect( r1 == r4, "filter stage/commitif equivalent" )

                        # Extended
                        r1 = g.Neighborhood( collect=C_NONE, neighbor={ 'traverse': { 'arc':D_OUT, 'collect':C_COLLECT } }, **params )
                        r2 = g.Neighborhood( collect=C_NONE, neighbor={ 'traverse': { 'arc':D_OUT, 'collect':C_SCAN, 'filter':"collect()" } }, **params )
                        r3 = g.Neighborhood( collect=C_NONE, neighbor={ 'traverse': { 'arc':D_OUT, 'collect':C_SCAN, 'filter':"stage() && commit()" } }, **params )
                        Expect( r1 == r2, "filter collect equivalent" )
                        Expect( r1 == r3, "filter stage/commit equivalent" )

                        # Extended with filters and mixed collection
                        r1 = g.Neighborhood( neighbor={ 'traverse': { 'arc':D_OUT, 'collect':C_COLLECT, 'filter':expr } }, **params )
                        r2 = g.Neighborhood( neighbor={ 'traverse': { 'arc':D_OUT, 'collect':C_SCAN, 'filter':"collectif(%s)" % expr } }, **params )
                        r3 = g.Neighborhood( neighbor={ 'traverse': { 'arc':D_OUT, 'collect':C_SCAN, 'filter':"stageif(%s) && do(commit())" % expr } }, **params )
                        r4 = g.Neighborhood( neighbor={ 'traverse': { 'arc':D_OUT, 'collect':C_SCAN, 'filter':"do(stage()) && commitif(%s) && do(unstage())" % expr } }, **params )
                        Expect( r1 == r2, "filter collector equivalent" )
                        Expect( r1 == r3, "filter stageif/commit equivalent" )
                        Expect( r1 == r4, "filter stage/commitif equivalent" )

                        # With staging slot and predicator override
                        r1 = g.Neighborhood( collect=C_NONE, neighbor={ 'traverse': { 'arc':D_OUT, 'collect':C_COLLECT, 'filter':expr } }, **params )
                        r2 = g.Neighborhood( collect=C_NONE, neighbor={ 'traverse': { 'arc':D_OUT, 'collect':C_SCAN, 'filter':"collectif( %s )" % expr } }, **params )
                        r3 = g.Neighborhood( collect=C_NONE, neighbor={ 'traverse': { 'arc':D_OUT, 'collect':C_SCAN, 'filter':"stageif(%s,null) && do(commit())" % expr } }, **params )
                        r4 = g.Neighborhood( collect=C_NONE, neighbor={ 'traverse': { 'arc':D_OUT, 'collect':C_SCAN, 'filter':"collectif(%s,.arc.value + 2*pi)" % expr } }, **params )
                        r5 = g.Neighborhood( collect=C_NONE, neighbor={ 'traverse': { 'arc':D_OUT, 'collect':C_SCAN, 'filter':"stageif(%s,.arc.value + 2*pi) && do(commit())" % expr } }, **params )
                        Expect( r1 == r2, "filter collector equivalent" )
                        Expect( r1 == r3, "filter stageif/commit equivalent" )
                        Expect( len(r1) == len(r4), "same result size despite predicator override" )
                        Expect( len(r1) == len(r5), "same result size despite predicator override" )
                        if r1:
                            Expect( r1 != r4, "predicator override different result" )
                            Expect( r1 != r5, "predicator override different result" )

                        for cx in [-1,0,1]:
                            r5 = g.Neighborhood( collect=C_NONE, neighbor={ 'traverse' : { 'arc':D_OUT, 'collect':C_SCAN, 'filter':"stageif(%s,.arc.value + 2*pi, %d) && do(commit(%d))" % (expr, cx, cx) } }, **params )
                            Expect( len(r1) == len(r5), "same result size despite predicator override" )
                            if r1:
                                Expect( r1 != r5, "predicator override different result" )


    g.Erase()




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
