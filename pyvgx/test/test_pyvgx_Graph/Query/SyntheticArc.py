###############################################################################
# 
# VGX Server
# Distributed engine for plugin-based graph and vector search
# 
# Module:  pyvgxtest
# File:    SyntheticArc.py
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
import math
import code

graph = None

REL_SYN = "__synthetic__"


###############################################################################
# outarc_dict
#
###############################################################################
def outarc_dict( modstr, relstr, val ):
    """
    """
    return { 'direction'    : 'D_OUT',
             'modifier'     : modstr,
             'relationship' : relstr,
             'value'        : val
           }

SYNTHETIC_OUTARC = outarc_dict( 'M_NONE', REL_SYN, -1 )



###############################################################################
# arc_dir
#
###############################################################################
def arc_dir( result, offset=0 ):
    """
    """
    return result[offset][ 'arc' ][ 'direction' ]



###############################################################################
# arc_rel
#
###############################################################################
def arc_rel( result, offset=0 ):
    """
    """
    return result[offset][ 'arc' ][ 'relationship' ]



###############################################################################
# arc_mod
#
###############################################################################
def arc_mod( result, offset=0 ):
    """
    """
    return result[offset][ 'arc' ][ 'modifier' ]



###############################################################################
# arc_val
#
###############################################################################
def arc_val( result, offset=0 ):
    """
    """
    return result[offset][ 'arc' ][ 'value' ]




###############################################################################
# TEST_SyntheticArc_Struct
#
###############################################################################
def TEST_SyntheticArc_Struct():
    """
    Synthetic Arc - structure/traversal
    test_level=3101
    """
    g = Graph( "synthetic_arcs" )
    g.Truncate()


    ROOT = g.NewVertex( "ROOT" )

    # Test repeatedly while adding more arcs, to check traversal correctness
    # for all different arcvector structures

    ADD_ARCS = [

        ( ("to", M_INT, 123), "Simple123"  ), # Simple arcvector after this
        ( ("to", M_INT, 456), "Other"      ), # Array of arcs after this
        ( ("to", M_FLT, 7.8), "Simple123"  ), # Multiple arc after this

    ]


    ARC_PARAMS = [
        ('to', D_OUT, M_INT),
        ('to', D_OUT, M_INT, V_EQ, 123),
        ('*',  D_OUT, M_INT, V_EQ, 123),
    ]


    for arc, term in ADD_ARCS:
        #
        print( "Adding %s %s" % (arc, term)  )
        # Different arcvector structures trigger different internal paths
        g.Connect( ROOT, arc, term )
        print( g.Neighborhood(ROOT, fields=F_AARC) )
        # Different arc parameters should trigger same generic arc filter which accepts synthetic arc
        for arc_param in ARC_PARAMS:
            print( "arc_param = %s" % str( arc_param ) )
            # Different collect modes trigger different internal paths
            for collect in [ C_COLLECT, C_SCAN ]:
                print( "collect = %s" % str(collect) )
                # Enable or disable collection in filter
                for collect_in_filter in [False, True]:
                    print( "collect_in_filter = %d" % collect_in_filter )
                    if collect_in_filter is False:
                        # Non-null for synthetic arc, null for normal arc.
                        # (filter match on non-null)
                        filter = "synarc.value( 'to', M_INT )"
                    else:
                        # Collect synthetic arc in filter, and make filter
                        # match in all cases modifier is M_INT 
                        filter = "collect( synarc.value( 'to', M_INT ) ); return( .arc.mod == M_INT || .arc.issyn )"
            #
                    ########################
                    # Neighborhood
                    #
                    result = g.Neighborhood( 
                        id       = ROOT,
                        hits     = 10,
                        fields   = F_AARC,
                        result   = R_DICT,
                        collect  = collect,
                        arc      = arc_param,
                        filter   = filter,
                        neighbor = "Simple123"
                    )
            #
                    # Collect by traverser
                    if collect == C_COLLECT and collect_in_filter is False:
                        Expect( len(result) == 1,                               "Only synthetic arc returned, got %d" % len( result ) )
                        R = result[0]['arc']
                        Expect( R == SYNTHETIC_OUTARC,                          "Synthetic arc collected by traverser, got %s" % R )
                    # No collection
                    elif collect == C_SCAN and collect_in_filter is False:
                        Expect( len(result) == 0,                               "Nothing collected" )
                    # Collect by traverser and in filter
                    elif collect == C_COLLECT and collect_in_filter is True:
                        Expect( len(result) == 3,                               "Three arc variants, got %d" % len( result ) )
                        R1, R2, R3 = [r['arc'] for r in result]
                        Expect( R1 == outarc_dict( 'M_INT', 'to', 123 ),        "Actual arc, got %s" % R1 )
                        Expect( R2 == outarc_dict( 'M_INT', REL_SYN, 123 ),     "Synthetic arc collected in filter, got %s" % R2 )
                        Expect( R3 == SYNTHETIC_OUTARC,                         "Synthetic arc collected by traverser, got %s" % R3 )
                    # Collect in filter only
                    elif collect == C_SCAN and collect_in_filter is True:
                        Expect( len(result) == 1,                               "Only synthetic arc returned, got %d" % len( result ) )
                        R = result[0]['arc']
                        Expect( R == outarc_dict( 'M_INT', REL_SYN, 123 ),      "Synthetic arc collected in filter, got %s" % R )
                    #
                    else:
                        raise Exeption( "Test setup error" )
            #
            #
            ########################
            # Adjacent
            #
            # No match due to arc param
            result = g.Adjacent( 
                id       = ROOT,
                arc      = arc_param,
                filter   = "synarc.value( 'to', M_INT ) == 122", # <- 122 should not match
                neighbor = "Simple123"
            )
            Expect( result is False,                "No match on synthetic arc value" )

            # Specify terminal
            result = g.Adjacent( 
                id       = ROOT,
                arc      = arc_param,
                filter   = "synarc.value( 'to', M_INT ) == 123",
                neighbor = "Simple123"
            )
            Expect( result is True,                 "Match on synthetic arc value" )

            # Unspecified terminal
            result = g.Adjacent( 
                id       = ROOT,
                arc      = arc_param,
                filter   = "synarc.value( 'to', M_INT ) == 123"
            )
            Expect( result is True,                 "Match on synthetic arc value" )




###############################################################################
# TEST_SyntheticArc
#
###############################################################################
def TEST_SyntheticArc():
    """
    Synthetic Arc
    t_nominal=10
    test_level=3101
    """

    for factor in [1,2,5]:
        for mods in [ [M_INT, M_FLT], [M_FLT, M_INT], [M_INT, M_FLT, M_CNT], [M_CNT, M_FLT, M_INT] ]:
            for rels in [ [], ["extra"] ]:

                g = NewFanout( "synthetic_arcs", "ROOT", fanout_factor=factor, levels=4, modifiers=mods, relationships=rels )
                print( "%s fanout=%d mods=%s rels=%s" % (g, factor, mods, rels) )


                def n_traversals( factor, levels ):
                    lvl = 0
                    N = 0
                    visit = 1
                    while lvl < levels:
                        lvl += 1              # 1   2   3
                        nodes = lvl * factor  # 5   10  15
                        visit = visit * nodes # 5   50  750
                        N += visit            # 5   55  805
                    return N
                    

                #GL = globals()
                #GL.update( locals() )
                #code.interact( local=GL )

                #########################
                # 1 level
                #########################
                N = n_traversals(factor,1)
                
                E1 = "synarc.value( 'to_level_1', M_FLT )"
                E2 = "synarc.value( 'to_level_1', M_INT )"
                E_NOVAL = "synarc.value( 'does_not_exist', M_INT )"

                base_expr = "fx = next['number']; val = %s + %s + %s; collectif( val == 2*fx, val )"
                collect_expr = [
                    base_expr % (E1, E2, E_NOVAL),
                    base_expr % (E1, E_NOVAL, E2),
                    base_expr % (E_NOVAL, E1, E2),
                    base_expr % (E2, E1, E_NOVAL),
                    base_expr % (E2, E_NOVAL, E1),
                    base_expr % (E_NOVAL, E2, E1)
                ]


                # Query A
                filter_L1 = "collect( %s + %s )" % (E1, E2)
                #print( "L1 QueryA: filter1=%s" % filter_L1 )
                resultA = g.Neighborhood( 
                    id       = "ROOT",
                    hits     = N+1,
                    fields   = F_AARC,
                    result   = R_DICT,
                    collect  = C_SCAN,
                    filter   = filter_L1
                )
                Expect( len(resultA) == N,                  "%d, got %d" % (N, len(resultA)) )

                for filter in collect_expr:
                    # Query B
                    #print( "L1 QueryB: filter1=%s" % filter_L1 )
                    resultB = g.Neighborhood( 
                        id       = "ROOT",
                        hits     = N+1,
                        fields   = F_AARC,
                        result   = R_DICT,
                        collect  = C_SCAN,
                        filter   = filter_L1
                    )
                    Expect( len(resultB) == N,                  "%d, got %d" % (N, len(resultB)) )
                    Expect( resultA == resultB,                 "Non-existing synarc.value() should have been ignored" )


                #########################
                # 2 levels
                #########################
                N = n_traversals(factor,2)

                E1L2 = "synarc.value( 'to_level_2', M_FLT )"
                E2L2 = "synarc.value( 'to_level_2', M_INT )"

                collect_expr_L2 = [
                    base_expr % (E1L2, E2L2, E_NOVAL),
                    base_expr % (E1L2, E_NOVAL, E2L2),
                    base_expr % (E_NOVAL, E1L2, E2L2),
                    base_expr % (E2L2, E1L2, E_NOVAL),
                    base_expr % (E2L2, E_NOVAL, E1L2),
                    base_expr % (E_NOVAL, E2L2, E1L2)
                ]

                filter_L1 = "collect( %s + %s )" % (E1, E2)
                filter_L2 = "collect( %s + %s )" % (E1L2, E2L2)
                #print( "L2 QueryA: filter1=%s" % filter_L1 )
                #print( "           filter2=%s" % filter_L2 )
                resultA = g.Neighborhood( 
                    id       = "ROOT",
                    hits     = N+1,
                    fields   = F_AARC,
                    result   = R_DICT,
                    collect  = C_SCAN,
                    filter   = filter_L1,
                    neighbor = {
                        'traverse': {
                            'collect'   :   C_SCAN,
                            'arc'       :   D_OUT,
                            'filter'    :   filter_L2
                        }
                    }
                )
                Expect( len(resultA) == N,               "%d, got %d" % (N, len(resultA)) )

                for filter_L1 in collect_expr:
                    for filter_L2 in collect_expr_L2:
                        #print( "L2 QueryB: filter1=%s" % filter_L1 )
                        #print( "           filter2=%s" % filter_L2 )
                        resultB = g.Neighborhood( 
                            id       = "ROOT",
                            hits     = N+1,
                            fields   = F_AARC,
                            result   = R_DICT,
                            collect  = C_SCAN,
                            filter   = filter_L1,
                            neighbor = {
                                'traverse': {
                                    'collect'   :   C_SCAN,
                                    'arc'       :   D_OUT,
                                    'filter'    :   filter_L2
                                }
                            }
                        )
                        Expect( len(resultB) == N,               "%d, got %d" % (N, len(resultB)) )
                        Expect( resultA == resultB,              "Non-existing synarc.value() should have been ignored" )

                #########################
                # 3 levels
                #########################
                N = n_traversals(factor,3)

                E1L3 = "synarc.value( 'to_level_3', M_FLT )"
                E2L3 = "synarc.value( 'to_level_3', M_INT )"

                collect_expr_L3 = [
                    base_expr % (E1L3, E2L3, E_NOVAL),
                    base_expr % (E1L3, E_NOVAL, E2L3),
                    base_expr % (E_NOVAL, E1L3, E2L3),
                    base_expr % (E2L3, E1L3, E_NOVAL),
                    base_expr % (E2L3, E_NOVAL, E1L3),
                    base_expr % (E_NOVAL, E2L3, E1L3)
                ]

                filter_L1 = "collect( %s + %s )" % (E1, E2)
                filter_L2 = "collect( %s + %s )" % (E1L2, E2L2)
                filter_L3 = "collect( %s + %s )" % (E1L3, E2L3)
                #print( "L3 QueryA: filter1=%s" % filter_L1 )
                #print( "           filter2=%s" % filter_L2 )
                #print( "           filter3=%s" % filter_L3 )
                resultA = g.Neighborhood( 
                    id       = "ROOT",
                    hits     = N+1,
                    fields   = F_AARC,
                    result   = R_DICT,
                    collect  = C_SCAN,
                    filter   = filter_L1,
                    neighbor = {
                        'traverse': {
                            'collect'   :   C_SCAN,
                            'arc'       :   D_OUT,
                            'filter'    :   filter_L2,
                            'neighbor'  : {
                                'traverse': {
                                    'collect'   :   C_SCAN,
                                    'arc'       :   D_OUT,
                                    'filter'    :   filter_L3
                                }
                            }
                        }
                    }
                )
                Expect( len(resultA) == N,               "%d, got %d" % (N, len(resultA)) )

                for filter_L1 in collect_expr:
                    for filter_L2 in collect_expr_L2:
                        for filter_L3 in collect_expr_L3:
                            #print( "L3 QueryB: filter1=%s" % filter_L1 )
                            #print( "           filter2=%s" % filter_L2 )
                            #print( "           filter3=%s" % filter_L3 )
                            resultB = g.Neighborhood( 
                                id       = "ROOT",
                                hits     = N+1,
                                fields   = F_AARC,
                                result   = R_DICT,
                                collect  = C_SCAN,
                                filter   = filter_L1,
                                neighbor = {
                                    'traverse': {
                                        'collect'   :   C_SCAN,
                                        'arc'       :   D_OUT,
                                        'filter'    :   filter_L2,
                                        'neighbor'  : {
                                            'traverse': {
                                                'collect'   :   C_SCAN,
                                                'arc'       :   D_OUT,
                                                'filter'    :   filter_L3
                                            }
                                        }
                                    }
                                }
                            )
                            Expect( len(resultB) == N,               "%d, got %d" % (N, len(resultB)) )
                            Expect( resultA == resultB,              "Non-existing synarc.value() should have been ignored" )

                #########################
                # 4 levels
                #########################
                N = n_traversals(factor,4)

                E1L4 = "synarc.value( 'to_level_4', M_FLT )"
                E2L4 = "synarc.value( 'to_level_4', M_INT )"

                collect_expr_L4 = [
                    base_expr % (E1L4, E2L4, E_NOVAL),
                    base_expr % (E1L4, E_NOVAL, E2L4),
                    base_expr % (E_NOVAL, E1L4, E2L4),
                    base_expr % (E2L4, E1L4, E_NOVAL),
                    base_expr % (E2L4, E_NOVAL, E1L4),
                    base_expr % (E_NOVAL, E2L4, E1L4)
                ]

                filter_L1 = "collect( %s + %s )" % (E1, E2)
                filter_L2 = "collect( %s + %s )" % (E1L2, E2L2)
                filter_L3 = "collect( %s + %s )" % (E1L3, E2L3)
                filter_L4 = "collect( %s + %s )" % (E1L4, E2L4)
                #print( "L4 QueryA: filter1=%s" % filter_L1 )
                #print( "           filter2=%s" % filter_L2 )
                #print( "           filter3=%s" % filter_L3 )
                #print( "           filter4=%s" % filter_L4 )
                resultA = g.Neighborhood( 
                    id       = "ROOT",
                    hits     = N+1,
                    fields   = F_AARC,
                    result   = R_DICT,
                    collect  = C_SCAN,
                    filter   = filter_L1,
                    neighbor = {
                        'traverse': {
                            'collect'   :   C_SCAN,
                            'arc'       :   D_OUT,
                            'filter'    :   filter_L2,
                            'neighbor'  : {
                                'traverse': {
                                    'collect'   :   C_SCAN,
                                    'arc'       :   D_OUT,
                                    'filter'    :   filter_L3,
                                    'neighbor'  : {
                                        'traverse': {
                                            'collect'   :   C_SCAN,
                                            'arc'       :   D_OUT,
                                            'filter'    :   filter_L4
                                        }
                                    }
                                }
                            }
                        }
                    }
                )
                Expect( len(resultA) == N,               "%d, got %d" % (N, len(resultA)) )

                """
                for filter_L1 in collect_expr:
                    for filter_L2 in collect_expr_L2:
                        for filter_L3 in collect_expr_L3:
                            for filter_L4 in collect_expr_L4:
                                #print( "L4 QueryB: filter1=%s" % filter_L1 )
                                #print( "           filter2=%s" % filter_L2 )
                                #print( "           filter3=%s" % filter_L3 )
                                #print( "           filter4=%s" % filter_L4 )
                                resultB = g.Neighborhood( 
                                    id       = "ROOT",
                                    hits     = N+1,
                                    fields   = F_AARC,
                                    result   = R_DICT,
                                    collect  = C_SCAN,
                                    filter   = filter_L1,
                                    neighbor = {
                                        'traverse': {
                                            'collect'   :   C_SCAN,
                                            'arc'       :   D_OUT,
                                            'filter'    :   filter_L2,
                                            'neighbor'  : {
                                                'traverse': {
                                                    'collect'   :   C_SCAN,
                                                    'arc'       :   D_OUT,
                                                    'filter'    :   filter_L3,
                                                    'neighbor'  : {
                                                        'traverse': {
                                                            'collect'   :   C_SCAN,
                                                            'arc'       :   D_OUT,
                                                            'filter'    :   filter_L4
                                                        }
                                                    }
                                                }
                                            }
                                        }
                                    }
                                )
                                Expect( len(resultB) == N,               "%d, got %d" % (N, len(resultB)) )
                                Expect( resultA == resultB,              "Non-existing synarc.value() should have been ignored" )
                """
                print( "Queries: %d" % system.Status()['performance']['internal_queries'] )


    g.Erase()





###############################################################################
# bitvector_match
#
###############################################################################
def bitvector_match( memobj, index, value ):
    """
    """
    memval = memobj[index]
    Expect( type(memval) == list and len(memval) == 1,  "bitvector encoded as singleton list, got %s" % type(memval) )
    bv_val = memval[0]
    Expect( type(bv_val) is int,                        "integer, got %s" % type(bv_val) )
    Expect( bv_val == value,                            "bitvector value %s at index %d, got %s" % (bin(value), index, bin(bv_val)) )





###############################################################################
# TEST_SyntheticArc_synarc_hasrel
#
###############################################################################
def TEST_SyntheticArc_synarc_hasrel():
    """
    Synthetic Arc - synarc.hasrel()
    test_level=3101
    """
    g = Graph( "synthetic_arcs" )
    g.Truncate()


    ROOT = g.NewVertex( "ROOT" )

    #
    #
    #           /-[int, M_INT, 100]-\           # 1
    #          ---[flt, M_FLT, 222]---> (A)     # 3
    #        /  \-[XXX, M_INT, 400]-/           # 4
    # (ROOT) 
    #        \
    #           -[flt, M_FLT, 333] -> (B)       # 2
    #
    ADD_ARCS = [
        [("int", M_INT, 100), "A"],
        [("flt", M_FLT, 222), "B"],
        [("flt", M_FLT, 333), "A"],
        [("XXX", M_INT, 400), "A"]
    ]


    NOISE_ARCS = [
        [('int', M_INT, n), str(n)] for n in range(1000)
    ]

    MEM = g.Memory( 1<<20 )


    for arc, term in ADD_ARCS + NOISE_ARCS:

        g.Connect( ROOT, arc, term )

        MEM.Reset()

        result_hasrel = g.Neighborhood(
            id      =   "ROOT",
            fields  =   F_ID,
            result  =   R_SIMPLE,
            arc     =   D_OUT,
            collect =   C_SCAN,
            memory  =   MEM,
            filter  =   """
                        bv1_0 = synarc.hasrel( 'int' );
                        bv1_1 = synarc.hasrel( 'flt' );
                        bv1_2 = synarc.hasrel( 'XXX' );

                        bv2_3 = synarc.hasrel( 'flt', 'int' );
                        bv2_4 = synarc.hasrel( 'int', 'flt' );
                        bv2_5 = synarc.hasrel( 'XXX', 'int' );
                        bv2_6 = synarc.hasrel( 'int', 'XXX' );
                        bv2_7 = synarc.hasrel( 'XXX', 'flt' );
                        bv2_8 = synarc.hasrel( 'flt', 'XXX' );

                        bv3_9 = synarc.hasrel( 'XXX', 'flt', 'int' );
                        bv3_10 = synarc.hasrel( 'XXX', 'int', 'flt' );
                        bv3_11 = synarc.hasrel( 'flt', 'XXX', 'int' );
                        bv3_12 = synarc.hasrel( 'flt', 'int', 'XXX' );
                        bv3_13 = synarc.hasrel( 'int', 'XXX', 'flt' );
                        bv3_14 = synarc.hasrel( 'int', 'flt', 'XXX' );

                        bv0_15 = synarc.hasrel();

                        returnif( !.arc.issyn, false );

                        rwrite( R1, bv1_0, bv1_1, bv1_2,
                                    bv2_3, bv2_4, bv2_5, bv2_6, bv2_7, bv2_8,
                                    bv3_9, bv3_10, bv3_11, bv3_12, bv3_13, bv3_14,
                                    bv0_15 );

                        collectif( 1 )
                        """
        )

        #################################
        # (ROOT) -[int, M_INT, 100]-> (A)
        #
        if g.size == 1:
            Expect( len(result_hasrel) == 1 and result_hasrel[0] == "A",      "Single synthetic arc to 'A', got %s" % str(result_hasrel) )
            for item in MEM[0:16]:
                Expect( type(item) is list,                     "Bitvector (encoded as singleton list) expected, got %s" % type(item) )
            # Arc matches to A
            bitvector_match( MEM, 0,  0b1 )     # hasrel( int ) == 0b1
            bitvector_match( MEM, 1,  0b0 )     # hasrel( flt ) == 0b0
            bitvector_match( MEM, 2,  0b0 )     # hasrel( XXX ) == 0b0
            bitvector_match( MEM, 3,  0b01 )    # hasrel( flt, int ) == 0b01
            bitvector_match( MEM, 4,  0b10 )    # hasrel( int, flt ) == 0b10
            bitvector_match( MEM, 5,  0b01 )    # hasrel( XXX, int ) == 0b01
            bitvector_match( MEM, 6,  0b10 )    # hasrel( int, XXX ) == 0b10
            bitvector_match( MEM, 7,  0b00 )    # hasrel( XXX, flt ) == 0b00
            bitvector_match( MEM, 8,  0b00 )    # hasrel( flt, XXX ) == 0b00
            bitvector_match( MEM, 9,  0b001 )   # hasrel( XXX, flt, int ) == 0b001
            bitvector_match( MEM, 10, 0b010 )   # hasrel( XXX, int, flt ) == 0b010
            bitvector_match( MEM, 11, 0b001 )   # hasrel( flt, XXX, int ) == 0b001
            bitvector_match( MEM, 12, 0b010 )   # hasrel( flt, int, XXX ) == 0b010
            bitvector_match( MEM, 13, 0b100 )   # hasrel( int, XXX, flt ) == 0b100
            bitvector_match( MEM, 14, 0b100 )   # hasrel( int, flt, XXX ) == 0b100
            bitvector_match( MEM, 15, 0b0 )     # hasrel() == 0b0
            end = MEM[16]
            Expect( type(end) is int and end == 0,              "Should not have written beyond end, found %s (%s)" % (end, type(end)) )


        ###################################
        # Arcs to A and B
        #
        # Only simple arc to B
        #   (ROOT) -[flt, M_FLT, 333]-> (B)
        # 
        # More arcs to A, plus other arcs 
        # 
        else:
            Expect( 'A' in result_hasrel and 'B' in result_hasrel, "Result should include arcs to 'A' and 'B'" )
            
            offset_A = result_hasrel.index('A')*16
            offset_B = result_hasrel.index('B')*16

            end = MEM[ 16 * len(result_hasrel )]
            Expect( type(end) is int and end == 0,              "Should not have written beyond end, found %s (%s)" % (end, type(end)) )

            # Arc matches to B
            bitvector_match( MEM, offset_B+0,  0b0 )     # hasrel( int ) == 0b0
            bitvector_match( MEM, offset_B+1,  0b1 )     # hasrel( flt ) == 0b1
            bitvector_match( MEM, offset_B+2,  0b0 )     # hasrel( XXX ) == 0b0
            bitvector_match( MEM, offset_B+3,  0b10 )    # hasrel( flt, int ) == 0b10
            bitvector_match( MEM, offset_B+4,  0b01 )    # hasrel( int, flt ) == 0b01
            bitvector_match( MEM, offset_B+5,  0b00 )    # hasrel( XXX, int ) == 0b00
            bitvector_match( MEM, offset_B+6,  0b00 )    # hasrel( int, XXX ) == 0b00
            bitvector_match( MEM, offset_B+7,  0b01 )    # hasrel( XXX, flt ) == 0b01
            bitvector_match( MEM, offset_B+8,  0b10 )    # hasrel( flt, XXX ) == 0b10
            bitvector_match( MEM, offset_B+9,  0b010 )   # hasrel( XXX, flt, int ) == 0b010
            bitvector_match( MEM, offset_B+10, 0b001 )   # hasrel( XXX, int, flt ) == 0b001
            bitvector_match( MEM, offset_B+11, 0b100 )   # hasrel( flt, XXX, int ) == 0b100
            bitvector_match( MEM, offset_B+12, 0b100 )   # hasrel( flt, int, XXX ) == 0b100
            bitvector_match( MEM, offset_B+13, 0b001 )   # hasrel( int, XXX, flt ) == 0b001
            bitvector_match( MEM, offset_B+14, 0b010 )   # hasrel( int, flt, XXX ) == 0b010
            bitvector_match( MEM, offset_B+15, 0b0 )     # hasrel() == 0b0

            ###################################
            #        / -[int, M_INT, 100]-> (A)
            # (ROOT)  
            #        \ -[flt, M_FLT, 333]-> (B)
            #
            if g.size == 2:
                # Arc matches to A
                bitvector_match( MEM, offset_A+0,  0b1 )     # hasrel( int ) == 0b1
                bitvector_match( MEM, offset_A+1,  0b0 )     # hasrel( flt ) == 0b0
                bitvector_match( MEM, offset_A+2,  0b0 )     # hasrel( XXX ) == 0b0
                bitvector_match( MEM, offset_A+3,  0b01 )    # hasrel( flt, int ) == 0b01
                bitvector_match( MEM, offset_A+4,  0b10 )    # hasrel( int, flt ) == 0b10
                bitvector_match( MEM, offset_A+5,  0b01 )    # hasrel( XXX, int ) == 0b01
                bitvector_match( MEM, offset_A+6,  0b10 )    # hasrel( int, XXX ) == 0b10
                bitvector_match( MEM, offset_A+7,  0b00 )    # hasrel( XXX, flt ) == 0b00
                bitvector_match( MEM, offset_A+8,  0b00 )    # hasrel( flt, XXX ) == 0b00
                bitvector_match( MEM, offset_A+9,  0b001 )   # hasrel( XXX, flt, int ) == 0b001
                bitvector_match( MEM, offset_A+10, 0b010 )   # hasrel( XXX, int, flt ) == 0b010
                bitvector_match( MEM, offset_A+11, 0b001 )   # hasrel( flt, XXX, int ) == 0b001
                bitvector_match( MEM, offset_A+12, 0b010 )   # hasrel( flt, int, XXX ) == 0b010
                bitvector_match( MEM, offset_A+13, 0b100 )   # hasrel( int, XXX, flt ) == 0b100
                bitvector_match( MEM, offset_A+14, 0b100 )   # hasrel( int, flt, XXX ) == 0b100
                bitvector_match( MEM, offset_A+15, 0b0 )     # hasrel() == 0b0


            ###################################
            #         /-[int, M_INT, 100]-\
            #        /--[flt, M_FLT, 222]--> (A)  
            # (ROOT)  
            #        \ -[flt, M_FLT, 333]-> (B)
            #
            elif g.size == 3:
                # Arc matches to A
                bitvector_match( MEM, offset_A+0,  0b1 )     # hasrel( int ) == 0b1
                bitvector_match( MEM, offset_A+1,  0b1 )     # hasrel( flt ) == 0b1
                bitvector_match( MEM, offset_A+2,  0b0 )     # hasrel( XXX ) == 0b0
                bitvector_match( MEM, offset_A+3,  0b11 )    # hasrel( flt, int ) == 0b11
                bitvector_match( MEM, offset_A+4,  0b11 )    # hasrel( int, flt ) == 0b11
                bitvector_match( MEM, offset_A+5,  0b01 )    # hasrel( XXX, int ) == 0b01
                bitvector_match( MEM, offset_A+6,  0b10 )    # hasrel( int, XXX ) == 0b10
                bitvector_match( MEM, offset_A+7,  0b01 )    # hasrel( XXX, flt ) == 0b01
                bitvector_match( MEM, offset_A+8,  0b10 )    # hasrel( flt, XXX ) == 0b10
                bitvector_match( MEM, offset_A+9,  0b011 )   # hasrel( XXX, flt, int ) == 0b011
                bitvector_match( MEM, offset_A+10, 0b011 )   # hasrel( XXX, int, flt ) == 0b011
                bitvector_match( MEM, offset_A+11, 0b101 )   # hasrel( flt, XXX, int ) == 0b101
                bitvector_match( MEM, offset_A+12, 0b110 )   # hasrel( flt, int, XXX ) == 0b110
                bitvector_match( MEM, offset_A+13, 0b101 )   # hasrel( int, XXX, flt ) == 0b101
                bitvector_match( MEM, offset_A+14, 0b110 )   # hasrel( int, flt, XXX ) == 0b110
                bitvector_match( MEM, offset_A+15, 0b0 )     # hasrel() == 0b0


            ###################################
            #          /-[int, M_INT, 100]-\
            #         /--[flt, M_FLT, 222]--> (A)  
            #        /---[XXX, M_INT, 400]-/ 
            # (ROOT)  
            #        \ -[flt, M_FLT, 333]-> (B)
            #
            elif g.size >= 4:

                # Arc matches to A
                bitvector_match( MEM, offset_A+0,  0b1 )     # hasrel( int ) == 0b1
                bitvector_match( MEM, offset_A+1,  0b1 )     # hasrel( flt ) == 0b1
                bitvector_match( MEM, offset_A+2,  0b1 )     # hasrel( XXX ) == 0b1
                bitvector_match( MEM, offset_A+3,  0b11 )    # hasrel( flt, int ) == 0b11
                bitvector_match( MEM, offset_A+4,  0b11 )    # hasrel( int, flt ) == 0b11
                bitvector_match( MEM, offset_A+5,  0b11 )    # hasrel( XXX, int ) == 0b11
                bitvector_match( MEM, offset_A+6,  0b11 )    # hasrel( int, XXX ) == 0b11
                bitvector_match( MEM, offset_A+7,  0b11 )    # hasrel( XXX, flt ) == 0b11
                bitvector_match( MEM, offset_A+8,  0b11 )    # hasrel( flt, XXX ) == 0b11
                bitvector_match( MEM, offset_A+9,  0b111 )   # hasrel( XXX, flt, int ) == 0b111
                bitvector_match( MEM, offset_A+10, 0b111 )   # hasrel( XXX, int, flt ) == 0b111
                bitvector_match( MEM, offset_A+11, 0b111 )   # hasrel( flt, XXX, int ) == 0b111
                bitvector_match( MEM, offset_A+12, 0b111 )   # hasrel( flt, int, XXX ) == 0b111
                bitvector_match( MEM, offset_A+13, 0b111 )   # hasrel( int, XXX, flt ) == 0b111
                bitvector_match( MEM, offset_A+14, 0b111 )   # hasrel( int, flt, XXX ) == 0b111
                bitvector_match( MEM, offset_A+15, 0b0 )     # hasrel() == 0b0

    g.Erase()




###############################################################################
# TEST_SyntheticArc_synarc_hasmod
#
###############################################################################
def TEST_SyntheticArc_synarc_hasmod():
    """
    Synthetic Arc - synarc.hasmod()
    test_level=3101
    """
    g = Graph( "synthetic_arcs" )
    g.Truncate()


    ROOT = g.NewVertex( "ROOT" )

    #
    #
    #           /-[int, M_INT, 100]-\           # 1
    #          ---[flt, M_FLT, 222]---> (A)     # 3
    #        /  \-[cnt, M_CNT, 400]-/           # 4
    # (ROOT) 
    #        \
    #           -[flt, M_FLT, 333] -> (B)       # 2
    #
    ADD_ARCS = [
        [("int", M_INT, 100), "A"],
        [("flt", M_FLT, 222), "B"],
        [("flt", M_FLT, 333), "A"],
        [("cnt", M_CNT, 400), "A"]
    ]


    NOISE_ARCS = [
        [('int', M_INT, n), str(n)] for n in range(1000)
    ]

    MEM = g.Memory( 1<<20 )


    for arc, term in ADD_ARCS + NOISE_ARCS:

        g.Connect( ROOT, arc, term )

        MEM.Reset()

        result_hasmod = g.Neighborhood(
            id      =   "ROOT",
            fields  =   F_ID,
            result  =   R_SIMPLE,
            arc     =   D_OUT,
            collect =   C_SCAN,
            memory  =   MEM,
            filter  =   """
                        bv1_0 = synarc.hasmod( M_INT );
                        bv1_1 = synarc.hasmod( M_FLT );
                        bv1_2 = synarc.hasmod( M_CNT );

                        bv2_3 = synarc.hasmod( M_FLT, M_INT );
                        bv2_4 = synarc.hasmod( M_INT, M_FLT );
                        bv2_5 = synarc.hasmod( M_CNT, M_INT );
                        bv2_6 = synarc.hasmod( M_INT, M_CNT );
                        bv2_7 = synarc.hasmod( M_CNT, M_FLT );
                        bv2_8 = synarc.hasmod( M_FLT, M_CNT );

                        bv3_9 = synarc.hasmod( M_CNT, M_FLT, M_INT );
                        bv3_10 = synarc.hasmod( M_CNT, M_INT, M_FLT );
                        bv3_11 = synarc.hasmod( M_FLT, M_CNT, M_INT );
                        bv3_12 = synarc.hasmod( M_FLT, M_INT, M_CNT );
                        bv3_13 = synarc.hasmod( M_INT, M_CNT, M_FLT );
                        bv3_14 = synarc.hasmod( M_INT, M_FLT, M_CNT );

                        bv0_15 = synarc.hasmod();

                        returnif( !.arc.issyn, false );

                        rwrite( R1, bv1_0, bv1_1, bv1_2,
                                    bv2_3, bv2_4, bv2_5, bv2_6, bv2_7, bv2_8,
                                    bv3_9, bv3_10, bv3_11, bv3_12, bv3_13, bv3_14,
                                    bv0_15 );

                        collectif( 1 )
                        """
        )

        #################################
        # (ROOT) -[int, M_INT, 100]-> (A)
        #
        if g.size == 1:
            Expect( len(result_hasmod) == 1 and result_hasmod[0] == "A",      "Single synthetic arc to 'A', got %s" % str(result_hasmod) )
            for item in MEM[0:16]:
                Expect( type(item) is list,                     "Bitvector (encoded as singleton list) expected, got %s" % type(item) )
            # Arc matches to A
            bitvector_match( MEM, 0,  0b1 )     # hasmod( M_INT ) == 0b1
            bitvector_match( MEM, 1,  0b0 )     # hasmod( M_FLT ) == 0b0
            bitvector_match( MEM, 2,  0b0 )     # hasmod( M_CNT ) == 0b0
            bitvector_match( MEM, 3,  0b01 )    # hasmod( M_FLT, M_INT ) == 0b01
            bitvector_match( MEM, 4,  0b10 )    # hasmod( M_INT, M_FLT ) == 0b10
            bitvector_match( MEM, 5,  0b01 )    # hasmod( M_CNT, M_INT ) == 0b01
            bitvector_match( MEM, 6,  0b10 )    # hasmod( M_INT, M_CNT ) == 0b10
            bitvector_match( MEM, 7,  0b00 )    # hasmod( M_CNT, M_FLT ) == 0b00
            bitvector_match( MEM, 8,  0b00 )    # hasmod( M_FLT, M_CNT ) == 0b00
            bitvector_match( MEM, 9,  0b001 )   # hasmod( M_CNT, M_FLT, M_INT ) == 0b001
            bitvector_match( MEM, 10, 0b010 )   # hasmod( M_CNT, M_INT, M_FLT ) == 0b010
            bitvector_match( MEM, 11, 0b001 )   # hasmod( M_FLT, M_CNT, M_INT ) == 0b001
            bitvector_match( MEM, 12, 0b010 )   # hasmod( M_FLT, M_INT, M_CNT ) == 0b010
            bitvector_match( MEM, 13, 0b100 )   # hasmod( M_INT, M_CNT, M_FLT ) == 0b100
            bitvector_match( MEM, 14, 0b100 )   # hasmod( M_INT, M_FLT, M_CNT ) == 0b100
            bitvector_match( MEM, 15, 0b0 )     # hasmod() == 0b0
            end = MEM[16]
            Expect( type(end) is int and end == 0,              "Should not have written beyond end, found %s (%s)" % (end, type(end)) )


        ###################################
        # Arcs to A and B
        #
        # Only simple arc to B
        #   (ROOT) -[flt, M_FLT, 333]-> (B)
        # 
        # More arcs to A, plus other arcs 
        # 
        else:
            Expect( 'A' in result_hasmod and 'B' in result_hasmod, "Result should include arcs to 'A' and 'B'" )
            
            offset_A = result_hasmod.index('A')*16
            offset_B = result_hasmod.index('B')*16

            end = MEM[ 16 * len(result_hasmod )]
            Expect( type(end) is int and end == 0,              "Should not have written beyond end, found %s (%s)" % (end, type(end)) )

            # Arc matches to B
            bitvector_match( MEM, offset_B+0,  0b0 )     # hasmod( M_INT ) == 0b0
            bitvector_match( MEM, offset_B+1,  0b1 )     # hasmod( M_FLT ) == 0b1
            bitvector_match( MEM, offset_B+2,  0b0 )     # hasmod( M_CNT ) == 0b0
            bitvector_match( MEM, offset_B+3,  0b10 )    # hasmod( M_FLT, M_INT ) == 0b10
            bitvector_match( MEM, offset_B+4,  0b01 )    # hasmod( M_INT, M_FLT ) == 0b01
            bitvector_match( MEM, offset_B+5,  0b00 )    # hasmod( M_CNT, M_INT ) == 0b00
            bitvector_match( MEM, offset_B+6,  0b00 )    # hasmod( M_INT, M_CNT ) == 0b00
            bitvector_match( MEM, offset_B+7,  0b01 )    # hasmod( M_CNT, M_FLT ) == 0b01
            bitvector_match( MEM, offset_B+8,  0b10 )    # hasmod( M_FLT, M_CNT ) == 0b10
            bitvector_match( MEM, offset_B+9,  0b010 )   # hasmod( M_CNT, M_FLT, M_INT ) == 0b010
            bitvector_match( MEM, offset_B+10, 0b001 )   # hasmod( M_CNT, M_INT, M_FLT ) == 0b001
            bitvector_match( MEM, offset_B+11, 0b100 )   # hasmod( M_FLT, M_CNT, M_INT ) == 0b100
            bitvector_match( MEM, offset_B+12, 0b100 )   # hasmod( M_FLT, M_INT, M_CNT ) == 0b100
            bitvector_match( MEM, offset_B+13, 0b001 )   # hasmod( M_INT, M_CNT, M_FLT ) == 0b001
            bitvector_match( MEM, offset_B+14, 0b010 )   # hasmod( M_INT, M_FLT, M_CNT ) == 0b010
            bitvector_match( MEM, offset_B+15, 0b0 )     # hasmod() == 0b0

            ###################################
            #        / -[int, M_INT, 100]-> (A)
            # (ROOT)  
            #        \ -[flt, M_FLT, 333]-> (B)
            #
            if g.size == 2:
                # Arc matches to A
                bitvector_match( MEM, offset_A+0,  0b1 )     # hasmod( M_INT ) == 0b1
                bitvector_match( MEM, offset_A+1,  0b0 )     # hasmod( M_FLT ) == 0b0
                bitvector_match( MEM, offset_A+2,  0b0 )     # hasmod( M_CNT ) == 0b0
                bitvector_match( MEM, offset_A+3,  0b01 )    # hasmod( M_FLT, M_INT ) == 0b01
                bitvector_match( MEM, offset_A+4,  0b10 )    # hasmod( M_INT, M_FLT ) == 0b10
                bitvector_match( MEM, offset_A+5,  0b01 )    # hasmod( M_CNT, M_INT ) == 0b01
                bitvector_match( MEM, offset_A+6,  0b10 )    # hasmod( M_INT, M_CNT ) == 0b10
                bitvector_match( MEM, offset_A+7,  0b00 )    # hasmod( M_CNT, M_FLT ) == 0b00
                bitvector_match( MEM, offset_A+8,  0b00 )    # hasmod( M_FLT, M_CNT ) == 0b00
                bitvector_match( MEM, offset_A+9,  0b001 )   # hasmod( M_CNT, M_FLT, M_INT ) == 0b001
                bitvector_match( MEM, offset_A+10, 0b010 )   # hasmod( M_CNT, M_INT, M_FLT ) == 0b010
                bitvector_match( MEM, offset_A+11, 0b001 )   # hasmod( M_FLT, M_CNT, M_INT ) == 0b001
                bitvector_match( MEM, offset_A+12, 0b010 )   # hasmod( M_FLT, M_INT, M_CNT ) == 0b010
                bitvector_match( MEM, offset_A+13, 0b100 )   # hasmod( M_INT, M_CNT, M_FLT ) == 0b100
                bitvector_match( MEM, offset_A+14, 0b100 )   # hasmod( M_INT, M_FLT, M_CNT ) == 0b100
                bitvector_match( MEM, offset_A+15, 0b0 )     # hasmod() == 0b0


            ###################################
            #         /-[int, M_INT, 100]-\
            #        /--[flt, M_FLT, 222]--> (A)  
            # (ROOT)  
            #        \ -[flt, M_FLT, 333]-> (B)
            #
            elif g.size == 3:
                # Arc matches to A
                bitvector_match( MEM, offset_A+0,  0b1 )     # hasmod( M_INT ) == 0b1
                bitvector_match( MEM, offset_A+1,  0b1 )     # hasmod( M_FLT ) == 0b1
                bitvector_match( MEM, offset_A+2,  0b0 )     # hasmod( M_CNT ) == 0b0
                bitvector_match( MEM, offset_A+3,  0b11 )    # hasmod( M_FLT, M_INT ) == 0b11
                bitvector_match( MEM, offset_A+4,  0b11 )    # hasmod( M_INT, M_FLT ) == 0b11
                bitvector_match( MEM, offset_A+5,  0b01 )    # hasmod( M_CNT, M_INT ) == 0b01
                bitvector_match( MEM, offset_A+6,  0b10 )    # hasmod( M_INT, M_CNT ) == 0b10
                bitvector_match( MEM, offset_A+7,  0b01 )    # hasmod( M_CNT, M_FLT ) == 0b01
                bitvector_match( MEM, offset_A+8,  0b10 )    # hasmod( M_FLT, M_CNT ) == 0b10
                bitvector_match( MEM, offset_A+9,  0b011 )   # hasmod( M_CNT, M_FLT, M_INT ) == 0b011
                bitvector_match( MEM, offset_A+10, 0b011 )   # hasmod( M_CNT, M_INT, M_FLT ) == 0b011
                bitvector_match( MEM, offset_A+11, 0b101 )   # hasmod( M_FLT, M_CNT, M_INT ) == 0b101
                bitvector_match( MEM, offset_A+12, 0b110 )   # hasmod( M_FLT, M_INT, M_CNT ) == 0b110
                bitvector_match( MEM, offset_A+13, 0b101 )   # hasmod( M_INT, M_CNT, M_FLT ) == 0b101
                bitvector_match( MEM, offset_A+14, 0b110 )   # hasmod( M_INT, M_FLT, M_CNT ) == 0b110
                bitvector_match( MEM, offset_A+15, 0b0 )     # hasmod() == 0b0


            ###################################
            #          /-[int, M_INT, 100]-\
            #         /--[flt, M_FLT, 222]--> (A)  
            #        /---[M_CNT, M_INT, 400]-/ 
            # (ROOT)  
            #        \ -[flt, M_FLT, 333]-> (B)
            #
            elif g.size >= 4:

                # Arc matches to A
                bitvector_match( MEM, offset_A+0,  0b1 )     # hasmod( M_INT ) == 0b1
                bitvector_match( MEM, offset_A+1,  0b1 )     # hasmod( M_FLT ) == 0b1
                bitvector_match( MEM, offset_A+2,  0b1 )     # hasmod( M_CNT ) == 0b1
                bitvector_match( MEM, offset_A+3,  0b11 )    # hasmod( M_FLT, M_INT ) == 0b11
                bitvector_match( MEM, offset_A+4,  0b11 )    # hasmod( M_INT, M_FLT ) == 0b11
                bitvector_match( MEM, offset_A+5,  0b11 )    # hasmod( M_CNT, M_INT ) == 0b11
                bitvector_match( MEM, offset_A+6,  0b11 )    # hasmod( M_INT, M_CNT ) == 0b11
                bitvector_match( MEM, offset_A+7,  0b11 )    # hasmod( M_CNT, M_FLT ) == 0b11
                bitvector_match( MEM, offset_A+8,  0b11 )    # hasmod( M_FLT, M_CNT ) == 0b11
                bitvector_match( MEM, offset_A+9,  0b111 )   # hasmod( M_CNT, M_FLT, M_INT ) == 0b111
                bitvector_match( MEM, offset_A+10, 0b111 )   # hasmod( M_CNT, M_INT, M_FLT ) == 0b111
                bitvector_match( MEM, offset_A+11, 0b111 )   # hasmod( M_FLT, M_CNT, M_INT ) == 0b111
                bitvector_match( MEM, offset_A+12, 0b111 )   # hasmod( M_FLT, M_INT, M_CNT ) == 0b111
                bitvector_match( MEM, offset_A+13, 0b111 )   # hasmod( M_INT, M_CNT, M_FLT ) == 0b111
                bitvector_match( MEM, offset_A+14, 0b111 )   # hasmod( M_INT, M_FLT, M_CNT ) == 0b111
                bitvector_match( MEM, offset_A+15, 0b0 )     # hasmod() == 0b0



    g.Erase()




###############################################################################
# TEST_SyntheticArc_synarc_hasrelmod
#
###############################################################################
def TEST_SyntheticArc_synarc_hasrelmod():
    """
    Synthetic Arc - synarc.hasrelmod()
    test_level=3101
    """
    g = Graph( "synthetic_arcs" )
    g.Truncate()


    ROOT = g.NewVertex( "ROOT" )

    #
    #
    #           /-[int, M_INT, 100]-\           # 1
    #          ---[flt, M_FLT, 222]---> (A)     # 3
    #        /  \-[cnt, M_CNT, 400]-/           # 4
    # (ROOT) 
    #        \
    #           -[flt, M_FLT, 333] -> (B)       # 2
    #
    ADD_ARCS = [
        [("int", M_INT, 100), "A"],
        [("flt", M_FLT, 222), "B"],
        [("flt", M_FLT, 333), "A"],
        [("cnt", M_CNT, 400), "A"]
    ]


    NOISE_ARCS = [
        [('int', M_INT, n), str(n)] for n in range(1000)
    ]

    MEM = g.Memory( 1<<20 )


    for arc, term in ADD_ARCS + NOISE_ARCS:

        g.Connect( ROOT, arc, term )

        MEM.Reset()

        result_hasrelmod = g.Neighborhood(
            id      =   "ROOT",
            fields  =   F_ID,
            result  =   R_SIMPLE,
            arc     =   D_OUT,
            collect =   C_SCAN,
            memory  =   MEM,
            filter  =   """
                        bv1_0 = synarc.hasrelmod( 'int', M_INT );
                        bv1_1 = synarc.hasrelmod( 'flt', M_FLT );
                        bv1_2 = synarc.hasrelmod( 'cnt', M_CNT );

                        bv2_3 = synarc.hasrelmod( 'flt', M_FLT, 'int', M_INT );
                        bv2_4 = synarc.hasrelmod( 'int', M_INT, 'flt', M_FLT );
                        bv2_5 = synarc.hasrelmod( 'cnt', M_CNT, 'int', M_INT );
                        bv2_6 = synarc.hasrelmod( 'int', M_INT, 'cnt', M_CNT );
                        bv2_7 = synarc.hasrelmod( 'cnt', M_CNT, 'flt', M_FLT );
                        bv2_8 = synarc.hasrelmod( 'flt', M_FLT, 'cnt', M_CNT );

                        bv3_9 = synarc.hasrelmod( 'cnt', M_CNT, 'flt', M_FLT, 'int', M_INT );
                        bv3_10 = synarc.hasrelmod( 'cnt', M_CNT, 'int', M_INT, 'flt', M_FLT );
                        bv3_11 = synarc.hasrelmod( 'flt', M_FLT, 'cnt', M_CNT, 'int', M_INT );
                        bv3_12 = synarc.hasrelmod( 'flt', M_FLT, 'int', M_INT, 'cnt', M_CNT );
                        bv3_13 = synarc.hasrelmod( 'int', M_INT, 'cnt', M_CNT, 'flt', M_FLT );
                        bv3_14 = synarc.hasrelmod( 'int', M_INT, 'flt', M_FLT, 'cnt', M_CNT );

                        bv0_15 = synarc.hasrelmod();

                        bv3miss_16 = synarc.hasrelmod( 'int', M_FLT, 'flt', M_CNT, 'cnt', M_INT );
                        bv2miss1hit_17 = synarc.hasrelmod( 'int', M_CNT, 'flt', M_FLT, 'cnt', M_INT );
                        bv3hit1ignore_18 = synarc.hasrelmod( 'int', M_INT, 'flt', M_FLT, 'cnt', M_CNT, 'int' );

                        returnif( !.arc.issyn, false );

                        rwrite( R1, bv1_0, bv1_1, bv1_2,
                                    bv2_3, bv2_4, bv2_5, bv2_6, bv2_7, bv2_8,
                                    bv3_9, bv3_10, bv3_11, bv3_12, bv3_13, bv3_14,
                                    bv0_15,
                                    bv3miss_16, bv2miss1hit_17, bv3hit1ignore_18
                                    );

                        collectif( 1 )
                        """
        )

        #################################
        # (ROOT) -[int, M_INT, 100]-> (A)
        #
        if g.size == 1:
            Expect( len(result_hasrelmod) == 1 and result_hasrelmod[0] == "A",      "Single synthetic arc to 'A', got %s" % str(result_hasrelmod) )
            for item in MEM[0:19]:
                Expect( type(item) is list,                     "Bitvector (encoded as singleton list) expected, got %s" % type(item) )
            # Arc matches to A
            bitvector_match( MEM, 0,  0b1 )     # hasrelmod( 'int', M_INT ) == 0b1
            bitvector_match( MEM, 1,  0b0 )     # hasrelmod( 'flt', M_FLT ) == 0b0
            bitvector_match( MEM, 2,  0b0 )     # hasrelmod( 'cnt', M_CNT ) == 0b0
            bitvector_match( MEM, 3,  0b01 )    # hasrelmod( 'flt', M_FLT, 'int', M_INT ) == 0b01
            bitvector_match( MEM, 4,  0b10 )    # hasrelmod( 'int', M_INT, 'flt', M_FLT ) == 0b10
            bitvector_match( MEM, 5,  0b01 )    # hasrelmod( 'cnt', M_CNT, 'int', M_INT ) == 0b01
            bitvector_match( MEM, 6,  0b10 )    # hasrelmod( 'int', M_INT, 'cnt', M_CNT ) == 0b10
            bitvector_match( MEM, 7,  0b00 )    # hasrelmod( 'cnt', M_CNT, 'flt', M_FLT ) == 0b00
            bitvector_match( MEM, 8,  0b00 )    # hasrelmod( 'flt', M_FLT, 'cnt', M_CNT ) == 0b00
            bitvector_match( MEM, 9,  0b001 )   # hasrelmod( 'cnt', M_CNT, 'flt', M_FLT, 'int', M_INT ) == 0b001
            bitvector_match( MEM, 10, 0b010 )   # hasrelmod( 'cnt', M_CNT, 'int', M_INT, 'flt', M_FLT ) == 0b010
            bitvector_match( MEM, 11, 0b001 )   # hasrelmod( 'flt', M_FLT, 'cnt', M_CNT, 'int', M_INT ) == 0b001
            bitvector_match( MEM, 12, 0b010 )   # hasrelmod( 'flt', M_FLT, 'int', M_INT, 'cnt', M_CNT ) == 0b010
            bitvector_match( MEM, 13, 0b100 )   # hasrelmod( 'int', M_INT, 'cnt', M_CNT, 'flt', M_FLT ) == 0b100
            bitvector_match( MEM, 14, 0b100 )   # hasrelmod( 'int', M_INT, 'flt', M_FLT, 'cnt', M_CNT ) == 0b100
            bitvector_match( MEM, 15, 0b0 )     # hasrelmod() == 0b0
            bitvector_match( MEM, 16, 0b0 )     # hasrelmod( 'int', M_FLT, 'flt', M_CNT, 'cnt', M_INT ) == 0b0
            bitvector_match( MEM, 17, 0b0 )     # hasrelmod( 'int', M_CNT, 'flt', M_FLT, 'cnt', M_INT ) == 0b0
            bitvector_match( MEM, 18, 0b100 )   # hasrelmod( 'int', M_INT, 'flt', M_FLT, 'cnt', M_CNT, 'int' ) == 0b100
            end = MEM[19]
            Expect( type(end) is int and end == 0,              "Should not have written beyond end, found %s (%s)" % (end, type(end)) )


        ###################################
        # Arcs to A and B
        #
        # Only simple arc to B
        #   (ROOT) -[flt, M_FLT, 333]-> (B)
        # 
        # More arcs to A, plus other arcs 
        # 
        else:
            Expect( 'A' in result_hasrelmod and 'B' in result_hasrelmod, "Result should include arcs to 'A' and 'B'" )
            
            offset_A = result_hasrelmod.index('A')*19
            offset_B = result_hasrelmod.index('B')*19

            end = MEM[ 19 * len(result_hasrelmod )]
            Expect( type(end) is int and end == 0,              "Should not have written beyond end, found %s (%s)" % (end, type(end)) )

            # Arc matches to B
            bitvector_match( MEM, offset_B+0,  0b0 )     # hasrelmod( 'int', M_INT ) == 0b0
            bitvector_match( MEM, offset_B+1,  0b1 )     # hasrelmod( 'flt', M_FLT ) == 0b1
            bitvector_match( MEM, offset_B+2,  0b0 )     # hasrelmod( 'cnt', M_CNT ) == 0b0
            bitvector_match( MEM, offset_B+3,  0b10 )    # hasrelmod( 'flt', M_FLT, 'int', M_INT ) == 0b10
            bitvector_match( MEM, offset_B+4,  0b01 )    # hasrelmod( 'int', M_INT, 'flt', M_FLT ) == 0b01
            bitvector_match( MEM, offset_B+5,  0b00 )    # hasrelmod( 'cnt', M_CNT, 'int', M_INT ) == 0b00
            bitvector_match( MEM, offset_B+6,  0b00 )    # hasrelmod( 'int', M_INT, 'cnt', M_CNT ) == 0b00
            bitvector_match( MEM, offset_B+7,  0b01 )    # hasrelmod( 'cnt', M_CNT, 'flt', M_FLT ) == 0b01
            bitvector_match( MEM, offset_B+8,  0b10 )    # hasrelmod( 'flt', M_FLT, 'cnt', M_CNT ) == 0b10
            bitvector_match( MEM, offset_B+9,  0b010 )   # hasrelmod( 'cnt', M_CNT, 'flt', M_FLT, 'int', M_INT ) == 0b010
            bitvector_match( MEM, offset_B+10, 0b001 )   # hasrelmod( 'cnt', M_CNT, 'int', M_INT, 'flt', M_FLT ) == 0b001
            bitvector_match( MEM, offset_B+11, 0b100 )   # hasrelmod( 'flt', M_FLT, 'cnt', M_CNT, 'int', M_INT ) == 0b100
            bitvector_match( MEM, offset_B+12, 0b100 )   # hasrelmod( 'flt', M_FLT, 'int', M_INT, 'cnt', M_CNT ) == 0b100
            bitvector_match( MEM, offset_B+13, 0b001 )   # hasrelmod( 'int', M_INT, 'cnt', M_CNT, 'flt', M_FLT ) == 0b001
            bitvector_match( MEM, offset_B+14, 0b010 )   # hasrelmod( 'int', M_INT, 'flt', M_FLT, 'cnt', M_CNT ) == 0b010
            bitvector_match( MEM, offset_B+15, 0b0 )     # hasrelmod() == 0b0
            bitvector_match( MEM, offset_B+16, 0b0 )     # hasrelmod( 'int', M_FLT, 'flt', M_CNT, 'cnt', M_INT ) == 0b0
            bitvector_match( MEM, offset_B+17, 0b010 )   # hasrelmod( 'int', M_CNT, 'flt', M_FLT, 'cnt', M_INT ) == 0b010
            bitvector_match( MEM, offset_B+18, 0b010 )   # hasrelmod( 'int', M_INT, 'flt', M_FLT, 'cnt', M_CNT, 'int' ) == 0b010


            ###################################
            #        / -[int, M_INT, 100]-> (A)
            # (ROOT)  
            #        \ -[flt, M_FLT, 333]-> (B)
            #
            if g.size == 2:
                # Arc matches to A
                bitvector_match( MEM, offset_A+0,  0b1 )     # hasrelmod( 'int', M_INT ) == 0b1
                bitvector_match( MEM, offset_A+1,  0b0 )     # hasrelmod( 'flt', M_FLT ) == 0b0
                bitvector_match( MEM, offset_A+2,  0b0 )     # hasrelmod( 'cnt', M_CNT ) == 0b0
                bitvector_match( MEM, offset_A+3,  0b01 )    # hasrelmod( 'flt', M_FLT, 'int', M_INT ) == 0b01
                bitvector_match( MEM, offset_A+4,  0b10 )    # hasrelmod( 'int', M_INT, 'flt', M_FLT ) == 0b10
                bitvector_match( MEM, offset_A+5,  0b01 )    # hasrelmod( 'cnt', M_CNT, 'int', M_INT ) == 0b01
                bitvector_match( MEM, offset_A+6,  0b10 )    # hasrelmod( 'int', M_INT, 'cnt', M_CNT ) == 0b10
                bitvector_match( MEM, offset_A+7,  0b00 )    # hasrelmod( 'cnt', M_CNT, 'flt', M_FLT ) == 0b00
                bitvector_match( MEM, offset_A+8,  0b00 )    # hasrelmod( 'flt', M_FLT, 'cnt', M_CNT ) == 0b00
                bitvector_match( MEM, offset_A+9,  0b001 )   # hasrelmod( 'cnt', M_CNT, 'flt', M_FLT, 'int', M_INT ) == 0b001
                bitvector_match( MEM, offset_A+10, 0b010 )   # hasrelmod( 'cnt', M_CNT, 'int', M_INT, 'flt', M_FLT ) == 0b010
                bitvector_match( MEM, offset_A+11, 0b001 )   # hasrelmod( 'flt', M_FLT, 'cnt', M_CNT, 'int', M_INT ) == 0b001
                bitvector_match( MEM, offset_A+12, 0b010 )   # hasrelmod( 'flt', M_FLT, 'int', M_INT, 'cnt', M_CNT ) == 0b010
                bitvector_match( MEM, offset_A+13, 0b100 )   # hasrelmod( 'int', M_INT, 'cnt', M_CNT, 'flt', M_FLT ) == 0b100
                bitvector_match( MEM, offset_A+14, 0b100 )   # hasrelmod( 'int', M_INT, 'flt', M_FLT, 'cnt', M_CNT ) == 0b100
                bitvector_match( MEM, offset_A+15, 0b0 )     # hasrelmod() == 0b0
                bitvector_match( MEM, offset_A+16, 0b0 )     # hasrelmod( 'int', M_FLT, 'flt', M_CNT, 'cnt', M_INT ) == 0b0
                bitvector_match( MEM, offset_A+17, 0b0 )     # hasrelmod( 'int', M_CNT, 'flt', M_FLT, 'cnt', M_INT ) == 0b0
                bitvector_match( MEM, offset_A+18, 0b100 )   # hasrelmod( 'int', M_INT, 'flt', M_FLT, 'cnt', M_CNT, 'int' ) == 0b100



            ###################################
            #         /-[int, M_INT, 100]-\
            #        /--[flt, M_FLT, 222]--> (A)  
            # (ROOT)  
            #        \ -[flt, M_FLT, 333]-> (B)
            #
            elif g.size == 3:
                # Arc matches to A
                bitvector_match( MEM, offset_A+0,  0b1 )     # hasrelmod( 'int', M_INT ) == 0b1
                bitvector_match( MEM, offset_A+1,  0b1 )     # hasrelmod( 'flt', M_FLT ) == 0b1
                bitvector_match( MEM, offset_A+2,  0b0 )     # hasrelmod( 'cnt', M_CNT ) == 0b0
                bitvector_match( MEM, offset_A+3,  0b11 )    # hasrelmod( 'flt', M_FLT, 'int', M_INT ) == 0b11
                bitvector_match( MEM, offset_A+4,  0b11 )    # hasrelmod( 'int', M_INT, 'flt', M_FLT ) == 0b11
                bitvector_match( MEM, offset_A+5,  0b01 )    # hasrelmod( 'cnt', M_CNT, 'int', M_INT ) == 0b01
                bitvector_match( MEM, offset_A+6,  0b10 )    # hasrelmod( 'int', M_INT, 'cnt', M_CNT ) == 0b10
                bitvector_match( MEM, offset_A+7,  0b01 )    # hasrelmod( 'cnt', M_CNT, 'flt', M_FLT ) == 0b01
                bitvector_match( MEM, offset_A+8,  0b10 )    # hasrelmod( 'flt', M_FLT, 'cnt', M_CNT ) == 0b10
                bitvector_match( MEM, offset_A+9,  0b011 )   # hasrelmod( 'cnt', M_CNT, 'flt', M_FLT, 'int', M_INT ) == 0b011
                bitvector_match( MEM, offset_A+10, 0b011 )   # hasrelmod( 'cnt', M_CNT, 'int', M_INT, 'flt', M_FLT ) == 0b011
                bitvector_match( MEM, offset_A+11, 0b101 )   # hasrelmod( 'flt', M_FLT, 'cnt', M_CNT, 'int', M_INT ) == 0b101
                bitvector_match( MEM, offset_A+12, 0b110 )   # hasrelmod( 'flt', M_FLT, 'int', M_INT, 'cnt', M_CNT ) == 0b110
                bitvector_match( MEM, offset_A+13, 0b101 )   # hasrelmod( 'int', M_INT, 'cnt', M_CNT, 'flt', M_FLT ) == 0b101
                bitvector_match( MEM, offset_A+14, 0b110 )   # hasrelmod( 'int', M_INT, 'flt', M_FLT, 'cnt', M_CNT ) == 0b110
                bitvector_match( MEM, offset_A+15, 0b0 )     # hasrelmod() == 0b0
                bitvector_match( MEM, offset_A+16, 0b0 )     # hasrelmod( 'int', M_FLT, 'flt', M_CNT, 'cnt', M_INT ) == 0b0
                bitvector_match( MEM, offset_A+17, 0b010 )   # hasrelmod( 'int', M_CNT, 'flt', M_FLT, 'cnt', M_INT ) == 0b010
                bitvector_match( MEM, offset_A+18, 0b110 )   # hasrelmod( 'int', M_INT, 'flt', M_FLT, 'cnt', M_CNT, 'int' ) == 0b110


            ###################################
            #          /-[int, M_INT, 100]-\
            #         /--[flt, M_FLT, 222]--> (A)  
            #        /---[M_CNT, M_INT, 400]-/ 
            # (ROOT)  
            #        \ -[flt, M_FLT, 333]-> (B)
            #
            elif g.size >= 4:

                # Arc matches to A
                bitvector_match( MEM, offset_A+0,  0b1 )     # hasrelmod( 'int', M_INT ) == 0b1
                bitvector_match( MEM, offset_A+1,  0b1 )     # hasrelmod( 'flt', M_FLT ) == 0b1
                bitvector_match( MEM, offset_A+2,  0b1 )     # hasrelmod( 'cnt', M_CNT ) == 0b1
                bitvector_match( MEM, offset_A+3,  0b11 )    # hasrelmod( 'flt', M_FLT, 'int', M_INT ) == 0b11
                bitvector_match( MEM, offset_A+4,  0b11 )    # hasrelmod( 'int', M_INT, 'flt', M_FLT ) == 0b11
                bitvector_match( MEM, offset_A+5,  0b11 )    # hasrelmod( 'cnt', M_CNT, 'int', M_INT ) == 0b11
                bitvector_match( MEM, offset_A+6,  0b11 )    # hasrelmod( 'int', M_INT, 'cnt', M_CNT ) == 0b11
                bitvector_match( MEM, offset_A+7,  0b11 )    # hasrelmod( 'cnt', M_CNT, 'flt', M_FLT ) == 0b11
                bitvector_match( MEM, offset_A+8,  0b11 )    # hasrelmod( 'flt', M_FLT, 'cnt', M_CNT ) == 0b11
                bitvector_match( MEM, offset_A+9,  0b111 )   # hasrelmod( 'cnt', M_CNT, 'flt', M_FLT, 'int', M_INT ) == 0b111
                bitvector_match( MEM, offset_A+10, 0b111 )   # hasrelmod( 'cnt', M_CNT, 'int', M_INT, 'flt', M_FLT ) == 0b111
                bitvector_match( MEM, offset_A+11, 0b111 )   # hasrelmod( 'flt', M_FLT, 'cnt', M_CNT, 'int', M_INT ) == 0b111
                bitvector_match( MEM, offset_A+12, 0b111 )   # hasrelmod( 'flt', M_FLT, 'int', M_INT, 'cnt', M_CNT ) == 0b111
                bitvector_match( MEM, offset_A+13, 0b111 )   # hasrelmod( 'int', M_INT, 'cnt', M_CNT, 'flt', M_FLT ) == 0b111
                bitvector_match( MEM, offset_A+14, 0b111 )   # hasrelmod( 'int', M_INT, 'flt', M_FLT, 'cnt', M_CNT ) == 0b111
                bitvector_match( MEM, offset_A+15, 0b0 )     # hasrelmod() == 0b0
                bitvector_match( MEM, offset_A+16, 0b0 )     # hasrelmod( 'int', M_FLT, 'flt', M_CNT, 'cnt', M_INT ) == 0b0
                bitvector_match( MEM, offset_A+17, 0b010 )   # hasrelmod( 'int', M_CNT, 'flt', M_FLT, 'cnt', M_INT ) == 0b010
                bitvector_match( MEM, offset_A+18, 0b111 )   # hasrelmod( 'int', M_INT, 'flt', M_FLT, 'cnt', M_CNT, 'int' ) == 0b111



    g.Erase()






###############################################################################
# TEST_SyntheticArc_synarc_value_single
#
###############################################################################
def TEST_SyntheticArc_synarc_value_single():
    """
    Synthetic Arc - synarc.value() single
    test_level=3101
    """
    g = Graph( "synthetic_arcs" )
    g.Truncate()

    ROOT = g.NewVertex( "ROOT" )


    ADD_ARCS = [
        [("int", M_INT, 100), "A"],
        [("flt", M_FLT, 222), "B"],
        [("flt", M_FLT, 333), "A"],
        [("XXX", M_INT, 400), "A"]
    ]

    for arc, term in ADD_ARCS:

        g.Connect( "ROOT", arc, term )

        #
        # Collect by traverser only
        #
        result = g.Neighborhood(
            id      =   "ROOT",
            hits    =   10,
            fields  =   F_AARC,
            result  =   R_DICT,
            arc     =   ("int", D_OUT, M_INT),
            filter  =   """
                        /* Match on synthetic arc */
                        synarc.value( 'int', M_INT ) == 100
                        """
        )

        Expect( len(result) == 1,                       "One arc returned, got %d" % len(result) )
        R = result[0]['arc']
        Expect( R == SYNTHETIC_OUTARC,                  "Synthetic arc collected by traverser, got %s" % R )


        #
        # Collect in filter and by traverser 
        #
        result = g.Neighborhood(
            id      =   "ROOT",
            hits    =   10,
            fields  =   F_AARC,
            result  =   R_DICT,
            arc     =   ("int", D_OUT, M_INT),
            filter  =   """
                        /* Collect synthetic value when not null
                           and return 1 if collected, 0 if not collected
                           (or -1 on error)
                        */
                        collect( 
                            /* Produce synthetic arc value
                               when processing synthetic arc,
                               otherwise produce null value
                            */
                            synarc.value( 'int', M_INT ) 
                        )
                        """
        )

        # Two synthetic arcs
        Expect( len(result) == 2,                       "Two arcs returned, got %d" % len(result) )
        # First collected in filter
        Expect( arc_dir( result, 0 ) == 'D_OUT',        "Outarc, got %s" % arc_dir( result, 0 ) )
        Expect( arc_rel( result, 0 ) == REL_SYN,        "Synthetic arc, got %s" % arc_rel( result, 0 ) )
        Expect( arc_mod( result, 0 ) == 'M_INT',        "M_INT, got %s" % arc_mod( result, 0 ) )
        Expect( arc_val( result, 0 ) == 100,            "100, got %s" % arc_val( result, 0 ) )
        # Second collected by traverser
        R = result[1]['arc']
        Expect( R == SYNTHETIC_OUTARC,                  "Synthetic arc collected by traverser, got %s" % R )


        #
        # Collect in filter only
        #
        result = g.Neighborhood(
            id      =   "ROOT",
            hits    =   10,
            fields  =   F_AARC,
            result  =   R_DICT,
            arc     =   ("int", D_OUT, M_INT),
            collect =   C_SCAN,
            filter  =   """
                        /* Collect synthetic value when not null
                           and return 1 if collected, 0 if not collected
                           (or -1 on error)
                        */
                        collect( 
                            /* Produce synthetic arc value
                               when processing synthetic arc,
                               otherwise produce null value
                            */
                            synarc.value( 'int', M_INT ) 
                        )
                        """
        )

        # One synthetic arc collected in filter
        Expect( len(result) == 1,                       "One arc returned, got %d" % len(result) )
        Expect( arc_dir( result, 0 ) == 'D_OUT',        "Outarc, got %s" % arc_dir( result, 0 ) )
        Expect( arc_rel( result, 0 ) == REL_SYN,        "Synthetic arc, got %s" % arc_rel( result, 0 ) )
        Expect( arc_mod( result, 0 ) == 'M_INT',        "M_INT, got %s" % arc_mod( result, 0 ) )
        Expect( arc_val( result, 0 ) == 100,            "100, got %s" % arc_val( result, 0 ) )


        #
        # Collect real arc by traverser and synthetic arc in filter
        #
        result = g.Neighborhood(
            id      =   "ROOT",
            hits    =   10,
            fields  =   F_AARC,
            result  =   R_DICT,
            arc     =   ("int", D_OUT, M_INT),
            filter  =   """
                        /* Attempt collection only when arc is synthetic,
                           and then the value will also be non-null so
                           collection will occur. Return the negated
                           condition so the filter is true when arc is
                           not synthetic, otherwise false.
                           This leads to collection by traverser first
                           when the arc is real (and no collection in 
                           filter), and then when the arc is synthetic
                           its value is collected in the filter but
                           not by traverser since filter result is false.
                        */
                        !collectif(
                            .arc.issyn,             // true when arc is synthetic
                            synarc.value( 'int', M_INT )  // non-null when ar is synthetic
                        )
                        """
        )

        # Two arcs, one real and one synthetic
        Expect( len(result) == 2,                       "Two arcs returned, got %d" % len(result) )
        # First (real) collected by traverser
        Expect( arc_dir( result, 0 ) == 'D_OUT',        "Outarc, got %s" % arc_dir( result, 0 ) )
        Expect( arc_rel( result, 0 ) == 'int',          "'int', got %s" % arc_rel( result, 0 ) )
        Expect( arc_mod( result, 0 ) == 'M_INT',        "M_INT, got %s" % arc_mod( result, 0 ) )
        Expect( arc_val( result, 0 ) == 100,            "100, got %s" % arc_val( result, 0 ) )
        # Second (synthetic) collected in filter
        Expect( arc_dir( result, 1 ) == 'D_OUT',        "Outarc, got %s" % arc_dir( result, 1 ) )
        Expect( arc_rel( result, 1 ) == REL_SYN,        "Synthetic arc, got %s" % arc_rel( result, 1 ) )
        Expect( arc_mod( result, 1 ) == 'M_INT',        "M_INT, got %s" % arc_mod( result, 1 ) )
        Expect( arc_val( result, 1 ) == 100,            "100, got %s" % arc_val( result, 1 ) )




    g.Erase()




###############################################################################
# TEST_SyntheticArc_synarc_value_multiple
#
###############################################################################
def TEST_SyntheticArc_synarc_value_multiple():
    """
    Synthetic Arc - synarc.value() multiple
    test_level=3101
    """
    g = Graph( "synthetic_arcs" )
    g.Truncate()

    ROOT = g.NewVertex( "ROOT" )


    ADD_ARCS = [
        [("x", M_INT, 100), "A"],
        [("y", M_FLT, 2.5), "A"],
        [("z", M_FLT, 1000), "A"]
    ]

    for arc, term in ADD_ARCS:
        
        g.Connect( ROOT, arc, term )

        for noise in range(10):

            pre_filter_noise = ";".join( [ "synarc.value( 'pre_noise_%d', M_INT )" % n for n in range(noise) ] )
            post_filter_noise = ";".join( [ "synarc.value( 'post_noise_%d', M_FLT )" % n for n in range(noise) ] )

            result = g.Neighborhood(
                id      =   "ROOT",
                hits    =   10,
                fields  =   F_AARC,
                result  =   R_DICT,
                arc     =   D_OUT,
                collect =   C_SCAN,
                filter  =   """
                            %s;
                            z = synarc.value( 'z', M_FLT ); // 1000.0
                            x = synarc.value( 'x', M_INT ); // 100
                            y = synarc.value( 'y', M_FLT ); // 2.5
                            returnif( anynan(x,y,z), false );
                            collect( z + x * y );
                            %s;
                            """ % (pre_filter_noise, post_filter_noise)
            )

            if g.size < 3:
                Expect( len(result) == 0,                   "No synthetic arc without all elements present in graph, got %s" % result )
            else:
                Expect( len(result) == 1,                   "One synthetic arc, got %s" % result )
                Expect( arc_dir( result ) == 'D_OUT',       "Outarc, got %s" % arc_dir( result ) )
                Expect( arc_rel( result ) == REL_SYN,       "Synthetic arc, got %s" % arc_rel( result ) )
                Expect( arc_mod( result ) == 'M_FLT',       "M_FLT, got %s" % arc_mod( result ) )
                ev = 1000.0 + 100 * 2.5
                Expect( arc_val( result ) == ev,            "%f, got %f" % (ev, arc_val( result )) )

    g.Erase()




###############################################################################
# TEST_SyntheticArc_synarc_decay
#
###############################################################################
def TEST_SyntheticArc_synarc_decay():
    """
    Synthetic Arc - synarc.decay()
    t_nominal=58
    test_level=3101
    """
    g = Graph( "synthetic_arcs" )
    g.Truncate()

    time.sleep( 2 )

    ROOT = g.NewVertex( "ROOT" )


    N_0 = 1000.0
    x_lifespan = 100

    ADD_ARCS = [
        [ ("x", M_FLT, N_0), "A", x_lifespan ],
        [ ("x", M_INT, 12345), "A", -1 ],
        [ ("y", M_FLT, 1e9), "A", 100*x_lifespan ]
    ]

    t0 = -1

    for arc, term, lifespan in ADD_ARCS:
        #
        if lifespan > 0:
            g.Connect( "ROOT", arc, term, lifespan )
        else:
            g.Connect( "ROOT", arc, term )

        if t0 < 0:
            t0 = g.ts

        print( "Added (ROOT)-%s->(%s)" % (list(arc), term) )

        time.sleep( 3 )

        loopmax = timestamp() + 15
        while timestamp() < loopmax:

            result = g.Neighborhood(
                id      =   "ROOT",
                timeout =   1000,
                hits    =   10,
                fields  =   F_AARC,
                result  =   R_DICT,
                arc     =   D_OUT,
                collect =   C_SCAN,
                filter  =   """
                            a = synarc.decay( 'x' );
                            b = synarc.xdecay( 'x' );
                            collect(a);
                            collect(b);
                            """
            )

            t1 = g.ts

            t_elapse = t1 - t0

            Expect( len(result) == 2,                           "Two synthetic arcs" )

            # synarc.decay result (linear)
            Expect( arc_dir( result, 0 ) == 'D_OUT',            "dir=D_OUT, got %s" % arc_dir( result, 0 ) )
            Expect( arc_rel( result, 0 ) == 'x',                "rel='x', got %s" % arc_rel( result, 0 ) )
            Expect( arc_mod( result, 0 ) == 'M_FLT',            "mod=M_FLT, got %s" % arc_mod( result, 0 ) )
            linval = arc_val( result, 0 )
            # Decay should have occurred
            Expect( linval < 1000.0,                            "linval N_t < 1000.0, got %f" % linval )
            # Should have decayed ~5% (with some lenience)
            r = N_0 / x_lifespan
            N_t = N_0 - r * t_elapse
            N_t_upper = N_0 - r * (t_elapse-1)
            N_t_lower = N_0 - r * (t_elapse+1)
            Expect( linval < N_t_upper and linval > N_t_lower,  "linval N_t ~ %.1f after %.1f sec decay, got %f" % (N_t, t_elapse, linval) )

            # arcxdecay result (exponential)
            Expect( arc_dir( result, 1 ) == 'D_OUT',            "dir=D_OUT, got %s" % arc_dir( result, 1 ) )
            Expect( arc_rel( result, 1 ) == 'x',                "rel='x', got %s" % arc_rel( result, 1 ) )
            Expect( arc_mod( result, 1 ) == 'M_FLT',            "mod=M_FLT, got %s" % arc_mod( result, 1 ) )
            expval = arc_val( result, 1 )
            # Decay should have occurred
            Expect( expval < 1000.0,                            "expval < 1000.0, got %f" % expval )
            # Should have decayed exponentially
            dcy = math.log(10**4) / x_lifespan
            N_t = N_0 * math.exp( -dcy * t_elapse )
            N_t_upper = N_0 * math.exp( -dcy * (t_elapse-1) )
            N_t_lower = N_0 * math.exp( -dcy * (t_elapse+1) )

            Expect( expval < N_t_upper and expval > N_t_lower,  "expval N_t ~ %.1f after %.1f sec decay, got %f" % (N_t, t_elapse, expval) )

            print( "t=%.4g lin:%.4g exp:%.4g" % (t_elapse, linval, expval) )
            time.sleep( 1 )
    
    g.Erase()




###############################################################################
# float_equal
#
###############################################################################
def float_equal( a, b, tolerance=0.0001 ):
    """
    """
    if b != 0:
        return ((a / b) - 1) < tolerance;
    else:
        return abs(a) < tolerance



###############################################################################
# TEST_SyntheticArc_synarc_decay_variants
#
###############################################################################
def TEST_SyntheticArc_synarc_decay_variants():
    """
    Synthetic Arc - synarc.decay() variants
    t_nominal=9
    test_level=3101
    """
    g = Graph( "synthetic_arcs" )
    g.Truncate()

    # Graph t0
    gt = g.Status()['time']
    g_0 = gt['current'] - gt['age'] 

    time.sleep( 2 )

    ROOT = g.NewVertex( "ROOT" )

    N_0 = 1024.0

    ############################################
    # DEFAULTS WITH NO TIMESTAMPS
    ############################################

    # Simple arc, no timestamps
    g.Connect( ROOT, ("x", M_FLT, N_0), "A" )

    for n in range( 5 ):
        time.sleep( 1 )
        for rate in [1e2, 1e1, 1e0, 1e-1, 1e-2, 1e-3, 1e-4, 1e-5, 1e-6 ]:
            print( "iter:%d rate:%f" % (n, rate), end=" -> " )
            result = g.Neighborhood(
                id      =   "ROOT",
                timeout =   1000,
                hits    =   10,
                fields  =   F_VAL,
                result  =   R_SIMPLE,
                arc     =   D_OUT,
                collect =   C_SCAN,
                filter  =   """
                            a = synarc.decay( 'x' );
                            b = synarc.xdecay( 'x' );
                            c = synarc.decay( 'x', %f );
                            d = synarc.xdecay( 'x', %f );
                            collect(a);
                            collect(b);
                            collect(c);
                            collect(d);
                            """ % (rate, rate)
            )

            # Current time
            t_1 = g.ts
            # No arc timestamps
            t_0 = g_0           # Fallback to graph inception
            r_dflt = 0.0        # No rate given, fallback to N_0/(t_end-t_0) -> 0, because t_end->inf without M_TMX and no rate
            r_rate = rate * N_0 # Rate given
            L_dflt = 0.0        # No rate given, fallback to log(10**4)/(t_end-t_0) -> 0, because t_end->inf without M_TMX and no rate
            L_rate = rate * math.log( 10**4 ) # Rate given
            t = t_1 - t_0

            Expect( len( result ) == 4,                         "Two arcs collected" )
            lin_dflt, exp_dflt, lin_rate, exp_rate = result
            print( "lin_static:%.4g  exp_static:%.4g  lin:%.4g  exp: %.4g" % (lin_dflt, exp_dflt, lin_rate, exp_rate) )

            # Linear, no rate
            r = r_dflt
            N_t = N_0 - r*t
            Expect( float_equal( lin_dflt, N_t ),               "Default linear decay value %.4g, got %.4g" % (N_t, lin_dflt) )
            
            # Exponential, no rate
            L = L_dflt
            N_t = N_0 * math.exp( -L * t )
            Expect( float_equal( exp_dflt, N_t ),               "Default exponential decay value %.4g, got %.4g" % (N_t, exp_dflt) )

            # Linear, rate given
            r = r_rate
            N_t = N_0 - r*t
            Expect( float_equal( lin_rate, N_t ),               "Linear decay value %.4g, got %.4g" % (N_t, lin_rate) )

            # Exponential, rate given
            L = L_rate
            N_t = N_0 * math.exp( -L * t )
            Expect( float_equal( exp_rate, N_t ),               "Exponential decay value %.4g, got %.4g" % (N_t, exp_rate) )









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
