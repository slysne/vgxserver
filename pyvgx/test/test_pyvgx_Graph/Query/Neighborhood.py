###############################################################################
# 
# VGX Server
# Distributed engine for plugin-based graph and vector search
# 
# Module:  pyvgxtest
# File:    Neighborhood.py
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
import operator
from functools import reduce

graph = None




###############################################################################
# TEST_vxquery_traverse
#
###############################################################################
def TEST_vxquery_traverse():
    """
    Core vxquery_traverse
    test_level=501
    """
    try:
        pyvgx.selftest( force=True, testroot="vgxtest", library="vgx", names=["vxquery_traverse.c" ] )
    except:
        Expect( False )




###############################################################################
# TEST_vxquery_collector
#
###############################################################################
def TEST_vxquery_collector():
    """
    Core vxquery_collector
    test_level=501
    """
    try:
        pyvgx.selftest( force=True, testroot="vgxtest", library="vgx", names=["vxquery_collector.c" ] )
    except:
        Expect( False )




###############################################################################
# TEST_Neighborhood_many
#
###############################################################################
def TEST_Neighborhood_many():
    """
    pyvgx.Graph.Neighborhood()
    Various scenarios
    t_nominal=27
    test_level=3101
    """
    graph.Truncate()
    N = 1000
    A = graph.NewVertex( "A" )
    T = set()
    RELS = ["rel1", "rel2", "rel3"]
    MODS = [M_INT, M_FLT, M_UINT]
    FIRST_REL = RELS[0]
    FIRST_MOD = MODS[0]
    narcs = 0
    
    first_filter_fmt = "next.arc.type == '%s' && next.arc.mod == %d && next.arc.value %%s %%d" % (FIRST_REL, FIRST_MOD)


    # Add arcs and test
    for n in range( 1, N+1 ):
        # Add arc
        terminal = "term_%d" % n
        T.add( terminal )
        for rel in RELS:
            for modifier in MODS:
                graph.Connect( A, (rel, modifier, n ), terminal )
                narcs += 1
                # Check outarcs for specific rel/mod/val
                r_1 = graph.Neighborhood( A, arc=( FIRST_REL, D_OUT, FIRST_MOD, V_EQ, n) )
                r_n = graph.Neighborhood( A, arc=( FIRST_REL, D_OUT, FIRST_MOD, V_LTE, n) )
                r_0 = graph.Neighborhood( A, arc=( FIRST_REL, D_OUT, FIRST_MOD, V_GT, n) )
                Expect( len( r_1 ) == 1,    "EQ should return 1 result" )
                Expect( len( r_n ) == n,    "LTE should return %d results" % n)
                Expect( len( r_0 ) == 0,    "GT should return 0 results" )
                # Cross-check filter syntax
                r_1_filter = graph.Neighborhood( A, arc=D_OUT, filter=first_filter_fmt % ("==", n)  )
                r_n_filter = graph.Neighborhood( A, arc=D_OUT, filter=first_filter_fmt % ("<=", n)  )
                r_0_filter = graph.Neighborhood( A, arc=D_OUT, filter=first_filter_fmt % (">", n)  )
                Expect( r_1 == r_1_filter,  "Filter syntax equivalent" )
                Expect( r_n == r_n_filter,  "Filter syntax equivalent" )
                Expect( r_0 == r_0_filter,  "Filter syntax equivalent" )
                # Check neighbors
                Expect( r_1[0] == terminal, "EQ result should match latest terminal='%s', got '%s'" % (terminal, r_1[0]) )
                Expect( set( r_n ) == T,    "LTE result should match all terminals" )
                # Check reverse
                r_rev = graph.Neighborhood( terminal, arc=( FIRST_REL, D_IN, FIRST_MOD, V_EQ, n), neighbor={'id':'A'} )
                # Cross-check filter syntax
                r_rev_filter = graph.Neighborhood( terminal, arc=D_IN, filter="next.arc.type == '%s' && next.arc.mod == %d && next.arc.value == %d && next.id == 'A'" % (FIRST_REL, FIRST_MOD, n) )
                Expect( r_rev == r_rev_filter,  "Filter syntax equivalent" )
                #zzz
                #zzz
                if len( r_rev ) != 1:
                    print(n, rel, modifier, narcs)
                    print(r_rev)
                    print(r_rev_filter)
                #zzz
                #zzz
                Expect( len( r_rev ) == 1, "reverse arc" )
                # Check total fwd
                r_fwd = graph.Neighborhood( A )
                nfwd = len( r_fwd )
                Expect( nfwd == narcs,    "should have %d outarcs (got %d)" % (narcs, nfwd) )


    # Remove arcs and test
    all = graph.Neighborhood( "A" )
    Expect( len(all) == narcs,          "should have %d outarcs" % narcs  )
    
    # Remove arcs and test
    all_terminals = graph.Neighborhood( "A", arc=(FIRST_REL, D_OUT, FIRST_MOD), sortby=S_RANDOM )

    # 
    for node in all_terminals:
        B = graph.OpenVertex( node, 'a' ) # open terminal to prevent auto-deletion of virtual node
        fwd_1 = graph.Neighborhood( A, neighbor={'id':B} )
        rev_1 = graph.Neighborhood( B, arc=D_IN, neighbor={'id':A} )
        # Cross-check filter syntax
        fwd_1_filter = graph.Neighborhood( A, filter="next.id == '%s'" % B.id )
        rev_1_filter = graph.Neighborhood( B, arc=D_IN, filter="next.id == '%s'" % A.id )
        Expect( fwd_1 == fwd_1_filter,  "Filter syntax equivalent" )
        Expect( rev_1 == rev_1_filter,  "Filter syntax equivalent" )
        # Check result size
        nmulti = len( RELS ) * len( MODS )
        Expect( len( fwd_1 ) == nmulti,               "should have %d arcs in %s-()->%s, got: %s" % (nmulti, A.id, B.id, fwd_1) )
        Expect( len( rev_1 ) == nmulti,               "should have %d arcs in %s<-()-%s, got: %s" % (nmulti, A.id, B.id, rev_1) )
        for rel in RELS:
            nmulti -= graph.Disconnect( A, rel, B )
            fwd_1 = graph.Neighborhood( A, neighbor={'id':B} )
            rev_1 = graph.Neighborhood( B, arc=D_IN, neighbor={'id':A} )
            # Cross-check filter syntax
            fwd_1_filter = graph.Neighborhood( A, filter="next.id == '%s'" % B.id )
            rev_1_filter = graph.Neighborhood( B, arc=D_IN, filter="next.id == '%s'" % A.id )
            Expect( fwd_1 == fwd_1_filter,  "Filter syntax equivalent" )
            Expect( rev_1 == rev_1_filter,  "Filter syntax equivalent" )
            # Check result size 
            Expect( len( fwd_1 ) == nmulti,               "should have %d arcs in %s-()->%s after removal of relationship '%s', got: %s" % (nmulti, A.id, B.id, rel, fwd_1) )
            Expect( len( rev_1 ) == nmulti,               "should have %d arcs in %s<-()-%s after removal of relationship '%s', got: %s" % (nmulti, A.id, B.id, rel, rev_1) )
        T.remove( node )
        FWD = set( graph.Neighborhood( A ) )
        Expect( T == FWD,           "should have %d remaining terminals" % len(T) )
        graph.CloseVertex( B )
    
    # Check empty 
    Expect( A.degree == 0,                                "should have zero outdegree" )
    Expect( len( graph.Neighborhood( A ) ) == 0,          "should have no neighbors"  )
    graph.CloseVertex( A )
    graph.DebugCheckAllocators()
        
            



###############################################################################
# TEST_Neighborhood_collection_filters_immediate
#
###############################################################################
def TEST_Neighborhood_collection_filters_immediate():
    """
    pyvgx.Graph.Neighborhood()
    Collection filters in immediate neighborhood
    test_level=3101
    """

    graph.Truncate()

    # Simple basic case
    FRIENDS = set( ["person_%d" % n for n in range( 100 )] )
    COWORKERS = set( ["person_%d" % n for n in range( 90, 200 )] )
    PERSONS = FRIENDS.union( COWORKERS )
    COMPANIES = set( ["company_%d" % n for n in range( 50 )] )
    CALLED = set( ["person_%d" % n for n in range( 70, 120 )] + ["company_%d" % n for n in range( 20, 40 )] )
    VISITED = set( ["person_%d" % n for n in range( 93, 98 )] + ["company_%d" % n for n in range( 10, 30 )] )

    for person in PERSONS:
        graph.CreateVertex( person, type="person" )
    for company in COMPANIES:
        graph.CreateVertex( company, type="company" )
    
    for person in FRIENDS:
        graph.Connect( "Alice", ("friend"), person )
    for person in COWORKERS:
        graph.Connect( "Alice", ("workswith"), person )
    t=0
    for calledby in CALLED:
        graph.Connect( "Alice", ("called",M_TMC,1000000+t), calledby )
        t += 1
    for visitedby in VISITED:
        graph.Connect( "Alice", ("visited",M_TMC,1000000+t), visitedby )
    

    # Friends that were called
    result = set( graph.Neighborhood( "Alice", arc="friend", collect="called" ) )
    Expect( result == FRIENDS.intersection( CALLED ),       "Return called friends" )
    # Coworkers that were called
    result = set( graph.Neighborhood( "Alice", arc="workswith", collect="called" ) )
    Expect( result == COWORKERS.intersection( CALLED ),     "Return called coworkers" )
    # Persons that were called
    result = set( graph.Neighborhood( "Alice", collect="called", neighbor={'type':'person'} ) )
    Expect( result == PERSONS.intersection( CALLED ),       "Return called persons" )
    # Companies that were called
    result = set( graph.Neighborhood( "Alice", collect="called", neighbor={'type':'company'} ) )
    Expect( result == COMPANIES.intersection( CALLED ),     "Return called companies" )
    # Friends that were visited
    result = set( graph.Neighborhood( "Alice", arc="friend", collect="visited" ) )
    Expect( result == FRIENDS.intersection( VISITED ),      "Return visited friends" )
    # Coworkers that were visited
    result = set( graph.Neighborhood( "Alice", arc="workswith", collect="visited" ) )
    Expect( result == COWORKERS.intersection( VISITED ),    "Return visited coworkers" )
    # Persons that were visited
    result = set( graph.Neighborhood( "Alice", collect="visited", neighbor={'type':'person'} ) )
    Expect( result == PERSONS.intersection( VISITED ),      "Return visited persons" )
    # Companies that were visited
    result = set( graph.Neighborhood( "Alice", collect="visited", neighbor={'type':'company'} ) )
    Expect( result == COMPANIES.intersection( VISITED ),    "Return visited companies" )
    # Coworkers who are friends
    result = set( graph.Neighborhood( "Alice", arc="friend", collect="workswith" ) )
    Expect( result == FRIENDS.intersection( COWORKERS ),    "Return coworkers who are friends" )
    result = set( graph.Neighborhood( "Alice", arc="workswith", collect="friend" ) )
    Expect( result == FRIENDS.intersection( COWORKERS ),    "Return friends who are coworkers" )
    # Visited companies that were called
    result = set( graph.Neighborhood( "Alice", arc="called", collect="visited", neighbor={'type':'company'} ) )
    Expect( result == COMPANIES.intersection(CALLED).intersection(VISITED),   "Return visited companies that were called" )
    graph.DebugCheckAllocators()




###############################################################################
# TEST_Neighborhood_collection_filters_extended
#
###############################################################################
def TEST_Neighborhood_collection_filters_extended():
    """
    pyvgx.Graph.Neighborhood()
    Collection filters in extended neighborhood
    t_nominal=19
    test_level=3102
    """

    # More advanced
    graph.Truncate()
    for SIZE_FIRST in range(1,5):
        for SIZE_SECOND in range(1,15):
            for SIZE_THIRD in range(1,45):

                FIRST = [("first_%d" % a, a) for a in range( SIZE_FIRST )]
                SECOND = [("second_%d" % b, b) for b in range( SIZE_SECOND )]
                THIRD = [("third_%d" % c, c) for c in range( SIZE_THIRD )]

                FIRST_SET = set( dict( FIRST) )
                SECOND_SET = set( dict( SECOND) )
                THIRD_SET = set( dict( THIRD) )

                FIRST_NARCS = 0
                SECOND_NARCS = 0
                THIRD_NARCS = 0

                R = "ROOT"
                for A, a in FIRST:
                    graph.Connect( R, ("first", M_INT, a), A )
                    FIRST_NARCS += 1
                    for B, b in SECOND:
                        graph.Connect( A, ("second", M_INT, b), B )
                        graph.Connect( A, ("second", M_FLT, b/float(a+1)), B )
                        SECOND_NARCS += 2
                        for C, c in THIRD:
                            graph.Connect( B, ("third", M_INT, c), C )
                            graph.Connect( B, ("third", M_FLT, c/float(b+1)), C )
                            graph.Connect( B, ("special", M_UINT, a+b+c ), C )
                            THIRD_NARCS += 3

                # Basic collection filters in immediate neighborhood
                Expect( set( graph.Neighborhood( R ) ) == FIRST_SET,                                "All hits collected" )
                Expect( set( graph.Neighborhood( R, collect=C_COLLECT ) ) == FIRST_SET,             "All hits collected" )
                Expect( len( graph.Neighborhood( R, collect=C_NONE ) ) == 0,                        "No hits collected" )
                Expect( set( graph.Neighborhood( R, collect=("first", D_OUT) ) ) == FIRST_SET,      "All hits matching collection filter" )
                Expect( len( graph.Neighborhood( R, collect=("first", D_IN) ) ) == 0,               "No hits matching collection filter of opposite direction" )
                Expect( len( graph.Neighborhood( R, collect=("first", D_OUT, M_CNT) ) ) == 0,       "No hits matching collection filter with different modifier" )
                Expect( set( graph.Neighborhood( R, collect=("first", D_OUT, M_INT) ) ) == FIRST_SET, "All hits matching collection filter with same modifier" )
                for a in range( SIZE_FIRST ):
                    Expect( set( graph.Neighborhood( R, collect=("first", D_OUT, M_INT, V_EQ, a) ) ) == set(["first_%d" % a]), "One hit matching collection filter for specific arc" )
                    Expect( set( graph.Neighborhood( R, collect=("first", D_OUT, M_INT, V_LT, a) ) ) == set(["first_%d" % x for x in range(a)]), "Hits matching collection filter for specific arcs" )
                Expect( len( graph.Neighborhood( R, arc=("nomatch",D_OUT), collect=("first", D_OUT) ) ) == 0,     "No arcs traversed, so no hits" )

                # Extended (second) neighborhood
                # Collect nothing
                result = graph.Neighborhood( R, arc=("first",D_OUT), collect=C_NONE, neighbor={ 'arc':("second", D_OUT, M_INT) } )
                result_filter = graph.Neighborhood( R, filter="next.arc.type == 'first'", collect=C_NONE, neighbor={ 'traverse':{ 'arc':D_OUT, 'filter':'next.arc.type == "second" && next.arc.mod == M_INT' } } )
                Expect( result == result_filter,                                "Filter syntax equivalent" )
                Expect( len( result ) == 0,                                     "No hits since nothing collected" )
                # Collection follows traversal
                result = graph.Neighborhood( R, arc=("first",D_OUT), collect=C_NONE, neighbor={ 'arc':("second", D_OUT, M_INT), 'collect':C_COLLECT } )
                result_filter = graph.Neighborhood( R, filter="next.arc.type == 'first'", collect=C_NONE, neighbor={ 'traverse':{ 'arc':D_OUT, 'filter':'next.arc.type == "second" && next.arc.mod == M_INT', 'collect':C_COLLECT } } )
                Expect( result == result_filter,                                "Filter syntax equivalent")
                Expect( len( result ) == SIZE_FIRST * SIZE_SECOND,              "Collection follows traversal" )
                Expect( set( result ) == SECOND_SET,                            "Full second set deduplicated" )
                # Make sure all paths are accounted for
                result = graph.Neighborhood( R, arc=("first",D_OUT), collect=C_NONE, neighbor={ 'arc':("second", D_OUT, M_INT), 'collect':C_COLLECT }, fields=F_ANCHOR|F_ID, result=R_LIST )
                Expect( len( set( result ) ) == SIZE_FIRST * SIZE_SECOND,       "All unique paths" )
                # Make sure collected arcs match traversal filter
                result = graph.Neighborhood( R, arc=("first",D_OUT), collect=C_NONE, neighbor={ 'arc':("second", D_OUT, M_INT), 'collect':C_COLLECT }, fields=F_MOD )
                Expect( set( result ) == set( ["M_INT"] ),                      "All collected arcs have same modifier as traversal filter" )
                result = graph.Neighborhood( R, arc=("first",D_OUT), collect=C_NONE, neighbor={ 'arc':("second", D_OUT, M_FLT), 'collect':C_COLLECT }, fields=F_MOD )
                result_filter = graph.Neighborhood( R, filter="next.arc.type == 'first'", collect=C_NONE, neighbor={ 'traverse':{ 'arc':D_OUT, 'filter':'next.arc.type == "second" && next.arc.mod == M_FLT', 'collect':C_COLLECT } }, fields=F_MOD )
                Expect( result == result_filter,                                "Filter syntax equivalent")
                Expect( len( result ) == SIZE_FIRST * SIZE_SECOND,              "Same number of arcs for the alternative modifier" )
                Expect( set( result ) == set( ["M_FLT"] ),                      "All collected arcs have same modifier as traversal filter" )

                # Collection filter different from traversal filter
                result = graph.Neighborhood( R, arc=("first",D_OUT), collect=C_NONE, neighbor={ 'arc':("second", D_OUT, M_INT), 'collect':("second", D_OUT, M_FLT) }, fields=F_MOD )
                result_filter = graph.Neighborhood( R, filter="next.arc.type == 'first'", collect=C_NONE, neighbor={ 'traverse':{ 'arc':D_OUT, 'filter':'next.arc.type == "second" && next.arc.mod == M_INT', 'collect':("second", D_OUT, M_FLT) } }, fields=F_MOD )
                Expect( result == result_filter,                                "Filter syntax equivalent")
                Expect( len( result ) == SIZE_FIRST * SIZE_SECOND,              "Traverse one arc, collect the other" )
                Expect( set( result ) == set( ["M_FLT"] ),                      "Collected arcs are different from traversal filter" )
                result = graph.Neighborhood( R, arc=("first",D_OUT), collect=C_NONE, neighbor={ 'arc':("second", D_OUT, M_INT), 'collect':("second", D_OUT, M_ANY) }, fields=F_MOD )
                result_filter = graph.Neighborhood( R, filter="next.arc.type == 'first'", collect=C_NONE, neighbor={ 'traverse':{ 'arc':D_OUT, 'filter':'next.arc.type == "second" && next.arc.mod == M_INT', 'collect':("second", D_OUT, M_ANY) } }, fields=F_MOD )
                Expect( result == result_filter,                                "Filter syntax equivalent")
                Expect( len( result ) == SIZE_FIRST * SIZE_SECOND * 2,          "Traverse one arc, collect all arcs" )
                Expect( set( result ) == set( ["M_FLT", "M_INT"] ),             "All arcs collected" )
                result = graph.Neighborhood( R, arc=("first",D_OUT), collect=C_NONE, neighbor={ 'arc':("second", D_OUT, M_INT), 'collect':("second", D_ANY, M_ANY) }, fields=F_MOD )
                result_filter = graph.Neighborhood( R, filter="next.arc.type == 'first'", collect=C_NONE, neighbor={ 'traverse':{ 'arc':D_OUT, 'filter':'next.arc.type == "second" && next.arc.mod == M_INT', 'collect':("second", D_ANY, M_ANY) } }, fields=F_MOD )
                Expect( result == result_filter,                                "Filter syntax equivalent")
                Expect( len( result ) == SIZE_FIRST * SIZE_SECOND * 2,          "Traverse one arc, collect all arcs" )
                Expect( set( result ) == set( ["M_FLT", "M_INT"] ),             "All arcs collected" )
                

                # Start at second level and look in both directions
                for A in FIRST_SET:
                    Expect( len( graph.Neighborhood( A, arc=("second", D_OUT), collect=("first",D_IN) ) ) == 0,     "No hits since collect filter does not match traversed multiple arc" )
                    Expect( len( graph.Neighborhood( A, arc=("first", D_IN), collect=("first",D_IN) ) ) == 1,       "Single hit matching the reverse arc" )
                    Expect( len( graph.Neighborhood( A, arc=("first", D_IN), collect=("first",D_OUT) ) ) == 0,      "No hit since collect filter arc has opposite direction" )

                # Various advanced cases
                #
                result = graph.Neighborhood( 
                    id       = R,
                    collect  = C_SCAN,
                    arc      = ("first",D_OUT),
                    neighbor = { 
                        'arc': ("second",D_OUT),
                        'collect': C_SCAN,
                        'neighbor': {
                            'arc': ("third",D_OUT),
                            'collect': ("special")
                        }
                    }
                )
                result_filter = graph.Neighborhood(
                    id       = R,
                    collect  = C_SCAN,
                    filter   = "next.arc.type == 'first'",
                    neighbor = {
                        'traverse': {
                            'arc': D_OUT,
                            'collect': C_SCAN,
                            'filter': 'next.arc.type == "second"',
                            'neighbor': {
                                'traverse': {
                                    'arc':D_OUT, 
                                    'filter': 'next.arc.type == "third"',
                                    'collect':("special")
                                }
                            }
                        }
                    }
                )
                Expect( result == result_filter,                                        "Filter syntax equivalent")
                #zzz
                #zzz
                #if len( result ) != SIZE_FIRST * SIZE_SECOND * SIZE_THIRD:
                #    print(SIZE_FIRST, SIZE_SECOND, SIZE_THIRD)
                #    print(len(result))
                #    print()
                #zzz
                #zzz
                NCOLLECTED = SIZE_FIRST * SIZE_SECOND * 2 * SIZE_THIRD
                Expect( len( result ) == NCOLLECTED,         "Account for {} collected paths to 'special', got {}".format(NCOLLECTED,len(result)) )

                result = graph.Neighborhood(
                    id       = R,
                    collect  = C_SCAN,
                    arc      = ("first",D_OUT),
                    neighbor = {
                        'arc': ("second",D_OUT),
                        'collect': C_SCAN,
                        'neighbor': {
                            'arc': ("third",D_OUT),
                            'collect': ("*")
                        }
                    }
                )
                result_filter = graph.Neighborhood(
                    id = R,
                    collect = C_SCAN,
                    filter = "next.arc.type == 'first'",
                    neighbor = {
                        'traverse': {
                            'arc': D_OUT,
                            'collect': C_SCAN,
                            'filter': 'next.arc.type == "second"',
                            'neighbor': {
                                'traverse': {
                                    'arc': D_OUT,
                                    'filter': 'next.arc.type == "third"',
                                    'collect': ("*")
                                } 
                            }
                        }
                    }
                )
                NCOLLECTED = SIZE_FIRST * SIZE_SECOND * 2 * SIZE_THIRD * 3
                Expect( len( result ) == NCOLLECTED,     "All {} paths are collected, got {}".format( NCOLLECTED, len(result) ) )
                Expect( result == result_filter,                                        "Filter syntax equivalent")

                result = graph.Neighborhood(
                    id       = R,
                    collect  = C_SCAN,
                    arc      = ("first",D_OUT),
                    neighbor = {
                        'arc': ("second",D_OUT),
                        'collect': C_SCAN,
                        'neighbor': {
                            'arc': D_OUT,
                            'collect': ("third",D_OUT,M_INT,V_EQ,0)
                        }
                    }
                )
                result_filter = graph.Neighborhood(
                    id = R,
                    collect = C_SCAN,
                    filter = "next.arc.type == 'first'",
                    neighbor = {
                        'traverse': {
                            'arc': D_OUT,
                            'collect': C_SCAN,
                            'filter': 'next.arc.type == "second"',
                            'neighbor': {
                                'traverse': {
                                    'arc': D_OUT,
                                    'filter': 'collectif( next.arc.type == "third" && next.arc.mod == M_INT && next.arc.value == 0 )'
                                }
                            }
                        }
                    }
                )
                Expect( result == result_filter,                                        "Filter syntax equivalent")
                NCOLLECTED = SIZE_FIRST * SIZE_SECOND * 2
                Expect( len( result ) == NCOLLECTED, "Only one multiarc ({}) in third neighborhood matches traversal filter, got {}".format(NCOLLECTED, len(result)) )

                result = graph.Neighborhood(
                    id = R,
                    arc = D_OUT,
                    neighbor = {
                        'arc': D_OUT,
                        'collect': C_COLLECT,
                        'neighbor': {
                            'arc': D_OUT,
                            'collect': C_COLLECT
                        }
                    }
                )
                NPATHS = (SIZE_FIRST) + (SIZE_FIRST * 2 * SIZE_SECOND) + (SIZE_FIRST * 2 * SIZE_SECOND * 3 * SIZE_THIRD)
                Expect( len( result ) == NPATHS, "Collect entire set of unique paths" )

                a = SIZE_FIRST // 2
                b = SIZE_SECOND // 2
                c = SIZE_THIRD // 2
                result = graph.Neighborhood( R, 
                            arc=("first",D_OUT,M_INT,V_EQ,a), collect=C_COLLECT, neighbor={ 
                                'arc':("second",D_OUT,M_INT,V_EQ,b), 'collect':("second",D_OUT,M_FLT), 'neighbor':{
                                    'arc':("third",D_OUT,M_INT,V_EQ,c), 'collect':("special",D_OUT)
                                 }
                            },
                            fields=F_AARC, result=R_DICT )
                result_filter = graph.Neighborhood( R, 
                            filter="next.arc.type == 'first' && next.arc.mod == M_INT && next.arc.value == %d" % a, collect=C_COLLECT, neighbor={
                                'traverse':{ 
                                    'arc':D_OUT, 'filter':'next.arc.type == "second" && next.arc.mod == M_INT && next.arc.value == %d' % b, 'collect':("second",D_OUT,M_FLT), 'neighbor':{
                                        'traverse':{
                                            'arc':D_OUT, 'filter':'next.arc.type == "third" && next.arc.mod == M_INT && next.arc.value == %d' % c, 'collect':("special",D_OUT)
                                        }
                                    }
                                }
                            },
                            fields=F_AARC, result=R_DICT )
                Expect( result == result_filter,                                        "Filter syntax equivalent")

                for item in result:
                    if item['distance'] == 1:
                        Expect( item['id'] == "first_%d" % (a),         "Collected one first arc" )
                        Expect( item['arc']['modifier'] == 'M_INT',     "Collected arc modifier matches traversed" )
                    elif item['distance'] == 2:
                        Expect( item['id'] == "second_%d" % (b),        "Collected one second arc" )
                        Expect( item['arc']['modifier'] == 'M_FLT',     "Collected arc modifier matches collection filter" )
                    elif item['distance'] == 3:
                        Expect( item['id'] == "third_%d" % (c),         "Collected one third arc" )
                        Expect( item['arc']['modifier'] == 'M_UINT',    "Collected arc modifier matches collection filter" )

                graph.DeleteVertex( R )
                for node in FIRST_SET:
                    graph.DeleteVertex( node )
                for node in SECOND_SET:
                    graph.DeleteVertex( node )
                for node in THIRD_SET:
                    graph.DeleteVertex( node )
                Expect( graph.size == 0 and graph.order == 0,           "Graph should be empty" )
    graph.DebugCheckAllocators()





###############################################################################
# TEST_Neighborhood_degree_conditions
#
###############################################################################
def TEST_Neighborhood_degree_conditions():
    """
    pyvgx.Graph.Neighborhood()
    Conditional degree filters
    test_level=3102
    """

    graph.Truncate()

    # Set up:
    #
    # (X) -[val]-> (node) -[s]-> (term_s)
    #
    # where s is an increasingly large set of values centered around f.
    #
    NODES = [("A",10), ("B",20), ("C",30), ("D",40)]
    NODESET = set( dict( NODES ) )
    delta = 0
    for node, val in NODES:
        graph.Connect( "X", ("first", M_INT, val), node )
        delta += 1
        for second in range( val-delta, val+delta+1 ):
            graph.Connect( node, ("second", M_INT, second), "term_%d" % second)

    # Only 'A' and 'B' have 6 or fewer arcs (both in and out)
    expected_result = set([ "A", "B" ])
    result = graph.Neighborhood( "X", arc=("first", D_OUT, M_INT), neighbor={ 'degree':(V_LTE,6) } )
    result_filter = graph.Neighborhood( "X", filter="next.arc.type == 'first' && next.arc.mod == M_INT && next.deg <= 6" )
    Expect( result == result_filter,            "Filter syntax equivalent" )
    Expect( set( result ) == expected_result,   "Only 'A' and 'B' have <= 6 arcs" )

    # Only one arc should have conditional degree == 1 for its center value
    for node, val in NODES:
        result = graph.Neighborhood( "X", arc=("first", D_OUT, M_INT), neighbor={ 'degree':( ("second", D_OUT, M_INT, V_EQ, val), (V_EQ,1) ) } )
        single = set( [node] )
        Expect( set( result ) == single, "Only '%s' has degree==1 for val=%d" % (node,val) )


    # All nodes should have exactly one second arc (cond. degree==1) matching the value of the first arc leading to the node
    Expect( set( graph.Neighborhood( "X", arc=("first", D_OUT, M_INT), neighbor={ 'degree':( ("second", D_OUT, M_INT, V_DYN_EQ), (V_EQ,1) ) } ) ) == NODESET, "All nodes" )

    # Only node 'A' has less than 3 second arcs with values greater than or equal to the first arc leading to the node
    # 'B' does not match because it has 3 such arcs.
    # 'C' does not match because it has 4 such arcs.
    # 'D' does not match because it has 5 such arcs.
    Expect( set( graph.Neighborhood( "X", arc=("first", D_OUT, M_INT), neighbor={ 'degree':( ("second", D_OUT, M_INT, V_DYN_GTE), (V_LT,3) ) } ) ) == set( ["A"] ), "Only 'A'" )

    # Only node 'C' has exactly 7 second arcs with values +/- 4 of the first arc leading to the node.
    # 'A' does not match because it has 3 such arcs.
    # 'B' does not match because it has 5 such arcs.
    # 'D' does not match because it has 9 such arcs.
    Expect( set( graph.Neighborhood( "X", arc=("first", D_OUT, M_INT), neighbor={ 'degree':( ("second", D_OUT, M_INT, V_DYN_DELTA, (-4,4)), (V_EQ,7) ) } ) ) == set( ["C"] ), "Only 'C'" )

    graph.DebugCheckAllocators()
    graph.Truncate()




###############################################################################
# TEST_Neighborhood_aggregate
#
###############################################################################
def TEST_Neighborhood_aggregate():
    """
    pyvgx.Graph.Neighborhood()
    Arc aggregation
    t_nominal=17
    test_level=3102
    """
    def count( L ):
        return len( L )

    def product( L ):
        return reduce( operator.mul, L )

    def sqsum( L ):
        return sum( [x*x for x in L] )

    def average( L ):
        return sum( L ) / float( len( L ) )

    qcnt = 0

    maxint = 2**31-1
    maxflt = 1e38
    def equals( a, b ):
        if type(a) in [int, int] and type(b) in [int, int]:
            if a >= maxint and b >= maxint:
                return True
            else:
                return a == b
        elif abs(b) > 0:
            if a > maxflt and b > 1e38:
                return True
            else:
                # Allow some fuzz
                return abs( a / b - 1 ) < 1e-4
        else:
            return a == 0
            



    graph.Truncate()


    N_VIAS = [1, 2, 3, 5, 10, 20, 100, 1000, 2000]
    N_TERMS = [0, 1, 2, 3, 5, 10, 20]


    MODS = [ (M_INT,), (M_INT, M_FLT), (M_FLT,), (M_INT, M_FLT, M_UINT) ]

    for n_vias in N_VIAS:
        for n_terms in N_TERMS:
            for mods in MODS:

                # create vertices
                A = graph.NewVertex( "A", type="root" )
                for t in range( 1, n_terms+1 ):
                    T = graph.NewVertex( "terminal_%d" % t, type="term" )
                    T['number'] = t
                for v in range( 1, n_vias+1 ):
                    V = graph.NewVertex( "via_%d" % v, type="via" )

                # connect
                for v in range( 1, n_vias+1 ):
                    V = graph.OpenVertex( "via_%d" % v )
                    graph.Connect( A, ("via"), V )
                    for t in range( 1, n_terms+1 ):
                        T = graph.OpenVertex( "terminal_%d" % t )
                        for mod in mods:
                            graph.Connect( V, ("term",mod,v*t), T )
                        graph.CloseVertex( T )
                    graph.CloseVertex( V )
                graph.CloseVertex( A )

                # verify
                order = 1 + n_vias + n_terms
                Expect( graph.order == order,                   "order = %d, got %d" % (order, graph.order) )
                size = n_vias + n_vias * n_terms * len( mods )
                Expect( graph.size == size,                     "size = %d, got %d" % (size, graph.size) )

                # test aggregation for all modifiers, with and without sorting
                for sortby in [S_NONE, S_VAL]:
                    for mod in mods:
                        if mod == M_FLT:
                            mod_aggr = M_FLTAGGR
                            tp = float
                        else:
                            mod_aggr = M_INTAGGR
                            tp = int

                        # first
                        aggr = graph.Neighborhood( "A", collect=C_NONE, fields=F_VAL, result=R_SIMPLE, sortby=sortby,
                                        neighbor={ 'collect':C_COLLECT, 'arc':("term",D_OUT,mod) },
                                        aggregate={ 'mode':'first' } )
                        aggr_filter = graph.Neighborhood( "A", collect=C_NONE, fields=F_VAL, result=R_SIMPLE, sortby=sortby,
                                        neighbor={ 'traverse':{ 'collect':C_COLLECT, 'arc':D_OUT, 'filter':'next.arc.type == "term" && next.arc.mod == %d' % mod } },
                                        aggregate={ 'mode':'first' } )
                        qcnt += 2

                        # order is not stable without sorting (due to vertex reference map's entries random nature which are then used as framehash keys for aggregation)
                        if sortby == S_NONE:
                            aggr = set( aggr )
                            aggr_filter = set( aggr_filter )
                        Expect( aggr == aggr_filter,                "Filter syntax equivalent" )
                        if len( aggr ) != n_terms:
                            print(n_vias, n_terms, mods, sortby, mod)
                        Expect( len( aggr ) == n_terms,             "results = %d, got %d" % (n_terms, len(aggr)) )

                        # aggregation functions
                        for func in [count, min, max, average, sum, product, sqsum]:
                            fname = func.__name__
                            # include all aggregations
                            aggr = graph.Neighborhood( "A", collect=C_NONE, fields=F_VAL|F_PROP, select="number", result=R_DICT, sortby=sortby,
                                            neighbor={ 'collect':C_COLLECT, 'arc':("term",D_OUT,mod) },
                                            aggregate={ 'mode':fname } )
                            qcnt += 1
                            Expect( len( aggr ) == n_terms,             "results = %d, got %d" % (n_terms, len(aggr)) )
                            for entry in aggr:
                                t = tp( entry['properties']['number'] )
                                arc_values = [ t*v for v in range( 1, n_vias+1 ) ] 
                                a = func( arc_values )
                                v = entry['arc']['value']
                                Expect( equals(v,a),                    "%s = %s, got %s" % (fname, a, v) )

                            # aggregation postfilter 
                            CUTOFF = tp(500)
                            aggr = graph.Neighborhood( "A", collect=C_NONE, fields=F_VAL|F_PROP, select="number", result=R_DICT, sortby=sortby,
                                            neighbor={ 'collect':C_COLLECT, 'arc':("term",D_OUT,mod) },
                                            aggregate={ 'mode':fname, 'arc':("term",D_ANY,mod_aggr,V_GT,CUTOFF) } )
                            qcnt += 1
                            for entry in aggr:
                                t = tp( entry['properties']['number'] )
                                arc_values = [ t*v for v in range( 1, n_vias+1 ) ] 
                                a = func( arc_values )
                                Expect( a > CUTOFF,                     "only include value > %s, got %s" % (CUTOFF, a) )
                                v = entry['arc']['value']
                                Expect( equals(v,a),                    "%s = %s, got %s" % (fname, a, v) )



                # clean up
                for node in graph.Vertices():
                    graph.DeleteVertex( node )
                
                Expect( graph.order == 0 and graph.size == 0,   "Empty graph after cleanup, got %s" % graph  )

    graph.DebugCheckAllocators()
    print("qcnt = %d" % qcnt)





###############################################################################
# TEST_Neighborhood_adjacency_by_vertexid
#
###############################################################################
def TEST_Neighborhood_adjacency_by_vertexid():
    """
    pyvgx.Graph.Neighborhood()
    Neighbors' adjacency test
    test_level=3102
    """
    graph.Truncate()

    N = 1000

    graph.CreateVertex( "ROOT" )

    graph.Connect( "ROOT", "to", "middle" )

    # immediate neighbor
    result = graph.Neighborhood( "ROOT", neighbor={ 'id':"middle" } )
    Expect( result == ["middle"] )

    # immediate neighbor should have an inarc
    result = graph.Neighborhood( "ROOT", neighbor={ 'id':"middle", 
                        'adjacent':{
                            'arc':D_IN
                         }})
    Expect( result == ["middle"] )

    # immediate neighbor should have an inarc to ROOT
    result = graph.Neighborhood( "ROOT", neighbor={ 'id':"middle", 
                        'adjacent':{
                            'arc':D_IN,
                            'neighbor':"ROOT"
                         }})
    Expect( result == ["middle"] )

    # immediate neighbor should not have an inarc to itself
    result = graph.Neighborhood( "ROOT", neighbor={ 'id':"middle", 
                        'adjacent':{
                            'arc':D_IN,
                            'neighbor':"middle"
                         }})
    Expect( result == [] )

    # immediate neighbor should not have an inarc to itself or something that doesn't exist
    result = graph.Neighborhood( "ROOT", neighbor={ 'id':"middle", 
                        'adjacent':{
                            'arc':D_IN,
                            'neighbor':["middle", "nonexist"]
                         }})
    Expect( result == [] )

    # immediate neighbor should have an inarc to ROOT which is a set of probes
    result = graph.Neighborhood( "ROOT", neighbor={ 'id':"middle", 
                        'adjacent':{
                            'arc':D_IN,
                            'neighbor':[ "middle", "nonexist", "ROOT", "nonexist2" ]
                         }})
    Expect( result == ["middle"] )


    for N in range( 100 ):
        for n in range( N ):
            term = "term_%d" % n
            graph.Connect( "middle", ("to",M_INT,n), term )
            if n > 10:
                graph.Connect( "middle", ("to",M_FLT,n), term )
                if n > 50:
                    graph.Connect( "middle", ("rel2",M_INT,n), term )

        for p in range( N+5 ):
            # Specific terminal should exist only after graph has become certain size
            result = graph.Neighborhood( "ROOT", neighbor={ 'id':"middle", 
                                'adjacent':{
                                    'arc'      : ("to", D_OUT),
                                    'neighbor' : "term_%d" % p
                                 }})
            if N > p:
                Expect( result == ["middle"] )
            else:
                Expect( result == [] )

            # Specific terminal should exist only after graph has become certain size
            result = graph.Neighborhood( "ROOT", neighbor={ 'id':"middle", 
                                'adjacent':{
                                    'arc'      : ("to", D_OUT),
                                    'neighbor' : ["nonexist", "term_%d" % p]
                                 }})
            if N > p:
                Expect( result == ["middle"] )
            else:
                Expect( result == [] )

            # term_o should exist after first iteration
            result = graph.Neighborhood( "ROOT", neighbor={ 'id':"middle", 
                                'adjacent':{
                                    'arc'      : ("to", D_OUT),
                                    'neighbor' : ["nonexist", "term_%d" % p, "term_0"]
                                 }})
            if N > 0:
                Expect( result == ["middle"] )
            else:
                Expect( result == [] )


    graph.DebugCheckAllocators()
        
            


###############################################################################
# TEST_ExecuteNeighborhoodQuery
#
###############################################################################
def TEST_ExecuteNeighborhoodQuery():
    """
    pyvgx.Graph.NewNeighborhoodQuery() and pyvgx.Query.Execute()
    test_level=3101
    """
    graph.Truncate()

    N = 1000

    # New simple query
    Q = graph.NewNeighborhoodQuery( "ROOT" )
    Expect( Q.type == "Neighborhood" )
    Expect( Q.error == None )
    Expect( Q.reason == 0 )
    Expect( Q.opid == 0 )
    Expect( Q.texec == 0.0 )
    Expect( Q.id == "ROOT" )

    # Anchor does not exist
    try:
        Q.Execute()
        Expect( False, "Query should fail when anchor does not exist" )
    except KeyError:
        Expect( len(str(Q.error)) > 0 and "ROOT" in str(Q.error) )
        Expect( Q.reason > 0 )
    except Exception as ex:
        Expect( False, "Unexpected exception: %s" % ex )

    # Populate
    for n in range( N ):
        graph.Connect( "ROOT", ("to", M_INT, n), "term_%d" % n )

    # Simple equivalence
    R1 = Q.Execute()
    R2 = graph.Neighborhood( "ROOT" )
    Expect( R1 == R2 )
    Expect( Q.error == None )
    Expect( Q.reason == 0 )
    Expect( Q.texec > 0 )

    # Modify hits and offset
    R1 = Q.Execute( hits=100, offset=15 )
    R2 = graph.Neighborhood( "ROOT", hits=100, offset=15 )
    Expect( R1 == R2 )
    Expect( Q.texec > 0 )

    # Repeated execution, same result
    R3 = Q.Execute( hits=100, offset=15 )
    R4 = Q.Execute( hits=100, offset=15 )
    R5 = Q.Execute( hits=100, offset=15 )
    Expect( R3 == R1 )
    Expect( R4 == R1 )
    Expect( R5 == R1 )

    graph.DebugCheckAllocators()





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
