###############################################################################
# 
# VGX Server
# Distributed engine for plugin-based graph and vector search
# 
# Module:  pyvgx.test
# File:    DivergentTraversal.py
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
from ..Query._query_test_support import *
from pyvgx import *
import pyvgx
from math import *
import operator
import random
from pyvgxtest.threads import Worker
import threading

graph = None



Na = 100
Nb = 10




###############################################################################
# query_first_basic
#
###############################################################################
def query_first_basic( g, a ):
    """
    """

    # Inarc from root
    for arc, X in [
        [ ("first", D_IN),  a ],
        [ ("first", D_OUT), 0 ],
        [ ("nope",  D_IN),  0 ],
        [ ("nope",  D_ANY), 0 ],
        [ ("first", D_ANY), a ] ]:

        result = g.Neighborhood( "ROOT", hits = a+1,
                    neighbor = {
                        'adjacent'  :   {
                            'arc'       : arc
                        }
                    }
        )

        Expect( len(result) == X,     "All first nodes should have inarc from ROOT" )



    # ...also outarc(s) to 'second_uniq', which all but the more recently added 'first' will have
    for a_arc, t_arc, X in [
        [ ("first", D_IN),  ("second_uniq", D_OUT), a-1 ],
        [ ("first", D_OUT), ("second_uniq", D_OUT), 0 ],
        [ ("nope",  D_IN),  ("second_uniq", D_OUT), 0 ],
        [ ("nope",  D_ANY), ("second_uniq", D_OUT), 0 ],
        [ ("first", D_ANY), ("second_uniq", D_OUT), a-1 ],

        [ ("first", D_IN),  ("second_uniq", D_IN), 0 ],
        [ ("first", D_OUT), ("second_uniq", D_IN), 0 ],
        [ ("nope",  D_IN),  ("second_uniq", D_IN), 0 ],
        [ ("nope",  D_ANY), ("second_uniq", D_IN), 0 ],
        [ ("first", D_ANY), ("second_uniq", D_IN), 0 ],

        [ ("first", D_IN),  ("second_uniq", D_ANY), a-1 ],
        [ ("first", D_OUT), ("second_uniq", D_ANY), 0 ],
        [ ("nope",  D_IN),  ("second_uniq", D_ANY), 0 ],
        [ ("nope",  D_ANY), ("second_uniq", D_ANY), 0 ],
        [ ("first", D_ANY), ("second_uniq", D_ANY), a-1 ],

        [ ("first", D_IN),  ("nope", D_ANY), 0 ],
        [ ("first", D_OUT), ("nope", D_ANY), 0 ],
        [ ("nope",  D_IN),  ("nope", D_ANY), 0 ],
        [ ("nope",  D_ANY), ("nope", D_ANY), 0 ],
        [ ("first", D_ANY), ("nope", D_ANY), 0 ],

        [ ("first", D_IN),  ("first", D_IN), a ],
        [ ("first", D_OUT), ("first", D_IN), 0 ],
        [ ("nope",  D_IN),  ("first", D_IN), 0 ],
        [ ("nope",  D_ANY), ("first", D_IN), 0 ],
        [ ("first", D_ANY), ("first", D_IN), a ],
                    ]:

        result = g.Neighborhood( "ROOT", hits = a+1,
                    neighbor = {
                        'adjacent'  :   {
                            'arc'       : a_arc
                        },
                        'traverse'  :   {
                            'arc'       : t_arc
                        }
                    }
        )

        Expect( len(result) == X,   "All but one first nodes should have outarc(s) to second_uniq" )




###############################################################################
# query_first_assert
#
###############################################################################
def query_first_assert( g, a ):
    """
    """

    for a_arc, a_asrt, t_arc, t_asrt, X in [
        [ ("first", D_IN), None, ("second_uniq", D_OUT), None,  a-1 ],
        [ ("first", D_IN), None, ("second_uniq", D_OUT), True,  a   ],
        [ ("first", D_IN), None, ("second_uniq", D_IN),  True,  a   ],
        [ ("first", D_IN), None, ("second_uniq", D_ANY), True,  a   ],
        [ ("first", D_IN), None, ("second_uniq", D_OUT), False, 0   ],
        [ ("first", D_IN), None, ("second_uniq", D_IN),  False, 0   ],
        [ ("first", D_IN), None, ("second_uniq", D_ANY), False, 0   ],

        [ ("first", D_OUT), None,  ("second_uniq", D_OUT), None, 0    ],
        [ ("first", D_OUT), True,  ("second_uniq", D_OUT), None, a-1  ],
        [ ("first", D_IN),  False, ("second_uniq", D_OUT), None, 0    ],
        [ ("first", D_IN),  False, ("second_uniq", D_OUT), True, 0    ],
                    ]:

        result = g.Neighborhood( "ROOT", hits = a+1,
                    neighbor = {
                        'adjacent'  :   {
                            'assert'    : a_asrt,
                            'arc'       : a_arc
                        },
                        'traverse'  :   {
                            'assert'    : t_asrt,
                            'arc'       : t_arc
                        }
                    }
        )

        Expect( len(result) == X,   "Assert?" )




###############################################################################
# query_second
#
###############################################################################
def query_second( g, a, b ):
    """
    """

    m = g.Memory(4)

    f2s_count = b+(a-1)*Nb

    s2f_count = f2s_count * (a-1) + b * a


    for a_arc1, Xa1, t_arc1, Xt1, C1, a_arc2, Xa2, t_arc2, Xt2, C2 in [
        [ ("first", D_IN), a, ("second", D_OUT), a, C_NONE, ("second", D_IN), a, ("second", D_IN), a, C_NONE ],     # no collection so traversal is non-greedy (like adjacent)
        [ ("first", D_IN), a, ("second", D_OUT), f2s_count, C_COLLECT, ("second", D_IN), f2s_count, ("second", D_IN), s2f_count, C_COLLECT ],  # collection for all traversals
                    ]:

        m.Reset()
        result = g.Neighborhood( "ROOT", hits = 1, sortby=S_VAL,
                    memory   = m,
                    neighbor = {
                        'adjacent'  :   {
                            'arc'       : a_arc1,
                            'filter'    : "inc(R1)"
                        },
                        'traverse'  :   {
                            'arc'       : t_arc1,
                            'filter'    : "inc(R2)",
                            'collect'   : C1,
                            'neighbor'  : {
                                'adjacent' : {
                                    'arc'       : a_arc2,
                                    'filter'    : "inc(R3)"
                                 },
                                'traverse' : {
                                    'arc'       : t_arc2,
                                    'filter'    : "inc(R4)",
                                    'collect'   : C2
                                 }
                             }
                        }
                    }
        )

        n_a_arc1 = m[R1] # Number of times adjacency check from first back to root was performed
        n_t_arc1 = m[R2] # Number of times outarcs from first to second were traversed
        n_a_arc2 = m[R3] # Number of times adjacency check from second back to first was performed
        n_t_arc2 = m[R4] # Number of times inarcs from second back to first were traversed

        Expect( n_a_arc1 == Xa1,   "R1 %d == %d ? (a=%d b=%d)" % (n_a_arc1, Xa1, a, b) )
        Expect( n_t_arc1 == Xt1,   "R2 %d == %d ? (a=%d b=%d)" % (n_t_arc1, Xt1, a, b) )
        Expect( n_a_arc2 == Xa2,   "R3 %d == %d ? (a=%d b=%d)" % (n_a_arc2, Xa2, a, b) )
        Expect( n_t_arc2 == Xt2,   "R4 %d == %d ? (a=%d b=%d)" % (n_t_arc2, Xt2, a, b) )


        print(a, b, n_a_arc1, n_t_arc1, n_a_arc2, n_t_arc2)





###############################################################################
# TEST_DivergentTraversal_basic
#
###############################################################################
def TEST_DivergentTraversal_basic():
    """
    Basic divergent traversal
    test_level=3202
    """

    g = Graph( "divergent" )

    g.CreateVertex( "ROOT" )


    for a in range( 1, Na+1 ):
        first = "first_%d" % a
        g.CreateVertex( first )
        g.Connect( "ROOT", ("first",M_INT,a), first )

        # TEST
        query_first_basic( g, a )
        query_first_assert( g, a )

        for b in range( 1, Nb+1 ):
            second_uniq = "second_uniq_%d_%d" % (a,b)
            g.Connect( first, ("second_uniq", M_INT, a*b), second_uniq )
            #
            # TODO: queries
            #query_second_uniq( g, a, b )


            second = "second_%d" % b
            g.Connect( first, ("second", M_INT, b), second )
            # TEST
            query_second( g, a, b )

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
