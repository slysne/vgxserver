###############################################################################
# 
# VGX Server
# Distributed engine for plugin-based graph and vector search
# 
# Module:  pyvgx.test
# File:    Adjacent.py
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
import random
from . import _query_test_support as QuerySupport


ALL_MOD = set( [M_DIST, M_LSH, M_INT, M_UINT, M_FLT, M_CNT, M_ACC, M_TMC, M_TMM, M_TMX] )


###############################################################################
# TEST_Adjacent
#
###############################################################################
def TEST_Adjacent():
    """
    pyvgx.Graph.Adjacent()
    test_level=3201
    """
    levels = 2
    for fanout_factor in [1,10,100]:
        for modifiers in [ [M_INT], [M_INT,M_UINT], [M_FLT], [M_INT,M_FLT], [M_INT,M_UINT,M_FLT] ]:

            g = QuerySupport.NewFanout( "fanout", "root", fanout_factor=fanout_factor, levels=levels, modifiers=modifiers )

            Expect( g.Adjacent( "root" ) == True,                                               "root is adjacent to others" )
            Expect( g.Adjacent( "root", None, neighbor="level_1_0" ) == True,                   "root is adjacent to level_1_0" )
            Expect( g.Adjacent( "root", ("to_level_1",D_OUT), neighbor="level_1_0" ) == True,   "relationship should exist" )
            Expect( g.Adjacent( "root", ("to_level_1",D_OUT), neighbor="level_2_0" ) == False,  "relationship should not exist" )
            Expect( g.Adjacent( "root", ("to_level_2",D_OUT), neighbor="level_1_0" ) == False,  "relationship should not exist" )
            Expect( g.Adjacent( "root", ("to_level_2",D_OUT), neighbor="level_2_0" ) == False,  "relationship should not exist" )

            Expect( g.Adjacent( "level_1_0", ("to_level_1",D_IN), neighbor="root" ) == True,        "(root) <-[to_level_1]- (level_1_0)" )
            Expect( g.Adjacent( "level_1_0", ("to_level_1",D_OUT), neighbor="root" ) == False,      "NOT (level_1_0) -[to_level_1]-> (root)" )

            Expect( g.Adjacent( "level_1_0", ("to_level_2",D_OUT), neighbor="level_2_0" ) == True,  "(level_1_0) -[to_level_1]-> (level_2_0)" )
            Expect( g.Adjacent( "level_1_0", ("to_level_2",D_IN), neighbor="level_2_0" ) == False,  "NOT (level_2_0) <-[to_level_1]- (level_1_0)" )

            Expect( g.Adjacent( "level_2_0" ) == False,                     "NOT (level_2_0) -[*]-> (*)" )
            Expect( g.Adjacent( "level_2_0", D_OUT ) == False,              "NOT (level_2_0) -[*]-> (*)" )
            Expect( g.Adjacent( "level_2_0", D_IN ) == True,                "(*) <-[]- (level_2_0)" )

            unused_modifiers = ALL_MOD.symmetric_difference( modifiers )

            for fx in range( fanout_factor ):
                for mod in modifiers:
                    Expect( g.Adjacent( "root", ("to_level_1", D_OUT, mod, V_EQ, fx), "level_1_%d" % fx ) == True,    "EQ" )
                    Expect( g.Adjacent( "root", ("to_level_1", D_OUT, mod, V_GT, fx), "level_1_%d" % fx ) == False,   "NOT GT" )
                    Expect( g.Adjacent( "root", ("to_level_1", D_OUT, mod, V_LT, fx), "level_1_%d" % fx ) == False,   "NOT LT" )
                for mod in unused_modifiers:
                    Expect( g.Adjacent( "root", ("to_level_1", D_OUT, mod), "level_1_%d" % fx ) == False,             "unused modifier" )
                    if mod != M_LSH:
                        Expect( g.Adjacent( "root", ("to_level_1", D_OUT, mod, V_EQ, fx), "level_1_%d" % fx ) == False,   "unused modifier" )
                        Expect( g.Adjacent( "root", ("to_level_1", D_OUT, mod, V_GT, fx), "level_1_%d" % fx ) == False,   "unused modifier" )
                        Expect( g.Adjacent( "root", ("to_level_1", D_OUT, mod, V_LT, fx), "level_1_%d" % fx ) == False,   "unused modifier" )
    g.Truncate()




###############################################################################
# TEST_Adjacent_LSH
#
###############################################################################
def TEST_Adjacent_LSH():
    """
    pyvgx.Graph.Adjacent() for M_LSH modifier
    test_level=3201
    """

    def get_ham(x, dist):
        h = x ^ sum( [1<<i for i in random.sample( range(15), dist )] )
        return h

    g = Graph("arclsh")

    g.CreateVertex( "A" )
    g.CreateVertex( "B" )

    for dist in range(15):
        b = random.randint(0,2**32-1)
        g.Connect( "A", ("to", M_LSH, b), "B" )
        b1 = get_ham(b, dist)
        Expect( g.Adjacent( "A", ("to", D_OUT, M_LSH, V_LTE, (b1,dist)) ) == True,          "match within hamming distance {} ({:032b} {:032b} h={})".format(dist,b,b1,popcnt(b^b1)) )
        Expect( g.Adjacent( "A", ("to", D_OUT, M_LSH, V_GT, (b1,dist)) ) == False,          "match within hamming distance {} ({:032b} {:032b} h={})".format(dist,b,b1,popcnt(b^b1)) )
        b2 = get_ham(b, dist+1)
        Expect( g.Adjacent( "A", ("to", D_OUT, M_LSH, V_LTE, (b2,dist)) ) == False,         "no match within hamming distance {} ({:032b} {:032b} h={})".format(dist+1,b,b2,popcnt(b^b2)) )
        Expect( g.Adjacent( "A", ("to", D_OUT, M_LSH, V_GT, (b2,dist)) ) == True,           "no match within hamming distance {} ({:032b} {:032b} h={})".format(dist+1,b,b2,popcnt(b^b2)) )

    g.Truncate()





###############################################################################
# Run
#
###############################################################################
def Run( name ):
    """
    """
    RunTests( [__name__] )
