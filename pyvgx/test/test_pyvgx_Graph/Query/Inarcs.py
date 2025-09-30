###############################################################################
# 
# VGX Server
# Distributed engine for plugin-based graph and vector search
# 
# Module:  pyvgx.test
# File:    Inarcs.py
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




###############################################################################
# TEST_Inarcs
#
###############################################################################
def TEST_Inarcs():
    """
    pyvgx.Graph.Inarcs()
    test_level=3101
    """
    levels = 1
    for fanout_factor in [1,10,100]:
        for modifiers in [ [M_INT], [M_INT,M_UINT], [M_FLT], [M_INT,M_FLT], [M_INT,M_UINT,M_FLT] ]:
            g = QuerySupport.NewFanout( "fanout", "root", fanout_factor=fanout_factor, levels=levels, modifiers=modifiers )
            root = "root"
            Expect( len( g.Inarcs( root ) ) == 0,                   "root has no inarcs" )

            for fx in range( fanout_factor ):
                inarcs = g.Inarcs( "level_1_%d" % fx )
                indegree = len( modifiers )
                Expect( len( inarcs ) == indegree,                  "level_1_%d has %d inarcs" % (fx, indegree) )
                for arcdir, rel, mod, val, init in inarcs:
                    Expect( eval(arcdir) == D_IN,                   "inarc" )
                    Expect( rel == "to_level_1",                    "relationship" )
                    Expect( val == fx,                              "val = %d" % fx )
                    Expect( init == root,                           "root is the only initial" )

                inarc_modifiers = set( [eval(mod) for arcdir, rel, mod, val, init in inarcs] )
                Expect( set( inarc_modifiers ) == set( modifiers ), "set of modifiers: %s" % modifiers )
    g.Truncate()
                



###############################################################################
# Run
#
###############################################################################
def Run( name ):
    """
    """
    RunTests( [__name__] )
