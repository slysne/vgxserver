###############################################################################
# 
# VGX Server
# Distributed engine for plugin-based graph and vector search
# 
# Module:  pyvgx.test
# File:    Initials.py
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
# TEST_Initials
#
###############################################################################
def TEST_Initials():
    """
    pyvgx.Graph.Initials()
    test_level=3101
    """
    levels = 1
    for fanout_factor in [1,10,100]:
        current_terminals = ["level_1_%d" % fx for fx in range(fanout_factor)]
        expect_terminals = set( current_terminals )
        for modifiers in [ [M_INT], [M_INT,M_UINT], [M_FLT], [M_INT,M_FLT], [M_INT,M_UINT,M_FLT] ]:
            g = QuerySupport.NewFanout( "fanout", "root", fanout_factor=fanout_factor, levels=levels, modifiers=modifiers )
            root = "root"

            for fx in range( fanout_factor ):
                 single = g.Initials( "level_1_%d" % fx )
                 Expect( len(single) == 1 and single[0] == root,        "root is only initial" )
    g.Truncate()








###############################################################################
# Run
#
###############################################################################
def Run( name ):
    """
    """
    RunTests( [__name__] )
