###############################################################################
# 
# VGX Server
# Distributed engine for plugin-based graph and vector search
# 
# Module:  pyvgx.test
# File:    Filter.py
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
from pyvgx import *
import pyvgx




###############################################################################
# TEST_Filter_global_rank
#
###############################################################################
def TEST_Filter_global_rank():
    """
    test_level=3101
    """
    graph_name = "special_filter_global_rank"
    g = Graph( graph_name )

    N = 100
    for n in range( N ):
        V = g.NewVertex( str(n) )
        V['n'] = n
        g.CloseVertex( V )

    # zero hits
    result = g.Vertices( sortby=S_RANK|S_ASC, rank="vertex['n']", hits=0 )
    Expect( result == [] )

    # ascending
    for h in range( 1, N+1 ):
        expected = [ str(n) for n in range(h) ]
        result1 = g.Vertices( sortby=S_RANK|S_ASC, rank="vertex['n']", hits=h )
        result2 = g.Vertices( sortby=S_RANK|S_ASC, rank="next['n']", hits=h )
        Expect( result1 == expected, "%s, got %s" % (expected, result1) )
        Expect( result1 == result2, "vertex == next in this context" )

    # descending
    for h in range( 1, N+1 ):
        expected = [ str(N-1-n) for n in range(h) ]
        result1 = g.Vertices( sortby=S_RANK|S_DESC, rank="vertex['n']", hits=h )
        result2 = g.Vertices( sortby=S_RANK|S_DESC, rank="next['n']", hits=h )
        Expect( result1 == expected, "%s, got %s" % (expected, result1) )
        Expect( result1 == result2, "vertex == next in this context" )


    nonsense = g.Vertices( sortby=S_RANK|S_DESC, rank="prev['n']", hits=10 )


    g.Erase()





###############################################################################
# Run
#
###############################################################################
def Run( name ):
    """
    """
    RunTests( [__name__] )
