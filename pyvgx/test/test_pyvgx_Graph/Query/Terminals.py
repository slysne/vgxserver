###############################################################################
# 
# VGX Server
# Distributed engine for plugin-based graph and vector search
# 
# Module:  pyvgx.test
# File:    Terminals.py
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
# TEST_Terminals
#
###############################################################################
def TEST_Terminals():
    """
    pyvgx.Graph.Terminals()
    test_level=3101
    """
    levels = 1
    for fanout_factor in [1,10,100]:
        current_terminals = ["level_1_%d" % fx for fx in range(fanout_factor)]
        expect_terminals = set( current_terminals )
        for modifiers in [ [M_INT], [M_INT,M_UINT], [M_FLT], [M_INT,M_FLT], [M_INT,M_UINT,M_FLT] ]:
            g = QuerySupport.NewFanout( "fanout", "root", fanout_factor=fanout_factor, levels=levels, modifiers=modifiers )
            root = "root"
            terminals = g.Terminals( root )
            Expect( set( terminals ) == expect_terminals,               "all terminals" )

            Expect( len( g.Terminals( root, hits=0 ) ) == 0,            "hits = 0" )
            Expect( len( g.Terminals( root, hits=1 ) ) == 1,            "hits = 1" )

            strsorted_terminals = g.Terminals( root, sortby=S_ID ) # asc by default
            Expect( strsorted_terminals == sorted( current_terminals ),                 "sorted by ID, ascending" )

            strsorted_terminals = g.Terminals( root, sortby=S_ID|S_DESC )
            Expect( strsorted_terminals == sorted( current_terminals, reverse=True ),   "sorted by ID, descending" )

            valsorted_terminals = g.Terminals( root, sortby=S_RANK, rank="next[ 'number' ]" ) # desc by default
            desc = list( current_terminals )
            desc.reverse()
            Expect( valsorted_terminals == desc,                                        "sorted by property int value, descending" )

            valsorted_terminals = g.Terminals( root, sortby=S_RANK|S_ASC, rank="next[ 'number' ]" )
            Expect( valsorted_terminals == current_terminals,                           "sorted by property int value, ascending" )
            

            for fx in range( fanout_factor ):
                name = 'level_1_%d' % fx
                single = g.Terminals( root, neighbor={ 'id':name } )
                Expect( len( single ) == 1 and single[0] == name,               "single terminal" )
                all_level1 = g.Terminals( root, neighbor={ 'property':{'level':1} } )
                Expect( len( all_level1 ) == fanout_factor,                     "all terminals" )
                single = g.Terminals( root, neighbor={ 'property':{'name':name} } )
                Expect( len( single ) == 1 and single[0] == name,               "single terminal" )


            result = g.Terminals( root, neighbor={ 'id':'level*' } ) 
            result_filter = g.Terminals( root, filter='next.id == "level*"' )
            Expect( result == result_filter,                                "Filter syntax equivalent" ) 
            Expect( len( result ) == fanout_factor,                         "all terminals match id wildcard" )

            result = g.Terminals( root, neighbor={ 'id':'no*' } ) 
            result_filter = g.Terminals( root, filter='next.id == "no*"' )
            Expect( result == result_filter,                                "Filter syntax equivalent" ) 
            Expect( len( result ) == 0,                                     "no terminals match id wildcard" )
            
            result = g.Terminals( root, neighbor={ 'property':{'name':'lev*'} } ) 
            result_filter = g.Terminals( root, filter='next["name"] == "lev*"' )
            Expect( result == result_filter,                                "Filter syntax equivalent" ) 
            Expect( len( result ) == fanout_factor,                         "all terminals match property string wildcard" )
            
            result = g.Terminals( root, neighbor={ 'property':{'name':'no*'} } ) 
            result_filter = g.Terminals( root, filter='next["name"] == "no*"' )
            Expect( result == result_filter,                                "Filter syntax equivalent" ) 
            Expect( len( result ) == 0,                                     "no terminals match property string wildcard" )
    g.Truncate()
 





###############################################################################
# Run
#
###############################################################################
def Run( name ):
    """
    """
    RunTests( [__name__] )
