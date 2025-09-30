###############################################################################
# 
# VGX Server
# Distributed engine for plugin-based graph and vector search
# 
# Module:  pyvgx.test
# File:    Collector.py
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





###############################################################################
# TEST_Collector_LockedTail
#
###############################################################################
def TEST_Collector_LockedTail():
    """
    Excercise collector logic to re-acquire tail lock after
    being discarded from heap.
    test_level=3101
    """
    graph_name = "special_collector_locked_tail"
    g = Graph( graph_name )

    #
    #
    # (A) -[2]-> (B) -[1]-> (C)
    #    \-[4]-> (D) -[3]-> (E)
    #
    g.Connect( "A", ("to",M_INT,2), "B" )
    g.Connect( "B", ("to",M_INT,1), "C" )
    g.Connect( "A", ("to",M_INT,4), "D" )
    g.Connect( "D", ("to",M_INT,3), "E" )


    # Verify native collection order: B->C, A->B, D->E, A->D
    native = g.Neighborhood( "A", 
        collect     =   C_COLLECT,
        arc         =   ( "to", D_OUT ),
        neighbor    =   {
            'traverse': {
                'collect'   :   C_COLLECT,
                'arc'       :   ("to", D_OUT)
            }
        },
        sortby      =   S_NONE,
        fields      =   F_ANCHOR | F_ID,
        result      =   R_LIST,
        hits        =   4
    )
    Expect( native[0] == ("B", "C") )
    Expect( native[1] == ("A", "B") )
    Expect( native[2] == ("D", "E") )
    Expect( native[3] == ("A", "D") )


    # Now 
    #
    #
    #
    single_replace_each_hit = g.Neighborhood( "A", 
        collect     =   C_COLLECT,
        arc         =   ( "to", D_OUT ),
        neighbor    =   {
            'traverse': {
                'collect'   :   C_COLLECT,
                'arc'       :   ("to", D_OUT)
            }
        },
        sortby      =   S_VAL | S_DESC,
        fields      =   F_AARC,
        result      =   R_LIST,
        hits        =   1
    )


    g.Erase()


    

###############################################################################
# TEST_Collector_LockedHead
#
###############################################################################
def TEST_Collector_LockedHead():
    """
    Excercise collector logic to re-acquire head lock after
    being discarded from heap.
    test_level=3101
    """
    graph_name = "special_collector_locked_head"
    g = Graph( graph_name )

    #
    #
    # (A) -[r1,2]-->(B) -[to,2]-> (C) -[to,1]-> (D)
    #    \-[r2,4]-/    \-[to,4]-> (E) -[to,3]-> (F)
    #            
    #
    g.Connect( "A", ("r1",M_INT,2), "B" )
    g.Connect( "A", ("r2",M_INT,4), "B" )

    g.Connect( "B", ("to",M_INT,2), "C" )
    g.Connect( "C", ("to",M_INT,1), "D" )

    g.Connect( "B", ("to",M_INT,4), "E" )
    g.Connect( "E", ("to",M_INT,3), "F" )


    # Verify native collection order: C-[1]->D, A-[2]->B, E-[3]->F, A-[4]->B
    native = g.Neighborhood( "A", 
        collect     =   C_COLLECT,
        arc         =   ( "*", D_OUT, M_INT ),
        neighbor    =   {
            'traverse'  : {
                'collect'   :   C_NONE,
                'arc'       :   ("to", D_OUT, M_INT),
                'filter'    :   "next.arc.value == prev.arc.value",
                'neighbor'  :   {
                    'traverse': {
                        'collect'   :   C_COLLECT,
                        'arc'       :   ("to", D_OUT, M_INT)
                    }
                }
            }
        },
        sortby      =   S_NONE,
        fields      =   F_ANCHOR | F_ID | F_VAL,
        result      =   R_LIST,
        hits        =   4
    )
    Expect( native[0] == ("C", 1, "D") )
    Expect( native[1] == ("A", 2, "B") )
    Expect( native[2] == ("E", 3, "F") )
    Expect( native[3] == ("A", 4, "B") )


    # Now 
    #
    #
    #
    single_replace_each_hit = g.Neighborhood( "A", 
        collect     =   C_COLLECT,
        arc         =   ( "*", D_OUT, M_INT ),
        neighbor    =   {
            'traverse'  : {
                'collect'   :   C_NONE,
                'arc'       :   ("to", D_OUT, M_INT),
                'filter'    :   "next.arc.value == prev.arc.value",
                'neighbor'  :   {
                    'traverse': {
                        'collect'   :   C_COLLECT,
                        'arc'       :   ("to", D_OUT, M_INT)
                    }
                }
            }
        },
        sortby      =   S_VAL | S_DESC,
        fields      =   F_VAL | F_ID,
        result      =   R_LIST,
        hits        =   1
    )

    g.Erase()




###############################################################################
# Run
#
###############################################################################
def Run( name ):
    """
    """
    RunTests( [__name__] )
