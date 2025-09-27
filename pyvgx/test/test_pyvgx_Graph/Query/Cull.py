###############################################################################
# 
# VGX Server
# Distributed engine for plugin-based graph and vector search
# 
# Module:  pyvgx
# File:    Cull.py
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
from . import _query_test_support as QuerySupport
from pyvgx import *
import pyvgx




###############################################################################
# TEST_mcull
#
###############################################################################
def TEST_mcull():
    """
    Test mcull() filter 
    test_level=3201
    """
    levels = 2
    for fanout_factor in [1, 2, 10, 100]:
        for modifiers in [ [M_INT], [M_INT,M_FLT], [M_INT,M_UINT,M_FLT] ]:
            g = QuerySupport.NewFanout( "fanout", "root", fanout_factor=fanout_factor, levels=levels, modifiers=modifiers )
            root = "root"

            # First level
            first = g.Neighborhood(
                id       = root,
                arc      = ('to_level_1', D_OUT),
                fields   = F_AARC,
                result   = R_DICT,
                neighbor = {
                    'filter':   """
                                is_int_arc = int( prev.arc.mod == M_INT );
                                score = is_int_arc * prev.arc.value;
                                do( mcull( score, 3 ) );
                                """
                }
            )

            Expect( len(first) <= 3 )
            minval = fanout_factor - len( first )
            maxval = fanout_factor - 1
            valset = set( [v for v in range(minval, maxval+1)] )
            for entry in first:
                tail = entry['anchor']
                rel = entry['arc']['relationship']
                mod = entry['arc']['modifier']
                val = entry['arc']['value']
                head = entry['id']
                Expect( tail == "root",                 "First level anchor = 'root'" )
                Expect( rel == 'to_level_1',            "rel = to_level_1" )
                Expect( mod == 'M_INT',                 "mod = 'M_INT'" )
                Expect( head.startswith('level_1_'),    "First level head 'level_1_*'" )
                Expect( val in valset,                  "val in {}".format( valset ) )

            # Second level
            second = g.Neighborhood(
                id       = root,
                arc      = ('*', D_OUT),
                fields   = F_AARC,
                result   = R_DICT,
                collect  = C_SCAN,
                neighbor = {
                    'filter':   """
                                is_int_arc = int( prev.arc.mod == M_INT );
                                score = is_int_arc * prev.arc.value;
                                do( mcull( score, 3 ) );
                                """,
                    'traverse': {
                        'arc': ('*', D_OUT),
                        'collect': C_COLLECT,
                        'neighbor': {
                            'filter':   """
                                        is_float_arc = int( prev.arc.mod == M_FLT );
                                        score = is_float_arc * prev.arc.value;
                                        do( mcull( score, 3 ) );
                                        """
                        }
                    }
                }
            )

            minfirst = fanout_factor - 3 if fanout_factor > 3 else 0
            maxfirst = fanout_factor - 1
            anchorset = set( ["level_1_{}".format(n) for n in range(minfirst, maxfirst+1)] )
            for entry in second:
                tail = entry['anchor']
                rel = entry['arc']['relationship']
                mod = entry['arc']['modifier']
                val = entry['arc']['value']
                head = entry['id']
                Expect( tail in anchorset,              "Second level anchor in {}".format(anchorset) )
                Expect( rel == 'to_level_2',            "rel = to_level_2" )
                Expect( mod == 'M_FLT',                 "mod = 'M_FLT'" )
                Expect( head.startswith('level_2_'),    "Second level head 'level_2_*'" )


    g.Truncate()
                




###############################################################################
# Run
#
###############################################################################
def Run( name ):
    """
    """
    RunTests( [__name__] )
