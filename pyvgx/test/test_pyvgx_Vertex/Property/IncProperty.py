###############################################################################
# 
# VGX Server
# Distributed engine for plugin-based graph and vector search
# 
# Module:  pyvgx
# File:    IncProperty.py
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

graph = None




###############################################################################
# TEST_IncProperty
#
###############################################################################
def TEST_IncProperty():
    """
    pyvgx.Vertex.IncProperty()
    Basic
    test_level=3101
    """
    if "vertex" in graph:
        graph.DeleteVertex( "vertex" )

    V = graph.NewVertex( "vertex" )

    Expect( "val" not in V,                     "no previous value" )

    Expect( V.IncProperty( "val" ) == 1,        "new value, set to 1" )
    Expect( V.IncProperty( "val" ) == 2,        "value = 2" )
    Expect( V.IncProperty( "val" ) == 3,        "value = 3" )
    Expect( V.IncProperty( "val", 0 ) == 3,     "value = 3" )
    Expect( V.IncProperty( "val", -1 ) == 2,    "value = 2" )
    Expect( V.IncProperty( "val", -2 ) == 0,    "value = 0" )
    Expect( V.IncProperty( "val", 2 ) == 2,     "value = 2" )
    Expect( V.IncProperty( "val", -3 ) == -1,   "value = -1" )
    Expect( V.IncProperty( "val" ) == 0,        "value = 0" )

    # Limits
    Expect( V.IncProperty( "val", 2**55-2 ) == 2**55-2,         "almost at positive limit" )
    Expect( V.IncProperty( "val" ) == 2**55-1,                  "at positive limit" )
    Expect( V.IncProperty( "val", -(2**55-1) ) == 0,            "value = 0" )
    Expect( V.IncProperty( "val", -(2**55-1) ) == -(2**55-1),   "almost at negative limit" )
    Expect( V.IncProperty( "val", -1 ) == -2**55,               "at negative limit" )
    V.IncProperty( "val" )
    Expect( V.IncProperty( "val", 2**55-1 ) == 0,               "value = 0" )

    # Wrap
    max_pos = (1<<55)-1
    min_neg = -(1<<55)
    V["val"] = max_pos - 2
    Expect( V.IncProperty( "val" ) == max_pos - 1,              "value = %d" % (max_pos-1) )
    Expect( V.IncProperty( "val" ) == max_pos,                  "value = %d" % (max_pos) )
    Expect( V.IncProperty( "val" ) == min_neg,                  "wrap to negative value = %d" % (min_neg) )
    Expect( V.IncProperty( "val" ) == min_neg + 1,              "value = %d" % (min_neg+1) )
    Expect( V.IncProperty( "val", -1 ) == min_neg,              "value = %d" % (min_neg) )
    Expect( V.IncProperty( "val", -1 ) == max_pos,              "wrap to positive value = %d" % (max_pos) )
    Expect( V.IncProperty( "val", -1 ) == max_pos - 1,          "wrap to positive value = %d" % (max_pos - 1) )

    # Convert to float
    V["val"] = 0
    Expect( V.IncProperty( "val" ) == 1,                        "value = 1" )
    Expect( type(V.IncProperty( "val", 2.0 )) is float,         "value converted to float" )
    Expect( V['val'] == 3.0,                                    "value = 3.0" )
    Expect( V.IncProperty( "val", 2 ) == 5.0,                   "value = 5.0" )
    Expect( V.IncProperty( "val", 2.5 ) == 7.5,                 "value = 7.5" )
    Expect( V.IncProperty( "val", -10 ) == -2.5,                "value = -2.5" )

    # Near float limits
    Expect( V.IncProperty( "val", 1e300 ) == 1e300,             "large float" )
    V['val'] = 0
    Expect( V.IncProperty( "val", 1e-300 ) == 1e-300,           "small float" )
    V['val'] = 0
    Expect( V.IncProperty( "val", -1e300 ) == -1e300,           "large negative float" )
    V['val'] = 0
    Expect( V.IncProperty( "val", -1e-300 ) == -1e-300,         "small negative float" )





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
    graph.Close()
    del graph
