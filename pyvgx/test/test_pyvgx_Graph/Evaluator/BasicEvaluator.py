###############################################################################
# 
# VGX Server
# Distributed engine for plugin-based graph and vector search
# 
# Module:  pyvgx.test
# File:    BasicEvaluator.py
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


g = None
graph_name = "eval_graph"


# TODO: This is a stub. We need to add tests for all basic functionality
#
#



###############################################################################
# TEST_properties
#
###############################################################################
def TEST_properties():
    """
    Evaluator properties
    t_nominal=1
    test_level=501
    """
    g.Truncate()

    A = g.NewVertex("A")
    val1 = "First Disk Property"
    val2 = "Second Disk Property"
    val3 = "Third Disk Property"
    A['*vprop1'] = val1
    A['*vprop2'] = val2
    A['*vprop3'] = val3
    A.Close()

    Expect( g.Evaluate( f"vertex.property('vprop1') == '{val1}'", tail="A" ),       "vprop1" )
    Expect( g.Evaluate( f"vertex.property('vprop2') == '{val2}'", tail="A" ),       "vprop2" )
    Expect( g.Evaluate( f"vertex.property('vprop3') == '{val3}'", tail="A" ),       "vprop3" )

    vprop1 = g.Evaluate( "vertex.property('vprop1')", tail="A" )
    vprop2 = g.Evaluate( "vertex.property('vprop2')", tail="A" )
    vprop3 = g.Evaluate( "vertex.property('vprop3')", tail="A" )

    Expect( vprop1 == val1,                                         f"'{val1}', got '{vprop1}'" )
    Expect( vprop2 == val2,                                         f"'{val2}', got '{vprop2}'" )
    Expect( vprop3 == val3,                                         f"'{val3}', got '{vprop3}'" )

    joined = g.Evaluate( f"a = vertex.property('vprop1'); b = vertex.property('vprop2'); c = vertex.property('vprop3'); join(',',a,b,c)", tail="A" )
    xval = ",".join([val1,val2,val3])
    Expect( joined == xval,                                         f"'{xval}', got '{joined}'" )




###############################################################################
# Run
#
###############################################################################
def Run( name ):
    """
    """
    global g
    g = Graph( graph_name )

    RunTests( [__name__] )

    g.Erase()
