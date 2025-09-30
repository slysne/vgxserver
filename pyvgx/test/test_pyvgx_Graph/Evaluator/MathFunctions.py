###############################################################################
# 
# VGX Server
# Distributed engine for plugin-based graph and vector search
# 
# Module:  pyvgxtest
# File:    MathFunctions.py
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
# TEST_Evaluator_MathFunctions
#
###############################################################################
def TEST_Evaluator_MathFunctions():
    """
    pyvgx Evaluator Math Functions
    test_level=3101
    """
    graph_name = "eval_graph"
    g = Graph( graph_name )

    Expect( g.Evaluate( "sqrt( 10 ) * sqrt( 10 ) == 10" ) )

    g.Erase()






###############################################################################
# Run
#
###############################################################################
def Run( name ):
    """
    """
    RunTests( [__name__] )
