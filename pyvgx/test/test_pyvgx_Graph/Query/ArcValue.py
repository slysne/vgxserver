###############################################################################
# 
# VGX Server
# Distributed engine for plugin-based graph and vector search
# 
# Module:  pyvgx
# File:    ArcValue.py
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




def TEST_ArcValue():
    """
    pyvgx.Graph.ArcValue()
    test_level=3201
    """
    
    levels = 2
    for fanout_factor in [1,10,100]:
        for modifiers in [ [M_INT], [M_INT,M_UINT], [M_FLT], [M_INT,M_FLT], [M_INT,M_UINT,M_FLT] ]:
            g = QuerySupport.NewFanout( "fanout", "arcvalue_root", fanout_factor=fanout_factor, levels=levels, modifiers=modifiers )
            root = "arcvalue_root"
            if M_INT in modifiers:
                for fx in range( fanout_factor ):
                    Expect( g.ArcValue( root, ("to_level_1", D_OUT, M_INT), "level_1_%d" % fx ) == fx,    "M_INT arc value = %d" % fx )
            if M_FLT in modifiers:
                for fx in range( fanout_factor ):
                    Expect( g.ArcValue( root, ("to_level_1", D_OUT, M_FLT), "level_1_%d" % fx ) == float(fx),    "M_FLT arc value = %f" % float(fx) )
    g.Truncate()




 



def Run( name ):
    RunTests( [__name__] )
