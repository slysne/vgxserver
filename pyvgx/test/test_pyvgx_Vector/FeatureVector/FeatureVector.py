###############################################################################
# 
# VGX Server
# Distributed engine for plugin-based graph and vector search
# 
# Module:  pyvgxtest
# File:    FeatureVector.py
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

from pyvgxtest.pyvgxtest import RunTests, Expect, TestFailed, PerformCleanup
from pyvgx import *
import pyvgx
import random
import math
import sys
import time
import re


graph = None




###############################################################################
# TEST_Vector
#
###############################################################################
def TEST_Vector():
    """
    pyvgx.Vector()
    Verify that pyvgx.Vector() creates a new, correctly initialized pyvgx.Vector object
    test_level=3101
    """
    Expect( type( Vector() ) is Vector )
    Expect( len( Vector() ) == 0 )
    Expect( len( Vector( [('a',1)] ) ) == 1 )





###############################################################################
# Run
#
###############################################################################
def Run( name ):
    """
    """
    global graph
    pyvgx.system.Initialize( name, euclidean=False )
    try:
        graph = pyvgx.Graph( name )
        RunTests( [__name__] )
        graph.Close()
        del graph
        PerformCleanup()
    finally:
        pyvgx.system.Unload()
