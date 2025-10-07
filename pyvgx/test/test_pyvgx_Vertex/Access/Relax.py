###############################################################################
# 
# VGX Server
# Distributed engine for plugin-based graph and vector search
# 
# Module:  pyvgx.test
# File:    Relax.py
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

graph = None




###############################################################################
# TEST_Relax
#
###############################################################################
def TEST_Relax():
    """
    pyvgx.Vertex.Relax()
    Call method
    test_level=3101
    """
    name = "relaxable"

    if name in graph:
        graph.DeleteVertex( name )

    graph.CreateVertex( name )

    # Open writable
    V = graph.OpenVertex( name, "a" )
    Expect( V.Readonly() is False,           "Vertex should not be readonly" )
    Expect( V.Writable() is True,            "Vertex should be writable" )

    # Relax to readonly
    V.Relax()
    Expect( V.Readonly() is True,           "Vertex should be readonly after relax" )
    Expect( V.Writable() is False,          "Vertex should be not writable after relax" )

    V.Close()




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
