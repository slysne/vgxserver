###############################################################################
# 
# VGX Server
# Distributed engine for plugin-based graph and vector search
# 
# Module:  pyvgx.test
# File:    RemoveProperty.py
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



NODE = "Vertex with Property"
SETUP = False



###############################################################################
# TEST_RemoveProperty_setup
#
###############################################################################
def TEST_RemoveProperty_setup():
    """
    pyvgx.Vertex.RemoveProperty()
    Setup
    t_nominal=1
    test_level=3101
    """
    global SETUP
    if not SETUP:
        graph.Truncate()
        V = graph.NewVertex( NODE, "prop_node" )
        Expect( V.HasProperties() is False,                           "Vertex should have no properties" )
        Expect( V.NumProperties() == 0,                               "Vertex should have no properties" )
        graph.CloseVertex( V )
        SETUP = True




###############################################################################
# TEST_RemoveProperty_set_and_remove
#
###############################################################################
def TEST_RemoveProperty_set_and_remove():
    """
    pyvgx.Vertex.RemoveProperty()
    Set and remove various properties
    t_nominal=2
    test_level=3101
    """
    TEST_RemoveProperty_setup()

    V = graph.OpenVertex( NODE, mode="a" )

    # Gradually set more and more properties and remove them one by one
    N = 1000
    for n in range( N ):
        D = dict([("p%s"%x,x) for x in range(n)])
        V.SetProperties( D )
        Expect( V.NumProperties() == n,                                 "Vertex should have %d properties, got %d" % (n, V.NumProperties()) )
        # Now remove them one by one
        c = n
        for k,v in list(D.items()):
            Expect( V[k] == v,                                          "property '%s' should not yet be removed" % k )
            V.RemoveProperty( k )
            c -= 1
            Expect( V.NumProperties() == c,                             "Vertex should have %d properties, got %d" % (c, V.NumProperties()) )
            Expect( V.HasProperty( k ) == False,                        "property '%s' should be removed" % k )

    
    # Close vertex
    graph.CloseVertex( V )






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
