###############################################################################
# 
# VGX Server
# Distributed engine for plugin-based graph and vector search
# 
# Module:  pyvgx.test
# File:    Parallel.py
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

from pyvgxtest.pyvgxtest import RunTests, Expect, TestFailed, PerformCleanup
from pyvgxtest.threads import Worker
from pyvgx import *
import pyvgx
#from pyframehash import *
import random
import time



graph_name = "parallel"




###############################################################################
# get_graph
#
###############################################################################
def get_graph( create=False  ):
    """
    """
    g = Graph( graph_name )
    if create:
        g.Truncate()
        for n in range( 1000000 ):
            for i in range( 10 ):
                g.Connect( str(n), "to", str(n+i) )
                g.Connect( str(i), ("index",M_INT,n), str(n) )


    return g




###############################################################################
# arcs
#
###############################################################################
def arcs( g ):
    """
    """
    t0 = timestamp()
    g.Arcs( hits=10, sortby=S_DEG )
    t1 = timestamp()
    LogInfo( "Arcs(): %.2fs" % (t1-t0) )




###############################################################################
# vertices
#
###############################################################################
def vertices( g ):
    """
    """
    t0 = timestamp()
    g.Vertices( hits=10, sortby=S_DEG )
    t1 = timestamp()
    LogInfo( "Vertices(): %.2fs" % (t1-t0) )




###############################################################################
# neighborhood
#
###############################################################################
def neighborhood( g ):
    """
    """
    root = str( random.randint(0,9) )
    t0 = timestamp()
    g.Neighborhood( root, hits=10, arc=("index",D_OUT,M_INT), sortby=S_VAL )
    t1 = timestamp()
    LogInfo( "Neighborhood(): %.2fs" % (t1-t0) )




###############################################################################
# terminate
#
###############################################################################
def terminate( self ):
    """
    """
    self.terminate()




###############################################################################
# TEST_Parallel_Writable
#
###############################################################################
def TEST_Parallel_Writable():
    """
    Test concurrent pyvgx.Vertices(), pyvgx.Arcs() and pyvgx.Neighborhood() in WRITABLE graph
    t_nominal=177
    test_level=3201
    """

    g = get_graph( create=True )
    g.DebugCheckAllocators()

    W = [ Worker(str(x)) for x in range(8) ]

    for i in range(5):
        for w in W:
            w.perform( arcs, g )
        for w in W:
            w.perform( vertices, g )
        for w in W:
            w.perform( neighborhood, g )


    for w in W:
        w.perform( terminate, w )

    
    while len([1 for w in W if not w.is_dead()]):
        time.sleep( 1 )

    g.DebugCheckAllocators()




###############################################################################
# TEST_Parallel_Readonly
#
###############################################################################
def TEST_Parallel_Readonly():
    """
    Test concurrent pyvgx.Vertices(), pyvgx.Arcs() and pyvgx.Neighborhood() in READONLY graph
    t_nominal=36
    test_level=3201
    """

    g = get_graph()
    g.DebugCheckAllocators()

    g.SetGraphReadonly( 60000 )
    g.DebugCheckAllocators()

    W = [ Worker(str(x)) for x in range(8) ]

    for i in range(15):
        for w in W:
            w.perform( arcs, g )
        for w in W:
            w.perform( vertices, g )
        for w in W:
            w.perform( neighborhood, g )


    for w in W:
        w.perform( terminate, w )

    
    while len([1 for w in W if not w.is_dead()]):
        time.sleep( 1 )

    g.ClearGraphReadonly()

    g.DebugCheckAllocators()
    g.Truncate()
    g.DebugCheckAllocators()

    g.Erase()




###############################################################################
# Run
#
###############################################################################
def Run( name ):
    """
    """
    RunTests( [__name__] )
    PerformCleanup()
