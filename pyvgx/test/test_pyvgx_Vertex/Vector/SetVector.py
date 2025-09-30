###############################################################################
# 
# VGX Server
# Distributed engine for plugin-based graph and vector search
# 
# Module:  pyvgxtest
# File:    SetVector.py
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

graph = None




###############################################################################
# TEST_SetVector
#
###############################################################################
def TEST_SetVector():
    """
    pyvgx.Vertex.SetVector()
    Basic
    test_level=3101
    """

    if "vertex" in graph:
        graph.DeleteVertex( "vertex" )

    V = graph.NewVertex( "vertex" )

    V.SetVector( [] )
    Expect( V.GetVector().external == [],                               "empty" )
    # TODO: fix such that V.GetVector() == [] also

    V.SetVector( [("a",1)] )
    Expect( V.GetVector().external == [("a",1.0)],                      "one element" )

    V.SetVector( [("a",1),("b",0.5)] )
    Expect( V.GetVector().external == [("a",1.0),("b",0.5)],            "two elements" )

    V.SetVector( [("a",1),("b",0.5),("c",0.25)] )
    Expect( V.GetVector().external == [("a",1.0),("b",0.5),("c",0.25)], "three elements" )

    V.SetVector( [("a",1),("b",0.5)] )
    Expect( V.GetVector().external == [("a",1.0),("b",0.5)],            "two elements" )

    V.SetVector( [("a",1)] )
    Expect( V.GetVector().external == [("a",1.0)],                      "one element" )

    V.SetVector( [] )
    Expect( V.GetVector().external == [],                               "empty" )




###############################################################################
# TEST_SetVector_limits
#
###############################################################################
def TEST_SetVector_limits():
    """
    pyvgx.Vertex.SetVector()
    Limits
    test_level=3101
    """

    if "vertex" in graph:
        graph.DeleteVertex( "vertex" )

    V = graph.NewVertex( "vertex" )

    MAX = graph.sim.max_vector_size

    for vlen in range( 2, MAX+10 ):
        c = ord( "A" )
        for termsz in range( 1, 28 ):
            vec = []
            for n in range( vlen ):
                if n == 0:
                    vec.append( ("<",2) ) # start marker
                elif n == vlen-1:
                    vec.append( (">",2) ) # end marker
                else:
                    term = chr(c) * termsz
                    weight = 2 * float(vlen-n) / vlen
                    vec.append( (term,weight) )
            V.SetVector( vec )

            if vlen <= MAX:
                Expect( V.GetVector().length == vlen,       "correct length" )
            else:
                vlen = MAX
                Expect( V.GetVector().length == MAX,         "truncated" )

            if termsz <= 27:
                for n in range( 1, vlen-1 ):
                    v_vec = V.GetVector().external[n][0]
                    vec_n = vec[n][0]
                    Expect( V.GetVector().external[n][0] == vec[n][0],      "expected '%s', got '%s' / vector=%s" % (vec_n, v_vec, V.GetVector()) )
            else:
                for n in range( 1, vlen-1 ):
                    Expect( V.GetVector().external[n][0] == vec[n][0][:27], "correct term prefix" )












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
