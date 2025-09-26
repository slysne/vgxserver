###############################################################################
# 
# VGX Server
# Distributed engine for plugin-based graph and vector search
# 
# Module:  pyvgx
# File:    CloseVertices.py
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
from . import _vertex_test_support as Support
from pyvgx import *
import pyvgx

graph = None


def expect_Exc( E, method, *args, **kwds ):
    try:
        method( *args, **kwds )
        Expect( False,              "Expected exception {}".format( E ) )
    except E:
        pass



def TEST_CloseVertices():
    """
    pyvgx.Graph.CloseVertices()
    test_level=3101
    """
    # Reset
    graph.Truncate()

    # Populate graph
    graph.CreateVertex( "A" )
    graph.CreateVertex( "B" )
    graph.CreateVertex( "C" )
    graph.CreateVertex( "D" )

    # Acquire set
    S = graph.OpenVertices( ["A", "B", "C", "D"] )

    # List item must be vertex object
    expect_Exc( TypeError, graph.CloseVertices, ["A","B","C","D"] )

    # Close Set
    graph.CloseVertices( S )

    # Acquire set
    S = graph.OpenVertices( ["A", "B", "C", "D"] )

    # Close part of set
    n = graph.CloseVertices( S[:2] )
    Expect( n == 2 )

    # Verify
    Expect( S[0].Readable() is False )
    Expect( S[1].Readable() is False )
    Expect( S[2].Readable() is True )
    Expect( S[3].Readable() is True )

    # Doing again has no effect
    n = graph.CloseVertices( S[:2] )
    Expect( n == 0 )

    # Closing empty set has no effect
    n = graph.CloseVertices( [] )
    Expect( n == 0 )

    # Close one
    n = graph.CloseVertices( S[3:] )
    Expect( n == 1 )

    # Verify
    Expect( S[0].Readable() is False )
    Expect( S[1].Readable() is False )
    Expect( S[2].Readable() is True )
    Expect( S[3].Readable() is False )

    # Close last one
    S[2].Close()
    Expect( S[2].Readable() is False )



def Run( name ):
    """
    Run the tests in this module
    """
    global graph
    graph = pyvgx.Graph( name )
    RunTests( [__name__] )
    graph.Close()
    del graph
