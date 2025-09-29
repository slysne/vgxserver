###############################################################################
# 
# VGX Server
# Distributed engine for plugin-based graph and vector search
# 
# Module:  pyvgx
# File:    OpenVertex.py
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
from . import _vertex_test_support as Support
from pyvgx import *
import pyvgx

graph = None




###############################################################################
# TEST_OpenVertex
#
###############################################################################
def TEST_OpenVertex():
    """
    pyvgx.Graph.OpenVertex()
    test_level=3101
    """
    # Reset
    graph.Truncate()

    # Implicitly create a typeless vertex when write mode specified
    V = graph.OpenVertex( "V2", mode="w" )
    Support._VerifyVertex( graph, V, "V2" )
    graph.CloseVertex( V )
    del V

    # Fail to implicitly create a vertex in readonly, append mode
    for args in [ {'id':'V3'}, {'id':'V3', 'mode':'a'}, {'id':'V3', 'mode':'r'} ]:
        try:
            graph.OpenVertex( **args )
            Expect( False,    "Should not open non-existing vertex with args '%s'" % args )
        except KeyError as ex:
            msg = ex.args[0] # do it this way since str(ex) would include literal quotes in the string
            Expect( msg.startswith( "Object does not exist" ), "Unexpected exception message: %s" % msg )
        except Exception as ex:
            print(ex)
            raise
            Expect( False,    "Incorrect exception" )

    # Open existing vertex in write mode
    graph.CreateVertex( "A" )
    A = graph.OpenVertex( "A" )
    Support._VerifyVertex( graph, A, "A" )
    graph.CloseVertex( A )
    del A
    A = graph.OpenVertex( "A", mode="w" )
    Support._VerifyVertex( graph, A, "A" )
    graph.CloseVertex( A )
    del A

    # Open existing vertex in read mode
    A = graph.OpenVertex( "A", mode="r" )
    Expect( A.Writable() is False,      "Vertex should not be writable" )
    Expect( A.Readable() is True,       "Vertex should be readable" )
    Expect( A.Readonly() is True,       "Vertex should be readonly" )
    graph.CloseVertex( A )
    del A

    # Open existing vertex in append mode
    A = graph.OpenVertex( "A", mode="a" )
    Expect( A.Writable() is True,       "Vertex should be writable" )
    Expect( A.Readable() is True,       "Vertex should be readable" )
    Expect( A.Readonly() is False,      "Vertex should not be readonly" )
    graph.CloseVertex( A )
    del A

    # Recursively open vertex
    for mode in ['r', 'w', 'a']:
        LA = []
        RECURSION_LIMIT = 0x70
        for n in range( 1, RECURSION_LIMIT+1 ):
            A = graph.OpenVertex( "A", mode=mode )
            Expect( A.Readable() is True,       "Vertex should be readable" )
            if mode == "r":
                Expect( A.Writable() is False,    "Vertex should not be writable" )
                Expect( A.Readonly() is True,     "Vertex should be readonly" )
            else:
                Expect( A.Writable() is True,     "Vertex should be writable" )
                Expect( A.Readonly() is False,    "Vertex should not be readonly" )
            LA.append( A ) # keep reference to let recursion continue
        try:
            graph.OpenVertex( "A", mode=mode )
            Expect( False, "Should not be able to go beyond recursion limit" )
        except pyvgx.AccessError as ex:
            Expect( str(ex).startswith( "Recursion limit reached" ), "Message should contain recursion limit reached" )
        except Exception as incorrect:
            Expect( False, "Incorrect exception: %s" % ( repr(incorrect) ) )

        # Recursively close
        for A in LA:
            Expect( graph.CloseVertex( A ) == True,   "Vertex should be closed" )
        del LA

    # Open writable, then readonly (but get writable)
    Aw = graph.OpenVertex( "A", mode="w" )
    Expect( Aw.Writable(),    "Vertex should be writable" )
    Ar = graph.OpenVertex( "A", mode="r" )
    Expect( Ar.Writable(),    "Vertex should be writable even when opened readonly in recursive mode" )
    graph.CloseVertex( Aw )
    graph.CloseVertex( Ar )
    del Aw
    del Ar

    # Open readonly, then fail to open writable
    Ar = graph.OpenVertex( "A", mode="r" )
    Expect( Ar.Readonly(),    "Vertex should be readonly" )
    try:
        graph.OpenVertex( "A", mode="w" )
        Expect( False,          "Should not be able to open vertex writable after holding readonly lock" )
    except pyvgx.AccessError as ex:
        Expect( str(ex).startswith( "Object is locked" ), "Message should contain vertex is locked" )
    except:
        Expect( False,          "Incorrect exception" )
    graph.CloseVertex( Ar )
    del Ar




###############################################################################
# TEST_OpenVertex_by_address
#
###############################################################################
def TEST_OpenVertex_by_address():
    """
    pyvgx.Graph.OpenVertex() by address
    test_level=3102
    """
    A = graph.OpenVertex( "A", mode="w" ) 
    address = A.address
    graph.CloseVertex( A )
    del A

    A = graph.OpenVertex( address )
    Expect( A.id == "A",                      "Should open 'A' by its address" )
    Expect( A.Writable() and A.Readable(),    "Vertex should be writable and readable" )
    graph.CloseVertex( A )

    A = graph.OpenVertex( address, mode="a" )
    Expect( A.id == "A",                      "Should open 'A' by its address" )
    Expect( A.Writable() and A.Readable(),    "Vertex should be writable and readable" )
    graph.CloseVertex( A )

    A = graph.OpenVertex( address, mode="r" )
    Expect( A.id == "A",                      "Should open 'A' by its address" )
    Expect( A.Readonly(),                     "Vertex should be readonly" )
    graph.CloseVertex( A )

    try:
        graph.OpenVertex( address, mode="w" )
        Expect( False,  "Cannot open vertex by address in 'w' mode" )
    except ValueError:
        pass
    except:
        Expect( False,          "Incorrect exception" )

    try:
        graph.OpenVertex( address + 1 )
        Expect( False,  "Cannot open vertex when bad memory address is supplied" )
    except KeyError:
        pass
    except:
        Expect( False,          "Incorrect exception" )

    try:
        graph.OpenVertex( 0 )
        Expect( False,  "Cannot open vertex when null is supplied" )
    except ValueError:
        pass
    except:
        Expect( False,          "Incorrect exception" )






###############################################################################
# Run
#
###############################################################################
def Run( name ):
    """
    Run the tests in this module
    """
    global graph
    graph = pyvgx.Graph( name )
    RunTests( [__name__] )
    graph.Close()
    del graph
