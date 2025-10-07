###############################################################################
# 
# VGX Server
# Distributed engine for plugin-based graph and vector search
# 
# Module:  pyvgx.test
# File:    pyvgx_system.py
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
import random
import time
import os

graph = None

SYSROOT = "pyvgx_op_test"

GRAPH1 = "local1"
GRAPH2 = "local2"




###############################################################################
# TEST_vxdurable_system
#
###############################################################################
def TEST_vxdurable_system():
    """
    Core vxdurable_system
    test_level=501
    """
    try:
        pyvgx.selftest( force=True, testroot="vgxtest", library="vgx", names=["vxdurable_system.c"] )
    except:
        Expect( False )




###############################################################################
# TEST_vxio_uri
#
###############################################################################
def TEST_vxio_uri():
    """
    Core vxio_uri
    test_level=501
    """
    try:
        pyvgx.selftest( force=True, testroot="vgxtest", library="vgx", names=["vxio_uri.c"] )
    except:
        Expect( False )




###############################################################################
# TEST_pyvgx_system_WritableVertices
#
###############################################################################
def TEST_pyvgx_system_WritableVertices():
    """
    Verify writable vertices counter
    test_level=1101
    """
    # Start empty
    n = system.WritableVertices()
    Expect( n == 0 )

    # Create graph1, still empty
    g1 = Graph( GRAPH1 )
    n = system.WritableVertices()
    Expect( n == 0 )

    # WL: A
    A = g1.NewVertex( "A" )
    n = system.WritableVertices()
    Expect( n == 1 )

    # WL: A, B
    B = g1.NewVertex( "B" )
    n = system.WritableVertices()
    Expect( n == 2 )

    # WL: B
    A.Close()
    n = system.WritableVertices()
    Expect( n == 1 )

    # WL: B
    # RO: A
    A = g1.OpenVertex( "A", "r" )
    n = system.WritableVertices()
    Expect( n == 1 )

    # RO: A
    B.Close()
    n = system.WritableVertices()
    Expect( n == 0 )

    # No locked
    A.Close()
    n = system.WritableVertices()
    Expect( n == 0 )

    # Create graph2, still empty
    g2 = Graph( GRAPH2 )
    n = system.WritableVertices()
    Expect( n == 0 )

    # WL: A1, B1
    A1 = g1.NewVertex( "A1" )
    B1 = g1.NewVertex( "B1" )
    n = system.WritableVertices()
    Expect( n == 2 )

    # WL: A1, B1
    # WL: A2, B2
    A2 = g2.NewVertex( "A2" )
    B2 = g2.NewVertex( "B2" )
    n = system.WritableVertices()
    Expect( n == 4 )

    # WL: A2, B2
    g1.CloseAll()
    n = system.WritableVertices()
    Expect( n == 2 )

    # No locked
    g2.CloseAll()
    n = system.WritableVertices()
    Expect( n == 0 )

    # Clean up
    g1.Truncate()
    g2.Truncate()
    g1.Erase()
    g2.Erase()
    del g1
    del g2






###############################################################################
# Run
#
###############################################################################
def Run( name ):
    """
    """
    U = pyvgx.op.GetDefaultURIs()
    pyvgx.system.Unload()
    pyvgx.system.Initialize( SYSROOT, euclidean=False )
    RunTests( [__name__] )
    pyvgx.op.SetDefaultURIs( U )
