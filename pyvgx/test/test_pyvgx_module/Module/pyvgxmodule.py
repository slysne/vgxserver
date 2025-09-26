###############################################################################
# 
# VGX Server
# Distributed engine for plugin-based graph and vector search
# 
# Module:  pyvgx
# File:    pyvgxmodule.py
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
from pyvgx import *
import pyvgx
import random
import time
import hashlib


graph = None

SYSROOT = "pyvgx_module_test"



def TEST_vxoballoc_cstring():
    """
    Core vxoballoc_cstring
    test_level=501
    """
    try:
        pyvgx.selftest( force=True, testroot="vgxtest", library="vgx", names=["vxoballoc_cstring.c"] )
    except:
        Expect( False )



def TEST_vxoballoc_vector():
    """
    Core vxoballoc_vector
    test_level=501
    """
    try:
        pyvgx.selftest( force=True, testroot="vgxtest", library="vgx", names=["vxoballoc_vector.c"] )
    except:
        Expect( False )



def TEST_vxoballoc_vertex():
    """
    Core vxoballoc_vertex
    test_level=501
    """
    try:
        pyvgx.selftest( force=True, testroot="vgxtest", library="vgx", names=["vxoballoc_vertex.c"] )
    except:
        Expect( False )



def TEST_vxoballoc_graph():
    """
    Core vxoballoc_graph
    test_level=501
    """
    try:
        pyvgx.selftest( force=True, testroot="vgxtest", library="vgx", names=["vxoballoc_graph.c"] )
    except:
        Expect( False )



def TEST_vxdurable_registry():
    """
    Core vxdurable_registry
    t_nominal=152
    test_level=502
    """
    try:
        pyvgx.selftest( force=True, testroot="vgxtest", library="vgx", names=["vxdurable_registry.c"] )
    except:
        Expect( False )



def TEST_pyvgxmodule_initialize():
    """
    pyvgx.system.Initialize()
    test_level=1101
    """
    Expect( pyvgx.system.IsInitialized() is False,           "pyvgx should be uninitialized"  )
    pyvgx.system.Initialize( SYSROOT )
    try:
        pyvgx.system.Initialize( SYSROOT )
        Expect( False,                              "should not be able to initialize twice" )
    except:
        Expect( True,                               "" )
    try:
        pyvgx.system.Initialize( "other" )
        Expect( False,                              "should not be able to initialize twice" )
    except:
        Expect( True,                               "" )



def TEST_pyvgxmodule_initialized():
    """
    pyvgx.system.IsInitialized()
    Check that pyvgx is now initialized
    test_level=1101
    """
    if not pyvgx.system.IsInitialized():
        pyvgx.system.Initialize( SYSROOT )

    Expect( pyvgx.system.IsInitialized() is True,            "pyvgx should be initialized" )



def TEST_pyvgxmodule_system_root():
    """
    pyvgx.system.Root()
    Check that system root is returned when initialized
    test_level=1101
    """
    if not pyvgx.system.IsInitialized():
        pyvgx.system.Initialize( SYSROOT )

    # should be initialized
    Expect( pyvgx.system.Root() == SYSROOT,         "pyvgx should be initialized" )
    # unload and verify
    pyvgx.system.Unload()
    Expect( pyvgx.system.Root() is None,            "pyvgx should be uninitialized" )
    # load and verify
    pyvgx.system.Initialize( SYSROOT )
    Expect( pyvgx.system.Root() == SYSROOT,         "pyvgx should be initialized" )



def TEST_pyvgxmodule_unload():
    """
    pyvgx.system.Unload()
    Unload pyvgx and check
    test_level=1101
    """
    if not pyvgx.system.IsInitialized():
        pyvgx.system.Initialize( SYSROOT )

    pyvgx.system.Unload()
    Expect( pyvgx.system.IsInitialized() is False,           "pyvgx should be uninitialized"  )



def TEST_pyvgxmodule_init_unload_many():
    """
    Repeated Initialize() Unload() loop
    test_level=1101
    """
    if pyvgx.system.IsInitialized():
        pyvgx.system.Unload()

    Expect( pyvgx.system.IsInitialized() is False,           "pyvgx should be uninitialized"  )

    for n in range(10):
        pyvgx.system.Initialize( SYSROOT )
        pyvgx.system.Unload()

    Expect( pyvgx.system.IsInitialized() is False,           "pyvgx should be uninitialized"  )



def TEST_pyvgxmodule_registry():
    """
    pyvgx.system.Registry()
    test_level=1101
    """
    if pyvgx.system.IsInitialized():
        pyvgx.system.Unload()

    # system.Registry() should raise exception when uninitialized
    try:
        pyvgx.system.Registry()
        Expect( False,                              "system.Registry() should raise an exception when uninitialized" )
    except:
        Expect( True,                               "" )
    # initialize and create two graphs with some vertices
    pyvgx.system.Initialize( SYSROOT )
    reg = pyvgx.system.Registry()
    Expect( type(reg) is dict,                      "system.Registry() should return a dict" )
    g1 = pyvgx.Graph( "g1" )
    for n in range( 10 ):
        g1.CreateVertex( "g1_%d" % n )
    g2 = pyvgx.Graph( "g2" )
    for n in range( 10 ):
        g2.CreateVertex( "g2_%d" % n )
    g3 = pyvgx.Graph( "g3" )
    for n in range( 10 ):
        g3.CreateVertex( "g3_%d" % n )
    # check registry
    reg = pyvgx.system.Registry()
    Expect( "g1" in reg and "g2" in reg and 'g3' in reg,    "graphs 'g1', 'g2' and 'g3' should be in registry" )



def TEST_pyvgxmodule_delete_graph():
    """
    pyvgx.system.DeleteGraph()
    Remove graphs from registry
    test_level=1101
    """
    if not pyvgx.system.IsInitialized():
        pyvgx.system.Initialize( SYSROOT )

    # remove graph 'g1' from registry without saving it
    g1 = pyvgx.Graph( "g1" )
    g1.Close()
    del g1
    pyvgx.system.DeleteGraph( "g1", timeout=2000 )
    reg = pyvgx.system.Registry()
    Expect( "g1" not in reg and "g2" in reg and 'g3' in reg,    "graph 'g1' should be removed from registry, 'g2' and 'g3' should still be in registry" )
    # remove graph 'g2' from registry after saving it
    g2 = Graph( "g2" )
    g2.Save( 10000 )
    g2.Close()
    del g2
    pyvgx.system.DeleteGraph( "g2", timeout=2000 )
    reg = pyvgx.system.Registry()
    Expect( "g1" not in reg and "g2" not in reg and 'g3' in reg,    "graph 'g1' and 'g2' should be removed from registry, 'g3' should still be in registry" )
    # unload and save graph 'g3'
    system.Unload( persist=True )
    # initialize again and check
    pyvgx.system.Initialize( SYSROOT )
    reg = pyvgx.system.Registry()
    Expect( "g1" not in reg and "g2" not in reg and 'g3' in reg,    "graph 'g1' and 'g2' should not be registry, graph 'g3' should be in registry" )
    # all graphs should still exist, g1 without data because it was unsaved, g2 and g3 with data
    g1 = pyvgx.Graph( "g1" )
    for n in range( 10 ):
        Expect( "g1_%d" % n not in g1,              "vertices in graph 'g1' should not exist" )
    g2 = pyvgx.Graph( "g2" )
    for n in range( 10 ):
        Expect( "g2_%d" % n in g2,                  "vertices in graph 'g2' should be restored" )
    g3 = pyvgx.Graph( "g3" )
    for n in range( 10 ):
        Expect( "g3_%d" % n in g3,                  "vertices in graph 'g3' should be restored" )

    

def TEST_pyvgxmodule_version():
    """
    pyvgx.version()
    Version
    test_level=1101
    """
    Expect( type( pyvgx.version() ) is str,         "version() should return string" )
    Expect( type( pyvgx.version(1) ) is str,         "version(1) should return string" )
    Expect( type( pyvgx.version(2) ) is str,         "version(2) should return string" )
    Expect( type( pyvgx.version(3) ) is str,         "version(3) should return string" )



def TEST_pyvgxmodule_ihash64():
    """
    pyvgx.ihash64()
    test_level=1101
    """
    N = 100000
    D = {}
    # populate a set of integer hashes
    for n in range( N ):
        h = pyvgx.ihash64( n )
        Expect( type( h ) is int,                  "ihash64() should return a long" )
        D[n] = h
    Expect( len(D) == N,                            "ihash64() should generate different values for each unique input value" )
    # check again
    for n in range( N ):
        h = pyvgx.ihash64( n )
        Expect( D[n] == h,                          "ihash64() should be deterministic" )
        


def TEST_pyvgxmodule_strhash64():
    """
    pyvgx.strhash64()
    test_level=1101
    """
    N = 100000
    D = {}
    # populate a set of string hashes
    for n in range( N ):
        s = ">>> %d <<<" % n
        h = pyvgx.strhash64( s )
        Expect( type( h ) is int,                  "strhash64() should return a long" )
        D[s] = h
    Expect( len(D) == N,                            "strhash64() should generate different values for each unique input value" )
    # check again
    for n in range( N ):
        s = ">>> %d <<<" % n
        h = pyvgx.strhash64( s )
        Expect( D[s] == h,                          "strhash64() should be deterministic" )



def TEST_pyvgxmodule_strhash128():
    """
    pyvgx.strhash128()
    test_level=1101
    """
    N = 100000
    D = {}
    # populate a set of string hashes
    for n in range( N ):
        s = ">>> %d <<<" % n
        h = pyvgx.strhash128( s )
        Expect( type( h ) is str,                   "strhash128() should return a string" )
        Expect( len( h ) == 32,                     "strhash128() should return a 32 character string" )
        D[s] = h
    Expect( len(D) == N,                            "strhash128() should generate different values for each unique input value" )
    # check again
    for n in range( N ):
        s = ">>> %d <<<" % n
        h = pyvgx.strhash128( s )
        Expect( D[s] == h,                          "strhash128() should be deterministic" )



def TEST_pyvgxmodule_tokenize():
    """
    pyvgx.tokenize()
    test_level=1101
    """
    # Simple test
    simple = "This is a string."
    simple_check = "This#is#a#string#."
    T = pyvgx.tokenize( simple )
    Expect( type( T ) is list,                      "tokens should be a list" )
    Expect( len( T ) == 5,                          "token list should have 5 tokens" )
    Expect( "#".join(T) == simple_check,            "tokens should be correct" )

    # More complex
    more = "def func( arg=10 ): return (arg**2+5) / 17.0"
    more_check = "def#func#(#arg#=#10#)#:#return#(#arg#*#*#2#+#5#)#/#17#.#0"
    T = pyvgx.tokenize( more )
    Expect( type( T ) is list,                      "tokens should be a list" )
    Expect( len( T ) == 21,                         "token list should have 21 tokens" )
    Expect( "#".join(T) == more_check,              "tokens should be correct" )
    


def TEST_pyvgxmodule_timestamp():
    """
    pyvgx.timestamp()
    test_level=1101
    """
    t0 = pyvgx.timestamp()
    time.sleep( 2.0 )
    t1 = pyvgx.timestamp()
    dt = t1 - t0
    Expect( dt > 1.8 and dt < 2.2,                  "timestamps should reflect real clock" )



def TEST_pyvgxmodule_SetOutputStream():
    """
    pyvgx.SetOutputStream()
    test_level=1101
    """
    pyvgx.SetOutputStream( "pyvgx_module_log.txt" )
    pyvgx.LogInfo( "This is some info ##1## " )
    pyvgx.LogWarning( "This is a warning ##2##" )
    pyvgx.LogError( "This is an error ##3##" )
    pyvgx.SetOutputStream( None )
    pyvgx.LogInfo( "This is more info ##4##" )
    f = open( "pyvgx_module_log.txt" )
    data = f.read()
    f.close()
    Expect( "##1##" in data and "##2##" in data and "##3##" in data and "##4##" not in data, "logged data should not include lines after output stream was detached" )
   


def TEST_pyvgxmodule_AutoArcTimestamps():
    """
    pyvgx.AutoArcTimestamps()
    test_level=1101
    """
    if not pyvgx.system.IsInitialized():
        pyvgx.system.Initialize( SYSROOT )

    g = pyvgx.Graph( "autoarcts" )
    g.Truncate()

    g.CreateVertex( "A" )
    g.CreateVertex( "B" )
    g.CreateVertex( "C" )
    g.CreateVertex( "D" )
    g.CreateVertex( "E" )
    g.CreateVertex( "F" )

    Expect( g.order == 6,   "Six vertices" )
    Expect( g.size == 0,    "No arcs" )

    pyvgx.AutoArcTimestamps( False )

    g.Connect( "A", "to", "B" )

    Expect(     g["A"].outdegree == 1,                         "A has one outarc" )
    Expect(     g.Adjacent( "A", ("to",D_OUT,M_STAT), "B" ),   "A is connected to B" )
    Expect( not g.Adjacent( "A", ("to",D_OUT,M_TMC), "B" ),    "No TMC arc" )
    Expect( not g.Adjacent( "A", ("to",D_OUT,M_TMM), "B" ),    "No TMM arc" )

    pyvgx.AutoArcTimestamps( True )

    t0 = g.ts
    g.Connect( "C", ("to",M_INT,1), "D" )

    Expect(     g["C"].outdegree == 3,                         "C has 3 outarcs" )
    Expect(     g.Adjacent( "C", ("to",D_OUT,M_INT), "D" ),    "C is connected to D" )
    Expect(     g.Adjacent( "C", ("to",D_OUT,M_TMC), "D" ),    "C has TMC arc to D" )
    Expect(     g.Adjacent( "C", ("to",D_OUT,M_TMM), "D" ),    "C has TMM arc to D" )

    Expect(     g.Adjacent( "C", ("to",D_OUT,M_TMC,V_RANGE,(t0,t0+1)), "D" ),    "TMC is now" )
    Expect(     g.Adjacent( "C", ("to",D_OUT,M_TMM,V_RANGE,(t0,t0+1)), "D" ),    "TMM is now" )

    time.sleep( 3 )

    t1 = g.ts
    g.Connect( "C", ("to",M_INT,2), "D" )
    Expect(     g.Adjacent( "C", ("to",D_OUT,M_TMC,V_RANGE,(t0,t0+1)), "D" ),    "TMC is original creation time" )
    Expect(     g.Adjacent( "C", ("to",D_OUT,M_TMM,V_RANGE,(t1,t1+1)), "D" ),    "TMC is now" )

    pyvgx.AutoArcTimestamps( False )

    g.Connect( "E", ("to",M_INT,1), "F" )

    Expect(     g["E"].outdegree == 1,                         "E has one outarc" )
    Expect(     g.Adjacent( "E", ("to",D_OUT,M_INT), "F" ),    "E is connected to F" )
    Expect( not g.Adjacent( "E", ("to",D_OUT,M_TMC), "F" ),    "No TMC arc" )
    Expect( not g.Adjacent( "E", ("to",D_OUT,M_TMM), "F" ),    "No TMM arc" )

    g.Erase()

 

def Run( name ):
    RunTests( [__name__] )
