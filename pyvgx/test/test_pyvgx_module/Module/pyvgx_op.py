###############################################################################
# 
# VGX Server
# Distributed engine for plugin-based graph and vector search
# 
# Module:  pyvgx.test
# File:    pyvgx_op.py
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
import random
import time
import os
import itertools

graph = None

SYSROOT = "pyvgx_op_test"

GRAPH = "local"




###############################################################################
# TEST_vxdurable_operation
#
###############################################################################
def TEST_vxdurable_operation():
    """
    Core vxdurable_operation
    test_level=501
    """
    try:
        pyvgx.selftest( force=True, testroot="vgxtest", library="vgx", names=["vxdurable_operation.c"] )
    except:
        Expect( False )




###############################################################################
# TEST_vxdurable_operation_buffers
#
###############################################################################
def TEST_vxdurable_operation_buffers():
    """
    Core vxdurable_operation_buffers
    test_level=501
    """
    try:
        pyvgx.selftest( force=True, testroot="vgxtest", library="vgx", names=["vxdurable_operation_buffers.c"] )
    except:
        Expect( False )




###############################################################################
# TEST_vxdurable_operation_transaction
#
###############################################################################
def TEST_vxdurable_operation_transaction():
    """
    Core vxdurable_operation_transaction
    test_level=501
    """
    try:
        pyvgx.selftest( force=True, testroot="vgxtest", library="vgx", names=["vxdurable_operation_transaction.c"] )
    except:
        Expect( False )




###############################################################################
# TEST_vxdurable_operation_capture
#
###############################################################################
def TEST_vxdurable_operation_capture():
    """
    Core vxdurable_operation_capture
    test_level=501
    """
    try:
        pyvgx.selftest( force=True, testroot="vgxtest", library="vgx", names=["vxdurable_operation_capture.c"] )
    except:
        Expect( False )




###############################################################################
# TEST_vxdurable_operation_emitter
#
###############################################################################
def TEST_vxdurable_operation_emitter():
    """
    Core vxdurable_operation_emitter
    test_level=501
    """
    try:
        pyvgx.selftest( force=True, testroot="vgxtest", library="vgx", names=["vxdurable_operation_emitter.c"] )
    except:
        Expect( False )




###############################################################################
# TEST_vxdurable_operation_produce_op
#
###############################################################################
def TEST_vxdurable_operation_produce_op():
    """
    Core vxdurable_operation_produce_op
    test_level=501
    """
    try:
        pyvgx.selftest( force=True, testroot="vgxtest", library="vgx", names=["vxdurable_operation_produce_op.c"] )
    except:
        Expect( False )




###############################################################################
# TEST_vxdurable_operation_parser
#
###############################################################################
def TEST_vxdurable_operation_parser():
    """
    Core vxdurable_operation_parser
    test_level=501
    """
    try:
        pyvgx.selftest( force=True, testroot="vgxtest", library="vgx", names=["vxdurable_operation_parser.c"] )
    except:
        Expect( False )




###############################################################################
# TEST_vxdurable_operation_consumer_service
#
###############################################################################
def TEST_vxdurable_operation_consumer_service():
    """
    Core vxdurable_operation_consumer_service
    test_level=501
    """
    try:
        pyvgx.selftest( force=True, testroot="vgxtest", library="vgx", names=["vxdurable_operation_consumer_service.c"] )
    except:
        Expect( False )




###############################################################################
# TEST_pyvgx_op_Attach_Detach
#
###############################################################################
def TEST_pyvgx_op_Attach_Detach():
    """
    pyvgx.op.Attach() and pyvgx.op.Detach()
    File output
    test_level=1101
    """
    opcodes1 = "./%s/opcodes1" % (SYSROOT)
    opcodes2 = "./%s/opcodes2" % (SYSROOT)
    Expect( not os.path.exists( opcodes1 ),              "file %s should not exist" % opcodes1 )
    Expect( not os.path.exists( opcodes2 ),              "file %s should not exist" % opcodes2 )

    # Attach single
    op.Attach( "file:///%s" % opcodes1 )
    Expect( os.path.exists( opcodes1 ),                  "file %s should exist" % opcodes1 )

    greeting = "Hello, world!"
    op.ProduceComment( greeting )

    # Detach
    op.Detach()

    # Check
    f = open( opcodes1, "r" )
    Expect( greeting in f.read(),                       "file %s should contain string '%s'" % (opcodes1, greeting) )
    f.close()

    # Attach both
    op.Attach( ["file:///%s" % opcodes1, "file:///%s" % opcodes2] )
    Expect( os.path.exists( opcodes1 ),                  "file %s should exist" % opcodes1 )
    Expect( os.path.exists( opcodes2 ),                  "file %s should exist" % opcodes2 )

    greeting = "Hello, again!"
    op.ProduceComment( greeting )

    # Detach
    op.Detach()

    # Check both
    f1 = open( opcodes1, "r" )
    f2 = open( opcodes2, "r" )
    Expect( greeting in f1.read(),                       "file %s should contain string '%s'" % (opcodes1, greeting) )
    Expect( greeting in f2.read(),                       "file %s should contain string '%s'" % (opcodes2, greeting) )
    f1.close()
    f2.close()




###############################################################################
# TEST_pyvgx_op_Attach_default
#
###############################################################################
def TEST_pyvgx_op_Attach_default():
    """
    pyvgx.op.Attach()
    Default URI, file output
    test_level=1101
    """
    opcodes = "./%s/opcodes_default" % (SYSROOT)
    URI = "file:///%s" % opcodes
    op.SetDefaultURIs( URI )
    Expect( not os.path.exists( opcodes ),               "file %s should not exist" % opcodes )

    # Attach default
    op.Attach()
    Expect( os.path.exists( opcodes ),                   "file %s should exist" % opcodes )

    # Detach
    op.Detach()




###############################################################################
# TEST_pyvgx_op_Suspend_Resume
#
###############################################################################
def TEST_pyvgx_op_Suspend_Resume():
    """
    pyvgx.op.Suspend() and pyvgx.op.Resume()
    test_level=1101
    """
    # First attach file output
    opcodes = "./%s/opcodes_suspend_resume" % (SYSROOT)
    op.Attach( "file:///%s" % opcodes )

    # Create a graph
    g = Graph( GRAPH )
    g.Truncate()

    N = 100000
    BINS = 100
    NX = 1000

    # Create some data
    for n in range( N ):
        V = g.NewVertex( "node_%d" % n )
        V['x'] = n
        V['y'] = "This is bin %d" % (n % BINS)
        V.Close()

    # Suspend
    op.Suspend()

    # Add more, should not appear in output
    for n in range( N, N+NX ):
        V = g.NewVertex( "node_%d" % n )
        V['x'] = n
        V['y'] = "This is bin %d" % (n % BINS)
        V.Close()

    # Check data
    f = open( opcodes, "r" )
    opdata = f.read()
    f.close()

    # Count expected operators
    n_vxn = 0
    n_vps = 0
    n_kea = 0
    n_sea = 0
    
    opcode_vxn = " %08X " % op.OP_vxn   # New vertex
    opcode_vps = " %08X " % op.OP_vps   # Set property
    opcode_kea = " %08X " % op.OP_kea   # New key enum
    opcode_sea = " %08X " % op.OP_sea   # New string enum

    # Scan
    for line in opdata.split( "\n" ):
        if opcode_vxn in line:
            n_vxn += 1
        elif opcode_vps in line:
            n_vps += 1
        elif opcode_kea in line:
            n_kea += 1
        elif opcode_sea in line:
            n_sea += 1

    Expect( n_vxn == N,                             "got %d vxn operators, expected %d" % (n_vxn, N) )
    Expect( n_vps == N*2,                           "got %d vps operators, expected %d" % (n_vps, N*2) )
    Expect( n_kea == 2,                             "got %d kea operators, expected %d" % (n_kea, 2) )
    Expect( n_sea == BINS,                          "got %d sea operators, expected %d" % (n_sea, BINS) )

    # Resume to flush out data
    op.Resume()

    # Suspend again and check
    op.Suspend()

    # Check data
    f = open( opcodes, "r" )
    opdata = f.read()
    f.close()

    # Reset counts and scan
    n_vxn = 0
    n_vps = 0
    n_kea = 0
    n_sea = 0

    for line in opdata.split( "\n" ):
        if opcode_vxn in line:
            n_vxn += 1
        elif opcode_vps in line:
            n_vps += 1
        elif opcode_kea in line:
            n_kea += 1
        elif opcode_sea in line:
            n_sea += 1

    Expect( n_vxn == N+NX,                          "got %d vxn operators, expected %d" % (n_vxn, N+NX) )
    Expect( n_vps == (N+NX)*2,                      "got %d vps operators, expected %d" % (n_vps, (N+NX)*2) )
    Expect( n_kea == 2,                             "got %d kea operators, expected %d" % (n_kea, 2) )
    Expect( n_sea == BINS,                          "got %d sea operators, expected %d" % (n_sea, BINS) )

    op.Detach()
    
    g.Erase()




###############################################################################
# TEST_pyvgx_op_Fence
#
###############################################################################
def TEST_pyvgx_op_Fence():
    """
    pyvgx.op.Fence()
    t_nominal=17
    test_level=1101
    """
    # First attach file output
    opcodes = "./%s/opcodes_fence" % (SYSROOT)
    op.Attach( "file:///%s" % opcodes )

    # Create a graph
    g = Graph( GRAPH )
    g.Truncate()


    opcode_vxn = " %08X " % op.OP_vxn   # New vertex
    opcode_vps = " %08X " % op.OP_vps   # Set property
    opcode_kea = " %08X " % op.OP_kea   # New key enum
    opcode_sea = " %08X " % op.OP_sea   # New string enum


    BINS = 50   # number of unique string property values
    NV = 1
    vx = 0
    # Create data in larger and larger chunks
    while vx < 1000000:
        LogInfo( "Creating and verifying output batch for %d vertices" % NV )
        for n in range( NV ):
            V = g.NewVertex( "node_%d" % vx )
            V['x'] = vx
            V['y'] = "This is property string bin %d" % (vx % BINS)
            V.Close()
            vx += 1
        NV *= 2
            
        # Fence
        op.Fence()
        f = open( opcodes, "r" )
        opdata = f.read()
        f.close()

        # Count expected operators
        n_vxn = 0
        n_vps = 0
        n_kea = 0
        n_sea = 0
        for line in opdata.split( "\n" ):
            if opcode_vxn in line:
                n_vxn += 1
            elif opcode_vps in line:
                n_vps += 1
            elif opcode_kea in line:
                n_kea += 1
            elif opcode_sea in line:
                n_sea += 1

        Expect( n_vxn == vx,                            "got %d vxn operators, expected %d" % (n_vxn, vx) )
        Expect( n_vps == vx*2,                          "got %d vps operators, expected %d" % (n_vps, vx*2) )
        Expect( n_kea == 2,                             "got %d kea operators, expected %d" % (n_kea, 2) )
        if vx >= BINS:
            Expect( n_sea == BINS,                      "got %d sea operators, expected %d" % (n_sea, BINS) )
        else:
            Expect( n_sea == vx,                        "got %d sea operators, expected %d" % (n_sea, vx) )

    op.Detach()

    g.Erase()




###############################################################################
# TEST_pyvgx_op_Throttle
#
###############################################################################
def TEST_pyvgx_op_Throttle():
    """
    pyvgx.op.Throttle()
    test_level=1101
    """
    BPS = 'bytes_per_second'
    CPS = 'opcodes_per_second'
    OPS = 'operations_per_second'
    TPS = 'transactions_per_second'

    keys = [ BPS, CPS, OPS, TPS ]
    min_rates = {
        "bytes"         : 65536,
        "opcodes"       : 256,
        "operations"    : 128,
        "transactions"  : 8
    }

    # Return defaults
    t = op.Throttle()
    Expect( type(t) is dict,            "Return value should be dict" )
    Expect( set(t) == set(keys),        "Throttle keys should be %s" % keys )
    for rate in t.values():
        Expect( rate is None,           "Default throttle rates all None" )

    bps = 100000.0
    cps = 10000.0
    ops = 1000.0
    tps = 100.0

    # Default unit = bytes
    t = op.Throttle( rate=bps )
    Expect( t[BPS] == bps )
    Expect( t[CPS] is None )
    Expect( t[OPS] is None )
    Expect( t[TPS] is None )

    # Add opcodes limit
    t = op.Throttle( rate=cps, unit="opcodes" )
    Expect( t[BPS] == bps )
    Expect( t[CPS] == cps )
    Expect( t[OPS] is None )
    Expect( t[TPS] is None )

    # Add operations limit
    t = op.Throttle( rate=ops, unit="operations" )
    Expect( t[BPS] == bps )
    Expect( t[CPS] == cps )
    Expect( t[OPS] == ops )
    Expect( t[TPS] is None )

    # Add transactions limit
    t = op.Throttle( rate=tps, unit="transactions" )
    Expect( t[BPS] == bps )
    Expect( t[CPS] == cps )
    Expect( t[OPS] == ops )
    Expect( t[TPS] == tps )

    # Remove bytes limit
    t = op.Throttle( None, "bytes" )
    Expect( t[BPS] is None )
    Expect( t[CPS] == cps )
    Expect( t[OPS] == ops )
    Expect( t[TPS] == tps )

    # Remove opcodes limit
    t = op.Throttle( None, "opcodes" )
    Expect( t[BPS] is None )
    Expect( t[CPS] is None )
    Expect( t[OPS] == ops )
    Expect( t[TPS] == tps )

    # Remove operations limit
    t = op.Throttle( None, "operations" )
    Expect( t[BPS] is None )
    Expect( t[CPS] is None )
    Expect( t[OPS] is None )
    Expect( t[TPS] == tps )

    # Remove transactions limit
    t = op.Throttle( None, "transactions" )
    Expect( t[BPS] is None )
    Expect( t[CPS] is None )
    Expect( t[OPS] is None )
    Expect( t[TPS] is None )

    # Set all
    op.Throttle( rate=bps, unit="bytes" )
    op.Throttle( rate=cps, unit="opcodes" )
    op.Throttle( rate=ops, unit="operations" )
    t = op.Throttle( rate=tps, unit="transactions" )
    Expect( t[BPS] == bps )
    Expect( t[CPS] == cps )
    Expect( t[OPS] == ops )
    Expect( t[TPS] == tps )

    # Remove all
    t = op.Throttle( None )
    Expect( t[BPS] is None )
    Expect( t[CPS] is None )
    Expect( t[OPS] is None )
    Expect( t[TPS] is None )

    # Wrong key
    try:
        op.Throttle( rate=1000000, unit="xyz" )
        Expect( False )
    except ValueError:
        pass
    except Exception as wrong:
        Expect( False, "wrong exception: %s" % wrong )

    # Invalid rates
    for unit, min_rate in min_rates.items():
        op.Throttle( rate=min_rate, unit=unit )
        try:
            too_low = min_rate-1
            op.Throttle( rate=too_low, unit=unit )
            Expect( False,      "Too low rate=%d for unit=%s" % (too_low, unit) )
        except ValueError:
            pass
        except Exception as wrong:
            Expect( False, "wrong exception: %s" % wrong )

    # rate=0.0 is the same as rate=None
    op.Throttle( None )
    t = op.Throttle( bps, "bytes" )
    t = op.Throttle( cps, "opcodes" )
    t = op.Throttle( ops, "operations" )
    t = op.Throttle( tps, "transactions" )
    Expect( t[BPS] == bps )
    Expect( t[CPS] == cps )
    Expect( t[OPS] == ops )
    Expect( t[TPS] == tps )
    t = op.Throttle( rate=0, unit="bytes" )
    Expect( t[BPS] is None )
    Expect( t[CPS] == cps )
    Expect( t[OPS] == ops )
    Expect( t[TPS] == tps )
    t = op.Throttle( rate=0 )
    Expect( t[BPS] is None )
    Expect( t[CPS] is None )
    Expect( t[OPS] is None )
    Expect( t[TPS] is None )

    # Rate cannot be negative
    try:
        op.Throttle( -1 )
        Expect( False )
    except ValueError:
        pass
    except Exception as wrong:
        Expect( False, "wrong exception: %s" % wrong )
   



###############################################################################
# TEST_pyvgx_op_Consume_and_Throttle
#
###############################################################################
def TEST_pyvgx_op_Consume_and_Throttle():
    """
    pyvgx.op.Consume() and pyvgx.op.Throttle()
    t_nominal=113
    test_level=1101
    """
    # First attach file output
    opcodes = "./%s/opcodes_consume" % (SYSROOT)
    op.Attach( "file:///%s" % opcodes )

    # Create a graph
    g = Graph( GRAPH )
    g.Truncate()

    # Put a variety of data into graph
    NV = 100000
    NA = 8

    def get_term( v, t ):
        return ((v * 113) + (v * t)) % NV

    for v in range( NV ):
        init = "vertex_%d" % v
        tp = "odd" if v%2 else "even"
        V = g.NewVertex( init, type=tp, properties={ 'v':v, 'graph_order_now':g.order, 'graph_size_now':g.size, 'bytes':bytearray( range(v%256) ) } )
        TERM = []
        for a in range( NA ):
            t = get_term( v, a )
            term = "vertex_%d" % t
            tp = "odd" if t%2 else "even"
            T = g.NewVertex( term, type=tp )
            g.Connect( V, ("rel_%d"%a, M_INT, v+a), T )
            T.Close()
        V.Close()
        if not v % 10000:
            LogInfo( "Building data: %s" % g )

    LogInfo( "Done building data" )
    op.Detach()

    # Count elements in output
    f = open( opcodes, "r" )
    DATA = f.readlines()
    f.close()
    ELEMENTS = {
        "tx"    : 0,
        "OP"    : 0,
        "code"  : 0,
        "lines" : 0,
        "bytes" : 0
    }
    OP = False
    for line in DATA:
        ELEMENTS["bytes"] += len(line)
        ELEMENTS["lines"] += 1
        if "TRANSACTION" in line:
            ELEMENTS["tx"] += 1
        elif " OP " in line:
            ELEMENTS["OP"] += 1
            OP = True
        elif "ENDOP" in line:
            OP = False
        elif OP:
           opcode = line.split(None,1)
           if opcode and len(opcode[0]) == 3:
               ELEMENTS["code"] += 1


    # Capture graph info
    # vertices
    LogInfo( "Capture all vertex info" )
    ALL_VERTICES = g.Vertices( result=R_LIST, fields=F_ID|F_TYPE|F_PROP )
    ALL_VERTICES.sort()
    # arcs
    LogInfo( "Capture all arc info" )
    ALL_ARCS = g.Arcs() 
    ALL_ARCS.sort()


    THROTTLE_LIMITS = [
        [ None,            None,                   0  ],
        [ "bytes",         ELEMENTS["bytes"]/10,   10 ],  # Limit bytes/s to complete in ~10 seconds
        [ "opcodes",       ELEMENTS["code"]/15,    15 ],  # Limit opcodes/s to complete in ~20 seconds
        [ "operations",    ELEMENTS["OP"]/20,      20 ],  # Limit operations/s to complete in ~30 seconds
        [ "transactions",  ELEMENTS["tx"]/25,      25 ],  # Limit transactions/s to complete in ~40 seconds
        [ None,            None,                   0  ]
    ]

    #
    UNLIMITED_RATE_LOAD_TIME = -1

    # Erase graph
    g.Erase()
    del g

    # Reset serial numbers
    op.Reset()
    system.System().ResetSerial()

    # Shut down
    system.Unload()

    for unit, rate, expect_duration in THROTTLE_LIMITS:

        # Initialize another system in consumer mode
        system.Initialize( SYSROOT+"_consumer", attach=None, events=False )
        op.Profile( op.OP_PROFILE_consumer )
        
        # Set throttle limit
        if( unit is None ):
            T = op.Throttle( None )
        else:
            T = op.Throttle( rate=rate, unit=unit )

        LogInfo( "Rebuild graph from operation data: %s" % opcodes )
        LogInfo( "Throttle limits: %s" % T )

        # Rebuild the graph from operation data
        op.Consume( DATA )
        t0 = time.time()

        LogInfo( "Consumed: %s" % ELEMENTS )

        time.sleep( 1 )

        # Give it time
        pending = op.Pending()
        if expect_duration == 0:
            MIN_TIME = 0
            if UNLIMITED_RATE_LOAD_TIME < 0:
                MAX_TIME = 30 # default
            else:
                MAX_TIME = UNLIMITED_RATE_LOAD_TIME + 2
        else:
            MIN_TIME = expect_duration - 5
            MAX_TIME = expect_duration + 5

        LogInfo( "Expect load time in range %d-%d seconds" % (MIN_TIME, MAX_TIME) )

        while pending > 0:
            #LogInfo( "Input data pending consumption: %d" % pending )
            time.sleep(0.1)
            pending = op.Pending()
            t1 = time.time()
            if t1 - t0 > MAX_TIME:
                Expect( False, "Consumer should have processed all input after %d seonds" % MAX_TIME )
        t1 = time.time()
        duration_measured = t1 - t0
        if duration_measured < MIN_TIME:
            Expect( False, "Consumer should limit processing speed. Expected time %d, got %d. Limits=%s" % (MIN_TIME, duration_measured, op.Throttle()) )

        LogInfo( "Loaded data in %d seconds" % duration_measured )

        # Register the max speed
        if UNLIMITED_RATE_LOAD_TIME < 0 and expect_duration == 0:
            UNLIMITED_RATE_LOAD_TIME = duration_measured

        LogInfo( "Restore and verify" )
        # 
        g = Graph( GRAPH )
        Expect( g.order == len( ALL_VERTICES ),                 "restored graph has %d vertices, should have %d" % ( g.order, len(ALL_VERTICES) ) )
        Expect( g.size == len( ALL_ARCS ),                      "restored graph has %d arcs, should have %d" % ( g.size, len(ALL_ARCS) ) )
        LogInfo( "Restored %d vertices and %d arcs" % (g.order, g.size) )

        # Now verify the reconstructed graph
        # Capture restored graph info
        
        # vertices
        RESTORED_VERTICES = g.Vertices( result=R_LIST, fields=F_ID|F_TYPE|F_PROP )
        RESTORED_VERTICES.sort()
        Expect( RESTORED_VERTICES == ALL_VERTICES,              "restored vertices should match original graph data" )
        LogInfo( "Verified %d restored vertex names, types and properties" % ( len(RESTORED_VERTICES) ) )

        # arcs
        RESTORED_ARCS = g.Arcs() 
        RESTORED_ARCS.sort()
        Expect( RESTORED_ARCS == ALL_ARCS,                      "restored arcs should match original graph data" )
        LogInfo( "Verified %d restored arcs" % ( len(RESTORED_ARCS) ) )

        # Erase graph
        g.Erase()
        del g

        # Reset serial numbers
        op.Reset()
        system.System().ResetSerial()

        # Shut down
        system.Unload()
        

    # ok!
    del ALL_VERTICES
    del ALL_ARCS
    del RESTORED_VERTICES
    del RESTORED_ARCS




UNICODE = list(itertools.chain(range(0x20,0xd800),range(0xe000,0x110000)))



###############################################################################
# get_vertex_name
#
###############################################################################
def get_vertex_name( N ):
    """
    """
    # Produce a vertex name with variable length
    r = random.randint( 1, N )
    digits = sha256(str(r))
    u = random.randint( 0, 5 )
    utf8 = "".join(map( chr, random.sample( UNICODE, u ) )).encode()
    name = b"node_%d_%s_%s" % (r, utf8, digits[:1+r%64].encode())
    return r, name, utf8, digits




###############################################################################
# get_vertex
#
###############################################################################
def get_vertex( g, N ):
    """
    """
    r, name, utf8, digits = get_vertex_name( N )
    type = "type_%d" % (r%20)
    raw = bytes( [int(x,16) for x in digits] )
    prop = { 'int':r, 'float':random.random(), 'raw':raw, 'digits':digits, 'utf8':utf8, 'unicode':utf8.decode(), ("key_%s" % digits[:3]) : r }
    if name in g:
        V = g.NewVertex( name, properties = prop )
        V.SetType( type )
    else:
        V = g.NewVertex( name, type=type, properties = prop )
    return V




###############################################################################
# delete_vertex
#
###############################################################################
def delete_vertex( g, N ):
    """
    """
    r, name, _ign1, _ign2 = get_vertex_name( N )
    try:
        g.DeleteVertex( name )
    except:
        pass




###############################################################################
# get_dim
#
###############################################################################
def get_dim():
    """
    """
    return "".join( [chr(random.randint( 97, 122 )) for n in range(4)] )



#def get_weight():
#    return random.random()



#def get_vector( N ):
#    return sorted( [(get_dim(), get_weight()) for n in range(N)], key=lambda x:x[1], reverse=True )



#def set_vector( vertex, N ):
#    vec = get_vector( N )
#    vertex.SetVector( vec )




###############################################################################
# delete_vector
#
###############################################################################
def delete_vector( vertex ):
    """
    """
    vertex.RemoveVector()




###############################################################################
# connect
#
###############################################################################
def connect( g, N, V, term_min=1, term_max=10 ):
    """
    """
    for n in range( random.randint( term_min, term_max ) ):
        r, term, _ign1, _ign2 = get_vertex_name( N )
        rel = "rel_%d" % random.randint(1,25)
        T = g.NewVertex( term )
        for mod in random.sample( [M_INT,M_FLT,M_UINT,M_CNT,M_ACC], random.randint(1,2) ):
            if mod in [M_FLT, M_ACC]:
                val = random.random() * 100
            else:
                val = random.randint( 0, 99 )
            g.Connect( V, (rel, mod, val), T )
        T.Close()




###############################################################################
# disconnect
#
###############################################################################
def disconnect( g, N ):
    """
    """
    r1, init, _ign1, _ign2 = get_vertex_name( N )
    r2, term, _ign1, _ign2 = get_vertex_name( N )
    try:
        g.Disconnect( init, "*", term )
    except:
        pass




###############################################################################
# TEST_pyvgx_op_Produce_random
#
###############################################################################
def TEST_pyvgx_op_Produce_random():
    """
    Random graph generation then pyvgx.op.Consume() to restore
    t_nominal=321
    test_level=1102
    """
    opcodes = "./%s/opcodes_random" % (SYSROOT)
    system.Unload()
    system.Initialize( SYSROOT+"_random_producer", attach="file:///%s" % opcodes )

    # Create a graph
    g = Graph( GRAPH )
    g.Truncate()

    op.Heartbeat( False )

    # Max number of vertices
    N = 100000
    # Total iterations for building random graph
    ITER = 1000000
    P = 50000

    for n in range(1, ITER+1):
        V = get_vertex( g, N )
        if random.random() > 0.95:
            delete_vector( V )
        if random.random() > 0.95:
            disconnect( g, N )
        connect( g, N, V, 1, 15 )
        #set_vector( V, random.randint(3,8) )
        V.SetVector( g.sim.rvec( 64 ) )
        V['n'] = n
        if random.random() > 0.8:
            disconnect( g, N )
        if not n % P:
            LogInfo( "Building graph: %d%%" % (100*n/ITER) )
        V.Close()

    op.Detach()

    # Capture graph status
    STATUS = g.Status()
    VERTICES = g.Vertices()
    ARCS = g.Arcs()

    # Erase graph and shut down
    g.Erase()
    system.Unload()

    LogInfo( "Rebuild graph from operation data: %s" % opcodes )

    # Grab the operation data written above
    f = open( opcodes, "r" )

    # Initialize another system in consumer mode
    system.Initialize( SYSROOT+"_random_consumer", attach=None, events=False )
    op.Profile( op.OP_PROFILE_consumer )

    # Rebuild the graph from operation data
    n = 0
    b = 0
    for line in f:
        op.Consume( line )
        n += 1
        b += len(line)
        if not n%10000:
            LogInfo( "Consumed %d input bytes" % b )
    
    LogInfo( "Consumed %d input bytes" % b )
    f.close()

    # Give it time
    pending = op.Pending()
    t0 = time.time()
    MAX_TIME = 30
    while pending > 0:
        LogInfo( "Input data pending consumption: %d" % pending )
        time.sleep(0.1)
        pending = op.Pending()
        t1 = time.time()
        if t1 - t0 > MAX_TIME:
            Expect( false, "Consumer should have processed all input after %d seonds" % MAX_TIME )

    # Verify
    g = Graph( GRAPH )
    RESTORED_STATUS = g.Status()
    RESTORED_VERTICES = g.Vertices()
    RESTORED_ARCS = g.Arcs()


    A = set(VERTICES)
    B = set(RESTORED_VERTICES)
    print( "Original vertex count: %d" % len(A) )
    print( "Restored vertex count: %d" % len(B) )
    if A != B:
        print( "DIFFERENCE DETECTED" )
        inA_notinB = A.difference( B )
        inB_notinA = B.difference( A )
        print( "Missing from restored set: %s" % inA_notinB )
        print( "Unexpected in restored set: %s" % inB_notinA )

    A = set(ARCS)
    B = set(RESTORED_ARCS)
    print( "Original arc count: %d" % len(A) )
    print( "Restored arc count: %d" % len(B) )
    if A != B:
        print( "DIFFERENCE DETECTED" )
        inA_notinB = A.difference( B )
        inB_notinA = B.difference( A )
        print( "Missing from restored set: %s" % inA_notinB )
        print( "Unexpected in restored set: %s" % inB_notinA )



    orig = STATUS['base']
    restored = RESTORED_STATUS['base']
    Expect( restored == orig,  "Restored base counts '%s', expected '%s'" % (restored, orig) )

    orig = STATUS['enum']
    restored = RESTORED_STATUS['enum']
    Expect( restored == orig,  "Restored enum '%s', expected '%s'" % (restored, orig) )

    tx_out_id = STATUS['transaction']['out']['id']
    tx_out_sn = STATUS['transaction']['out']['serial']
    tx_in_id = RESTORED_STATUS['transaction']['in']['id']
    tx_in_sn = RESTORED_STATUS['transaction']['in']['serial']
    Expect( tx_in_id == tx_out_id, "Last transaction id %s, expected %s" % (tx_in_id, tx_out_id) )
    Expect( tx_in_sn == tx_out_sn, "Last transaction serial %d, expected %d" % (tx_in_sn, tx_out_sn) )

    g.Erase()



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
