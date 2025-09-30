###############################################################################
# 
# VGX Server
# Distributed engine for plugin-based graph and vector search
# 
# Module:  pyvgx.test
# File:    Memory.py
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




###############################################################################
# TEST_Memory_basic
#
###############################################################################
def TEST_Memory_basic():
    """
    pyvgx.Memory basic
    test_level=3101
    """
    graph_name = "memory_graph"
    g = Graph( graph_name )

    # Test basic assign and lookup
    for x in range(2,16):
        memsz = 2 ** x
        m = g.Memory( memsz )
        
        # Order
        Expect( m.order == x )

        # Registers
        m.R1 = 1001
        m.R2 = 2002
        m.R3 = 3003
        m.R4 = 4004

        Expect( m.R1 == 1001 )
        Expect( m.R2 == 2002 )
        Expect( m.R3 == 3003 )
        Expect( m.R4 == 4004 )

        Expect( m[R1] == m.R1 )
        Expect( m[R2] == m.R2 )
        Expect( m[R3] == m.R3 )
        Expect( m[R4] == m.R4 )

        Expect( g.Evaluate( "load(R1) == 1001", memory=m ) )
        Expect( g.Evaluate( "load(R2) == 2002", memory=m ) )
        Expect( g.Evaluate( "load(R3) == 3003", memory=m ) )
        Expect( g.Evaluate( "load(R4) == 4004", memory=m ) )

        # Reset
        m.Reset()
        Expect( m.R1 == 0 )
        Expect( m.R2 == 0 )
        Expect( m.R3 == 0 )
        Expect( m.R4 == 0 )

        # AsList
        Expect( m.AsList() == list(m) )
        Expect( m.AsList() == [0]*memsz )

        # Reset, range
        m.Reset( 0, 1 )
        Expect( m.AsList() == list(m) )
        Expect( m.AsList() == list(range(memsz)) )

        # Assign and lookup
        tval = 1234
        for n in range( memsz ):
            val = n + tval
            m[n] = val
            Expect( m[n] == val, "m[%d] = %d" % (n,val) )
            Expect( g.Evaluate( "load(%d) == %d" % (n, val), memory=m ) )
            Expect( g.Evaluate( "load(%d+%d) == %d" % (n, memsz, val), memory=m ) )
            Expect( g.Evaluate( "load(%d-%d) == %d" % (n, memsz, val), memory=m ) )
            Expect( g.Evaluate( "store(%d, %d)" % (n, val*2), memory=m ) )
            Expect( m[n] == val*2, "m[%d] = %d" % (n,val*2) )
            fval = float( val ) / 3.0
            m[n] = fval
            Expect( m[n] == fval, "m[%d] = %f" % (n,val) )

    g.Erase()




###############################################################################
# TEST_Memory_init_source
#
###############################################################################
def TEST_Memory_init_source():
    """
    pyvgx.Memory init from source
    test_level=3101
    """
    graph_name = "memory_graph"
    g = Graph( graph_name )

    for n in range(1000):
        # int
        source = list(range(n))
        m = g.Memory( source )
        Expect( list(m)[:n] == source )
        # float
        source = [x/3.0 for x in range(n)]
        m = g.Memory( source )
        Expect( list(m)[:n] == source )

    g.Erase()




###############################################################################
# TEST_Memory_sort
#
###############################################################################
def TEST_Memory_sort():
    """
    pyvgx.Memory sort
    test_level=3101
    """
    graph_name = "memory_graph"
    g = Graph( graph_name )


    m = g.Memory( 1024 )

    m[0] = 5
    m[1] = 7
    m[2] = 4
    m[3] = 1
    m[4] = 6
    m[5] = 7
    m.R1 = 1
    m.R2 = 2
    m.R3 = 3
    m.R4 = 4

    Expect( m[:6] == [5,7,4,1,6,7] )

    m.Sort( 1, 4 )

    Expect( m[:6] == [5,  1,4,7,  6,7] )

    m.Sort( 1, 4, reverse=True )

    Expect( m[:6] == [5,  7,4,1,  6,7] )

    m.Sort( reverse=True )

    Expect( m[:10] == [7,7,6,5,4,1,0,0,0,0] )
    Expect( m.R1 == 1 and m.R2 == 2 and m.R3 == 3 and m.R4 == 4 )

    m.Sort()

    Expect( m[:10] == [0,0,0,0,0,0,0,0,0,0] )
    Expect( m[-10:-4] == [1,4,5,6,7,7] )
    Expect( m.R1 == 1 and m.R2 == 2 and m.R3 == 3 and m.R4 == 4 )

    m.Sort( 0, 1024 )
    Expect( m[-10:] == [1,1,2,3,4,4,5,6,7,7] )

    g.Evaluate( "mrandomize(0,1023)", memory=m )
    L = list(m)
    L.sort()
    m.Sort( 0, 1024 )
    Expect( list(m) == L )

    g.Erase()



###############################################################################
# TEST_Memory_slices
#
###############################################################################
def TEST_Memory_slices():
    """
    pyvgx.Memory.slices
    test_level=3101
    """
    graph_name = "memory_graph"
    g = Graph( graph_name )

    # Test slices   
    m = g.Memory( 32 )

    m[0] = 1000
    m[0:] = []
    Expect( m[0] == 1000 )

    m[0:] = [1]
    Expect( m[0] == 1 )
    Expect( m[1] == 0 )

    m[0:] = [1,2]
    Expect( m[0] == 1 )
    Expect( m[1] == 2 )
    Expect( m[2] == 0 )

    m[0:] = [1,2,3,4,500]
    Expect( m[0] == 1 )
    Expect( m[1] == 2 )
    Expect( m[2] == 3 )
    Expect( m[3] == 4 )
    Expect( m[4] == 500 )

    try:
        m[30:] = 7
    except ValueError:
        pass # can only assign list
    except:
        Expect( False )

    m[30:] = [1]
    m[30:] = [1,2]

    try:
        m[30:] = [1,2,3]
    except ValueError:
        pass # too many values (beyond end)
    except:
        Expect( False )
    
    m[0:4] = list(range(1))
    m[0:4] = list(range(2))
    m[0:4] = list(range(3))
    m[0:4] = list(range(4))

    try:
        m[0:4] = list(range(5))
    except ValueError:
        pass # too many values
    except:
        Expect( False ) 

    m.Reset()
    for n in range( 32 ):
        Expect( m[n] == 0 )
    Expect( m[:] == [0]*32 )

    m.Reset(1)
    for n in range( 32 ):
        Expect( m[n] == 1 )
    Expect( m[:] == [1]*32 )

    m.Reset(0,1)
    for n in range( 32 ):
        Expect( m[n] == n )
    Expect( m[:] == list(range(32)) )

    m.Reset(7,11)
    for n in range( 32 ):
        Expect( m[n] == 7 + n*11 )

    m.Reset(0,1)
    for n in range(32):
        Expect( m[n:] == list(range(n,32)) )
        for k in range(n,32):
            Expect( m[n:k] == list(range(n,k)) )

    for n in range(1,32+1):
        m[-n:] == list(range(32-n))


    g.Erase()



###############################################################################
# TEST_Memory_bitvector
#
###############################################################################
def TEST_Memory_bitvector():
    """
    pyvgx.Memory bitvector
    test_level=3101
    """
    graph_name = "memory_graph"
    g = Graph( graph_name )

    m = g.Memory( 32 )

    # Bitvector assignment syntax is to assign list when memory index is not a slice
    m[0] = 1000       # int
    m[1] = 3.14       # real
    m[2] = [0xABCDEF] # bitvector

    Expect( m[0] == 1000 )
    Expect( m[1] == 3.14 )
    Expect( m[2] == [0xABCDEF] )

    Expect(     g.Evaluate( "isint(load(0))",       memory=m ) )
    Expect( not g.Evaluate( "isreal(load(0))",      memory=m ) )
    Expect( not g.Evaluate( "isbitvector(load(0))", memory=m ) )

    Expect( not g.Evaluate( "isint(load(1))",       memory=m ) )
    Expect(     g.Evaluate( "isreal(load(1))",      memory=m ) )
    Expect( not g.Evaluate( "isbitvector(load(1))", memory=m ) )
    
    Expect( not g.Evaluate( "isint(load(2))",       memory=m ) )
    Expect( not g.Evaluate( "isreal(load(2))",      memory=m ) )
    Expect(     g.Evaluate( "isbitvector(load(2))", memory=m ) )

    g.Erase()




###############################################################################
# TEST_Memory_bytearray
#
###############################################################################
def TEST_Memory_bytearray():
    """
    pyvgx.Memory bytearray
    test_level=3101
    """
    graph_name = "memory_graph"
    g = Graph( graph_name )

    m = g.Memory( 32 )

    R = list(range( 0, 128))

    m[0] = bytearray( R )
    m[1] = 123

    Expect( m[0] == bytearray(R) )

    Expect( g.Evaluate( "isbytearray( load(0) )", memory=m ) )
    Expect( g.Evaluate( "!isbytearray( load(1) )", memory=m ) )

    g.Erase()




###############################################################################
# TEST_Memory_keyval
#
###############################################################################
def TEST_Memory_keyval():
    """
    pyvgx.memory keyval
    test_level=3101
    """
    graph_name = "memory_graph"
    g = Graph( graph_name )

    m = g.Memory( 32 )

    m[0] = (123, 1.0)
    m[1] = 123

    Expect( type( m[0] ) is tuple )
    Expect( len( m[0] ) == 2 )
    Expect( type( m[0][0] ) is int )
    Expect( type( m[0][1] ) is float )
    Expect( m[0][0] == 123 )
    Expect( m[0][1] == 1.0 )
    Expect( type( m[1] ) is int )

    Expect( g.Evaluate( "iskeyval( load(0) )", memory=m ) )
    Expect( g.Evaluate( "!iskeyval( load(1) )", memory=m ) )

    g.Erase()




###############################################################################
# TEST_Memory_multivalue
#
###############################################################################
def TEST_Memory_multivalue():
    """
    pyvgx.memory multi-value properties and probes
    test_level=3101
    """
    graph_name = "memory_graph"
    g = Graph( graph_name )

    m = g.Memory( 32 )

    V = g.NewVertex( "V" )
    V['array'] = list(range(1000))
    V['bytearray'] = bytearray( [x%256 for x in range(1000)] )
    V['map'] = dict.fromkeys( list(range(1000)), 1.23 )

    Expect( g.Evaluate( "isarray( vertex['array'] )", memory=m, tail="V" ) )
    Expect( g.Evaluate( "!isarray( vertex['bytearray'] )", memory=m, tail="V" ) )
    Expect( g.Evaluate( "!isarray( vertex['map'] )", memory=m, tail="V" ) )

    Expect( g.Evaluate( "!isbytearray( vertex['array'] )", memory=m, tail="V" ) )
    Expect( g.Evaluate( "isbytearray( vertex['bytearray'] )", memory=m, tail="V" ) )
    Expect( g.Evaluate( "!isbytearray( vertex['map'] )", memory=m, tail="V" ) )

    Expect( g.Evaluate( "!ismap( vertex['array'] )", memory=m, tail="V" ) )
    Expect( g.Evaluate( "!ismap( vertex['bytearray'] )", memory=m, tail="V" ) )
    Expect( g.Evaluate( "ismap( vertex['map'] )", memory=m, tail="V" ) )

    del V
    del m

    # TODO: msumprodobj()

    # TODO: dp_pi8

    # TODO: cos_pi8

    g.Erase()




###############################################################################
# TEST_memory_msubsetobj
#
###############################################################################
def TEST_memory_msubsetobj():
    """
    pyvgx.memory msubsetobj
    test_level=3102
    """
    graph_name = "memory_graph"
    g = Graph( graph_name )

    m = g.Memory( 32 )

    V = g.NewVertex( "V" )

    R = list(range(1,2500))

    for N in R:
        values = list(range(N))

        intarray = values
        keyvalmap = dict([ (n, 2.0*n) for n in values ])

        m[0] = values[0]                # first (int type)
        m[1] = (values[0], 1.0)         # last (keyval type)
        m[2] = values[-1] + 1           # should not match (int type)
        m[3] = (values[-1] + 1 , 1.0)   # should not match (keyval type)

        for obj in [intarray, keyvalmap]:
            V['obj'] = obj

            if type( obj ) is list:
                Expect( g.Evaluate( "isarray( vertex['obj'] )", memory=m, tail=V ) )
            elif type( obj ) is dict:
                Expect( g.Evaluate( "ismap( vertex['obj'] )", memory=m, tail=V ) )
            else:
                Expect( False, "bad object" )

            Expect( g.Evaluate( "  msubsetobj( 0, 0, vertex['obj'] )", memory=m, tail=V ) )
            Expect( g.Evaluate( "  msubsetobj( 0, 1, vertex['obj'] )", memory=m, tail=V ) )
            Expect( g.Evaluate( "! msubsetobj( 0, 2, vertex['obj'] )", memory=m, tail=V ) )
            Expect( g.Evaluate( "! msubsetobj( 0, 3, vertex['obj'] )", memory=m, tail=V ) )
            Expect( g.Evaluate( "  msubsetobj( 1, 1, vertex['obj'] )", memory=m, tail=V ) )
            Expect( g.Evaluate( "! msubsetobj( 1, 2, vertex['obj'] )", memory=m, tail=V ) )
            Expect( g.Evaluate( "! msubsetobj( 1, 3, vertex['obj'] )", memory=m, tail=V ) )
            Expect( g.Evaluate( "! msubsetobj( 2, 2, vertex['obj'] )", memory=m, tail=V ) )
            Expect( g.Evaluate( "! msubsetobj( 2, 3, vertex['obj'] )", memory=m, tail=V ) )
            Expect( g.Evaluate( "! msubsetobj( 3, 3, vertex['obj'] )", memory=m, tail=V ) )


    del V
    del m

    g.Erase()




###############################################################################
# TEST_memory_msumprodobj
#
###############################################################################
def TEST_memory_msumprodobj():
    """
    pyvgx.memory msumprodobj
    t_nominal=12
    test_level=3102
    """
    graph_name = "memory_graph"
    g = Graph( graph_name )

    m = g.Memory( 2000 )

    P = 1000

    for n in range( 0, P ):
        # int probes
        m[n] = n
        # keyval probes
        m[n+P] = (n, 2.0)


    V = g.NewVertex( "V" )

    R = list(range(1,2500))

    for N in R:
        values = list(range(N))

        V['intarray'] = values
        V['keyvalmap'] = dict([ (n, 2.0) for n in values ])

        m.R1 = min(N,P) - 1
        m.R2 = P + min(N,P) - 1

        m.R4 = min(N,P)

        Expect( g.Evaluate( "store( R3, msumprodobj( 0, load(R1), vertex['intarray'] )) == load(R4)", memory=m, tail=V ),               "%g, got %g" % (m.R4, m.R3) )
        Expect( g.Evaluate( "store( R3, msumprodobj( load(R4), load(R2), vertex['intarray'] )) == load(R4) * 2.0", memory=m, tail=V ),  "%g, got %g" % (2.0*m.R4, m.R3) )

        Expect( g.Evaluate( "store( R3, msumprodobj( 0, load(R1), vertex['keyvalmap'] )) == load(R4) * 2.0", memory=m, tail=V ),        "%g, got %g" % (2.0*m.R4, m.R3) )
        Expect( g.Evaluate( "store( R3, msumprodobj( load(R4), load(R2), vertex['keyvalmap'] )) == load(R4) * 4.0", memory=m, tail=V ), "%g, got %g" % (4.0*m.R4, m.R3) )

    del V
    del m

    g.Erase()





###############################################################################
# Run
#
###############################################################################
def Run( name ):
    """
    """
    RunTests( [__name__] )
