###############################################################################
# 
# VGX Server
# Distributed engine for plugin-based graph and vector search
# 
# Module:  pyvgx
# File:    EuclideanVector.py
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

from pytest.pytest import RunTests, Expect, TestFailed, PerformCleanup
from pyvgx import *
import pyvgx
import random
import math
import sys
import time
import re
import struct
import itertools


graph = None

quant = {
    512: 64,
    2 : 32,
    1 : 16,
    0 : 16 # No AVX, most likely ARM/Neon
}

SZ_STEP = quant[ avxbuild() ]

SIZES = itertools.chain( range(0, 4096, SZ_STEP), range(4096, 64512, 1024), (65456,) )


def approx_eq( f1, f2, err=0.05 ):
    diff = abs( 1 - f1/f2 ) if f2 != 0 else abs( 1 - f2/f1) if f1 != 0 else 0
    return diff < err



def EuclideanDistance( L1, L2 ):
    return math.sqrt( sum( [ (a-b)**2 for a, b in zip( L1, L2 ) ] ) )


def Cosine( L1, L2 ):
    dp = sum([a*b for a, b in zip( L1, L2 )])
    m1 = sum([a**2 for a in L1]) ** 0.5
    m2 = sum([b**2 for b in L2]) ** 0.5
    return dp / (m1*m2) if m1 > 0 and m2 > 0 else 0.0



def TEST_Vector_basic():
    """
    pyvgx.Vector() basics
    test_level=3101
    t_nominal=1
    """

    # Type
    Expect( type( Vector() ) is Vector )
    Expect( type( graph.sim.NewVector() ) is Vector )

    # Nullvector
    Expect( len( Vector() ) == 0 )
    Expect( len( graph.sim.NewVector() ) == 0 )
    Expect( len( Vector([]) ) == 0 )
    Expect( len( graph.sim.NewVector([]) ) == 0 )

    # Create basic vector
    L = [0.25, 0.5, 0.75, 1.0, -0.25, -0.5, -0.75, -1.0]

    # Vector pads zeros for missing trailing elements
    V0 = Vector( L )
    V1 = graph.sim.NewVector( L )

    # Length (w/padding) depends on build's avx version
    Expect( len(V0) == SZ_STEP )
    Expect( len(V1) == SZ_STEP )

    # Magnitude
    mag = sum([x**2 for x in L]) ** 0.5
    Expect( approx_eq( V0.magnitude, mag ) )
    Expect( approx_eq( V0.magnitude, abs(V0) ) )
    Expect( approx_eq( V1.magnitude, mag ) )
    Expect( approx_eq( V1.magnitude, abs(V1) ) )

    # Internal component values
    for i in range( len(L) ):
        x = L[i]
        b = round( (256 if L[i] < 0 else 0) + 127 * x )
        Expect( int(V0.internal[i]) == b,                   "expected {} got {}".format( b, int(V0.internal[i]) ) )
        Expect( int(V1.internal[i]) == b,                   "expected {} got {}".format( b, int(V1.internal[i]) ) )

    # External component values
    for i in range( len(L) ):
        x = L[i]
        Expect( approx_eq( V0.external[i], x ),             "expected {} got {}".format( x, V0.external[i] ) )
        Expect( approx_eq( V1.external[i], x ),             "expected {} got {}".format( x, V1.external[i] ) )

    # Alpha
    fmt = "{}b".format( SZ_STEP )
    V0_bytes = struct.unpack( fmt, V0.internal )
    V1_bytes = struct.unpack( fmt, V1.internal )
    for i in range( len(L) ):
        x = L[i]
        V0_bxa = V0_bytes[i] * V0.alpha
        V1_bxa = V1_bytes[i] * V1.alpha
        Expect( approx_eq( V0_bxa, x ),                     "expected {} got {}".format( x, V0_bxa ) )
        Expect( approx_eq( V1_bxa, x ),                     "expected {} got {}".format( x, V1_bxa ) )



def TEST_Vector_components():
    """
    pyvgx.Vector() normalization, quantization and positional correctness
    test_level=3101
    t_nominal=1
    """

    # Create vectors
    L3 = [0.00, 0.01, 0.02, 0.03, 0.04, 0.05, 0.06, 0.07, 0.08, 0.09,  0.1]
    V3 = Vector( L3 )
    L1 = [0.00, 0.10, 0.20, 0.30, 0.40, 0.50, 0.60, 0.70, 0.80, 0.90,  1.0]
    V1 = Vector( L1 )
    L2 = [0.00, 1.00, 2.00, 3.00, 4.00, 5.00, 6.00, 7.00, 8.00, 9.00, 10.0]
    V2 = Vector( L2 )

    # Check component values within quantization tolerance
    Expect( False not in [ abs(a-b) < 0.04 for a,b in zip( V3.external, L3 ) ] )
    Expect( False not in [ abs(a-b) < 0.04 for a,b in zip( V1.external, L1 ) ] )
    Expect( False not in [ abs(a-b) < 0.04 for a,b in zip( V2.external, L2 ) ] )

    # Check component position correctness
    STEPS = [SZ_STEP*i for i in range(1,8)]

    for N in STEPS:
        L = [0] * N
        for i in range( N ):
            L[i] = 1
            V = Vector( L )
            L[i] = 0

            V_int = list(V.internal)
            V_ext = list(V.external)

            Expect( V_int[i] == 127 )
            Expect( V_ext[i] == 1.0 )

            V_int[i] = 0
            V_ext[i] = 0.0

            Expect( V_int == L )
            Expect( V_ext == L )



def TEST_Vector_similarity():
    """
    Euclidean distance and Cosine similarity
    test_level=3101
    t_nominal=2
    """
    def VertexEuclideanDistance( vertex_A, vertex_B ):
        V1 = vertex_A.GetVector().external
        V2 = vertex_B.GetVector().external
        return EuclideanDistance( V1, V2 )


    def VertexCosine( vertex_A, vertex_B ):
        V1 = vertex_A.GetVector().external
        V2 = vertex_B.GetVector().external
        return Cosine( V1, V2 )


    m = graph.Memory( 1024 )

    R = [ random.randint(1,3)*(random.random()-0.5) for n in range(65536) ]
    Ra = [ x+random.random()-0.5 for x in R ]
    Rb = [ x+random.random()-0.5 for x in R ]

    for n in SIZES:
        print("{}".format(n), end=' ', flush=True)

        # Source
        La = Ra[:n]
        Lb = Rb[:n]

        # Actual metrics
        true_sim = Cosine( La, Lb )
        true_dist = EuclideanDistance( La, Lb )

        # First
        Va = graph.sim.NewVector( La )
        A = graph.NewVertex("A")
        A.SetVector( La ) # set source array

        # Second
        Vb = graph.sim.NewVector( Lb )
        B = graph.NewVertex("B")
        B.SetVector( Vb ) # set vector object

        # Compute metrics on vectors
        sim = graph.sim.Cosine( Va, Vb )
        dist = graph.sim.EuclideanDistance( Va, Vb )
        Expect( approx_eq( sim, true_sim ),                     "Cosine(Va,Vb)={}, got {}".format( true_sim, sim ) )
        Expect( approx_eq( dist, true_dist ),                   "EuclideanDistance(Va,Vb)={}, got {}".format( true_dist, dist ) )

        # Compute metrics on vertices
        sim = graph.sim.Cosine( A, B )
        dist = graph.sim.EuclideanDistance( A, B )
        Expect( approx_eq( sim, true_sim ),                     "Cosine(Va,Vb)={}, got {}".format( true_sim, sim ) )
        Expect( approx_eq( dist, true_dist ),                   "EuclideanDistance(Va,Vb)={}, got {}".format( true_dist, dist ) )

        # Vector methods
        sim1 = Va.Cosine( Vb )
        sim2 = Vb.Cosine( Va )
        dist1 = Va.EuclideanDistance( Vb )
        dist2 = Vb.EuclideanDistance( Va )
        Expect( approx_eq( sim1, true_sim ),                    "Va.Cosine(Vb)={}, got {}".format( true_sim, sim1 ) )
        Expect( approx_eq( sim2, true_sim ),                    "Vb.Cosine(Va)={}, got {}".format( true_sim, sim2 ) )
        Expect( approx_eq( dist1, true_dist ),                  "Va.EuclideanDistance(Vb)={}, got {}".format( true_dist, dist1 ) )
        Expect( approx_eq( dist2, true_dist ),                  "Vb.EuclideanDistance(Va)={}, got {}".format( true_dist, dist2 ) )

        # Vector arithmetic
        Lab = [ a+b for a, b in zip(La,Lb) ]
        La_b = [ a-b for a, b in zip(La,Lb) ]
        dpL = sum([ a*b for a, b in zip(La,Lb) ])
        Laf = [ a * 3.14 for a in La ]
        Lbf = [ b * 2.72 for b in Lb ]

        # Compute various
        Vab = Va + Vb
        Va_b = Va - Vb
        dpV = Va * Vb
        Vaf = Va * 3.14
        Vbf = Vb * 2.72

        # Compare
        err_dist = (1+abs(Vab)) * 0.05
        Expect( graph.sim.EuclideanDistance( Vab, Lab ) < err_dist,     "Vector sum, got error distance {}".format( err_dist ) )
        Expect( graph.sim.EuclideanDistance( Va_b, La_b ) < err_dist,   "Vector difference, got error distance {}".format( err_dist ) )
        Expect( approx_eq( dpV, dpL ),                                  "Vector dot product, got {} != {}".format( dpV, dpL ) )
        err_dist = (1+abs(Vaf)) * 0.05
        Expect( graph.sim.EuclideanDistance( Vaf, Laf ) < err_dist,     "Vector scalar multiply, got error distance {}".format( err_dist ) )
        err_dist = (1+abs(Vbf)) * 0.05
        Expect( graph.sim.EuclideanDistance( Vbf, Lbf ) < err_dist,     "Vector scalar multiply, got error distance {}".format( err_dist ) )
        
        # Expected distance
        m.R4 = dist
        
        # Expected cosine
        m.R3 = sim
        
        # Max relative error
        m.R2 = 0.001
        
        # Test#
        m.R1 = 1

        match = graph.Evaluate(
            """
            target_dist = r4;
            target_sim = r3;
            maxerr = r2;
            resultreg = 0;
            targetreg = 1;

            /* Euclidean Distance */
            store( targetreg, target_dist );
            dist_A_A = store( resultreg, ecld_pi8_256( vertex.vector, vertex.vector ) );  require( approx( dist_A_A,         0.0, maxerr ) ); inc( R1 );
            dist_A_B = store( resultreg, ecld_pi8_256( vertex.vector, next.vector ) );    require( approx( dist_A_B, target_dist, maxerr ) ); inc( R1 );
            dist_B_A = store( resultreg, ecld_pi8_256( next.vector,   vertex.vector ) );  require( approx( dist_B_A, target_dist, maxerr ) ); inc( R1 );
            dist_B_B = store( resultreg, ecld_pi8_256( next.vector,   next.vector ) );    require( approx( dist_B_B,         0.0, maxerr ) ); inc( R1 );

            /* Cosine */
            sim_ident = len(vertex.vector) > 0 ? 1.0 : 0.0;
            store( targetreg, target_sim );
            sim_A_A = store( resultreg, cos_pi8_256( vertex.vector, vertex.vector ) );  require( approx( sim_A_A,  sim_ident, maxerr ) ); inc( R1 );
            sim_A_B = store( resultreg, cos_pi8_256( vertex.vector, next.vector ) );    require( approx( sim_A_B, target_sim, maxerr ) ); inc( R1 );
            sim_B_A = store( resultreg, cos_pi8_256( next.vector,   vertex.vector ) );  require( approx( sim_B_A, target_sim, maxerr ) ); inc( R1 );
            sim_B_B = store( resultreg, cos_pi8_256( next.vector,   next.vector ) );    require( approx( sim_B_B,  sim_ident, maxerr ) ); inc( R1 );


            true;
            """,
            memory = m,
            tail   = A,
            head   = B )

        result = m[0]
        target = m[1]
        Expect( match, "target={}, got {} for test number {} ".format( target, result, m.R1 ) )





def TEST_Vector_centroid():
    """
    pyvgx.Vector() centroid
    test_level=3101
    t_nominal=8
    """

    # Empty centroid
    E = graph.sim.NewCentroid( [] )
    Expect( E.length == 0,              "nullvector" )

    # Centroid of single vector with all zeros
    C = graph.sim.NewCentroid( [ [0]*SZ_STEP ] )
    Expect( C.external == [0.0]*SZ_STEP,     "all zeros" )

    # Centroid of more and more vectors
    L = []
    for n in range( 1000 ):
        L.append( [2*random.random()-1 for i in range(128)] )
        C = graph.sim.NewCentroid( L )

    # Centroid of more and more vectors with different lengths
    L = []
    for n in range( 1000 ):
        STEPS = [SZ_STEP*i for i in range(1,5)]
        vlen = random.sample( [32, 64, 96, 128], 1 )[0]
        L.append( [2*random.random()-1 for i in range(vlen)] )
        C = graph.sim.NewCentroid( L )



def TEST_Vector_exhaustive():
    """
    pyvgx.Vector() random vectors, all lengths
    test_level=3101
    t_nominal=93
    """

    VALID_RANGE = range( SZ_STEP, 65472+1, SZ_STEP )

    # All valid vector lengths
    for N in VALID_RANGE:
        print("{}".format(N), end=' ', flush=True)
        # N
        # Bipolar
        r1 = [2*random.random()-1 for i in range(N)]
        r2 = [2*random.random()-1 for i in range(N)]

        # Reference cosine
        ref_cos = Cosine( r1, r2 )

        # Normalize
        for scale in [0.1, 1, 10]:
            s1 = [scale*x for x in r1]
            s2 = [scale*x for x in r2]
            Vs1 = Vector( s1 )
            Vs2 = Vector( s2 )
            Expect( len(Vs1) == N,              "Length should be %d, got %d" % (N, len(Vs1)) )
            Expect( len(Vs2) == N,              "Length should be %d, got %d" % (N, len(Vs2)) )
            max_abs_s1 = max( (abs(x) for x in Vs1.external) )
            max_abs_s2 = max( (abs(x) for x in Vs2.external) )
            #Expect( max_abs_s1 == 1.0,          "Max abs component should be 1.0, got %f" % max_abs_s1 )
            #Expect( max_abs_s2 == 1.0,          "Max abs component should be 1.0, got %f" % max_abs_s2 )
            c = DefaultSimilarity.Cosine( Vs1, Vs2 )
            delta = abs( ref_cos - c )
            Expect( delta < 0.01,               "Cosine should be close to zero, got {} (scale={} N={})".format( c, scale, N ) )





def Run( name ):
    global graph
    pyvgx.system.Initialize( name, euclidean=True )
    try:
        graph = pyvgx.Graph( name )
        RunTests( [__name__] )
        graph.Close()
        del graph
        PerformCleanup()
    finally:
        pyvgx.system.Unload()
