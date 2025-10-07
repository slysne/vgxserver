###############################################################################
# 
# VGX Server
# Distributed engine for plugin-based graph and vector search
# 
# Module:  pyvgx.test
# File:    VectorArithmetic.py
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
import operator
import random
import math
from functools import reduce


g = None
graph_name = "eval_graph"


quant = {
    512: 64,
    2 : 32,
    1 : 16,
    0 : 16 # No AVX, most likely ARM/Neon
}

sz_step = quant[ avxbuild() ]

SIZES = list(range(0, 4096, sz_step)) + list(range(4096, 64512, 1024)) + [ 65456 ]



###############################################################################
# TEST_Evaluator_dp_pi8_512
#
###############################################################################
def TEST_Evaluator_dp_pi8_512():
    """
    pyvgx Evaluator dp_pi8_512()
    test_level=3101
    """
    def dp( A, B ):
        if len(A) > 0:
            return float( reduce( operator.add, [operator.mul(*x) for x in zip( A, B )] ) )
        else:
            return 0.0

    m = g.Memory( 1024 )

    zeros = [ 0 ] * 65536
    Ra = [ random.randint(0,127) for n in range(65536) ]
    Rb = [ random.randint(0,127) for n in range(65536) ]
    Rc = [ int(random.random()>0.9) for n in range(65536) ]

    for n in SIZES:

        Z = zeros[:n]
        A = Ra[:n]
        B = Rb[:n]
        C = Rc[:n]

        m[0] = bytearray( Z )
        m[1] = bytearray( A )
        m[2] = bytearray( B )
        m[3] = bytearray( C )


        # Expected dot-product
        m.R1 = dp( A, B )
        m.R2 = dp( A, C )
        m.R3 = dp( B, C )

        Expect( g.Evaluate( "store( R4, dp_pi8_512( M[0], M[0] ) ) == 0",                   memory=m ),    "%.12f, got %.12f" % (0,    m.R4) )
        Expect( g.Evaluate( "store( R4, dp_pi8_512( M[0], M[1] ) ) == 0",                   memory=m ),    "%.12f, got %.12f" % (0,    m.R4) )
        Expect( g.Evaluate( "store( R4, dp_pi8_512( M[1], M[0] ) ) == 0",                   memory=m ),    "%.12f, got %.12f" % (0,    m.R4) )
        Expect( g.Evaluate( "abs(store( R4, dp_pi8_512( M[1], M[2] ) ) - r1) / r1 < 0.001", memory=m ),    "%.12f, got %.12f (error ratio: %g)" % (m.R1, m.R4, (m.R4 - m.R1)/(m.R1+1e-100)) )
        Expect( g.Evaluate( "abs(store( R4, dp_pi8_512( M[2], M[1] ) ) - r1) / r1 < 0.001", memory=m ),    "%.12f, got %.12f (error ratio: %g)" % (m.R1, m.R4, (m.R4 - m.R1)/(m.R1+1e-100)) )
        Expect( g.Evaluate( "abs(store( R4, dp_pi8_512( M[1], M[3] ) ) - r2) / r2 < 0.001", memory=m ),    "%.12f, got %.12f (error ratio: %g)" % (m.R2, m.R4, (m.R4 - m.R2)/(m.R2+1e-100)) )
        Expect( g.Evaluate( "abs(store( R4, dp_pi8_512( M[3], M[1] ) ) - r2) / r2 < 0.001", memory=m ),    "%.12f, got %.12f (error ratio: %g)" % (m.R2, m.R4, (m.R4 - m.R2)/(m.R2+1e-100)) )
        Expect( g.Evaluate( "abs(store( R4, dp_pi8_512( M[2], M[3] ) ) - r3) / r3 < 0.001", memory=m ),    "%.12f, got %.12f (error ratio: %g)" % (m.R3, m.R4, (m.R4 - m.R3)/(m.R3+1e-100)) )
        Expect( g.Evaluate( "abs(store( R4, dp_pi8_512( M[3], M[2] ) ) - r3) / r3 < 0.001", memory=m ),    "%.12f, got %.12f (error ratio: %g)" % (m.R3, m.R4, (m.R4 - m.R3)/(m.R3+1e-100)) )
        



###############################################################################
# TEST_Evaluator_ecld_pi8_512
#
###############################################################################
def TEST_Evaluator_ecld_pi8_512():
    """
    pyvgx Evaluator ecld_pi8_512()
    test_level=3101
    """
    def EuclideanDistance( A, B ):
        return math.sqrt( sum( [ ((a-b)/1)**2 for a, b in zip( A, B ) ] ) )

    m = g.Memory( 1024 )

    Ra = [127] + [ random.randint(0,127) for n in range(65536) ]
    Rb = [127] + [ random.randint(0,127) for n in range(65536) ]

    for n in SIZES:

        m[0] = bytearray( Ra[:n] )
        m[1] = bytearray( Rb[:n] )

        # Expected distance
        m.R4 = EuclideanDistance( m[0], m[1] )
        # Max relative error
        m.R3 = 0.001

        match = g.Evaluate(
            """
            target = r4;
            maxerr = r3;

            d_0_0 = store( R2, ecld_pi8_512( M[0], M[0] ) ); require( d_0_0 == 0.0 );                     inc( R1 );
            d_0_1 = store( R2, ecld_pi8_512( M[0], M[1] ) ); require( approx( d_0_1, target, maxerr ) );  inc( R1 );
            d_1_0 = store( R2, ecld_pi8_512( M[1], M[0] ) ); require( approx( d_1_0, target, maxerr ) );  inc( R1 );
            d_1_1 = store( R2, ecld_pi8_512( M[1], M[1] ) ); require( d_1_1 == 0.0 );                     inc( R1 );

            true;
            """,
            memory = m
        )

        Expect( match, "Failed after test: {} (got {} expected {}) ".format(m.R1, m.R2, m.R4) )




###############################################################################
# TEST_Evaluator_cos_pi8_512
#
###############################################################################
def TEST_Evaluator_cos_pi8_512():
    """
    pyvgx Evaluator cos_pi8_512()
    test_level=3101
    """
    def Cos( A, B ):
        if len(A) > 0:
            dp = float( reduce( operator.add, [operator.mul(*x) for x in zip( A, B )] ) )
            mA = math.sqrt( reduce( operator.add, [x**2 for x in A] ) )
            mB = math.sqrt( reduce( operator.add, [x**2 for x in B] ) )
            return dp / (mA*mB)
        else:
            return 0.0

    m = g.Memory( 1024 )

    Ra = [ random.randint(0,127) for n in range(65536) ]
    Rb = [ random.randint(0,127) for n in range(65536) ]

    for n in SIZES:

        A = Ra[:n]
        B = Rb[:n]

        m[0] = bytearray( A )
        m[1] = bytearray( B )

        # Expected cosine
        m.R4 = Cos( A, B )

        Expect( g.Evaluate( "abs( store( R3, cos_pi8_512( M[0], M[1] ) ) - r4 ) < 0.001", memory=m ),    "%.12f, got %.12f (delta %g)" % (m.R4, m.R3, m.R4-m.R3) )
        Expect( g.Evaluate( "abs( store( R3, cos_pi8_512( M[1], M[0] ) ) - r4 ) < 0.001", memory=m ),    "%.12f, got %.12f (delta %g)" % (m.R4, m.R3, m.R4-m.R3) )
    



###############################################################################
# Run
#
###############################################################################
def Run( name ):
    """
    """
    global g
    g = Graph( graph_name )

    RunTests( [__name__] )

    g.Erase()
