###############################################################################
# 
# VGX Server
# Distributed engine for plugin-based graph and vector search
# 
# Module:  pyvgxtest
# File:    Similarity.py
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
import math
import sys
import time
import re


graph = None



###############################################################################
# getstr
#
###############################################################################
def getstr( N ):
    """
    """
    return "".join( [ chr(random.randint(ord('a'),ord('z'))) for n in range(N) ] )



###############################################################################
# getvec
#
###############################################################################
def getvec( N, C, common=[] ):
    """
    """
    V = common + [ (getstr( C ), random.randint(0,1875)/1000.0) for n in range(N) ]
    return sorted( V, key=lambda x:x[1], reverse=True )





###############################################################################
# TEST_vxsim_sim
#
###############################################################################
def TEST_vxsim_sim():
    """
    Core vxsim_sim
    test_level=501
    """
    try:
        pyvgx.selftest( force=True, testroot="vgxtest", library="vgx", names=["vxsim_sim.c"] )
    except:
        Expect( False )




###############################################################################
# TEST_vxsim_vector
#
###############################################################################
def TEST_vxsim_vector():
    """
    Core vxsim_vector
    test_level=501
    """
    try:
        pyvgx.selftest( force=True, testroot="vgxtest", library="vgx", names=["vxsim_vector.c"] )
    except:
        Expect( False )




###############################################################################
# TEST_vxsim_lsh
#
###############################################################################
def TEST_vxsim_lsh():
    """
    Core vxsim_lsh
    test_level=501
    """
    try:
        pyvgx.selftest( force=True, testroot="vgxtest", library="vgx", names=["vxsim_lsh.c"] )
    except:
        Expect( False )




###############################################################################
# TEST_vxsim_centroid
#
###############################################################################
def TEST_vxsim_centroid():
    """
    Core vxsim_centroid
    test_level=501
    """
    try:
        pyvgx.selftest( force=True, testroot="vgxtest", library="vgx", names=["vxsim_centroid.c"] )
    except:
        Expect( False )




###############################################################################
# TEST_Similarity
#
###############################################################################
def TEST_Similarity():
    """
    pyvgx.Similarity()
    Verify that pyvgx.Similarity() creates a new, correctly initialized pyvgx.Similarity object
    t_nominal=1
    test_level=3101
    """
    s = Similarity( graph )
    Expect( type(s) is Similarity )
    
    Expect( s.max_vector_size == 48 )
    Expect( s.sim_threshold == 1.0 )
    Expect( s.min_cosine == 0.0 )
    Expect( s.cosine_exp == 1.0 )
    Expect( s.min_jaccard == 0.0 )
    Expect( s.jaccard_exp == 0.0 )
    Expect( s.min_isect == 1 )




###############################################################################
# TEST_Similarity_centroid
#
###############################################################################
def TEST_Similarity_centroid():
    """
    pyvgx.Similarity.NewCentroid()
    t_nominal=32
    test_level=3101
    """
    sim = Similarity( graph )

    COMMON = [("Always", 2.0), ("Sometimes",0.5)]

    for n_vectors in [0, 1, 2, 5, 20, 100, 1000, 10000]:
        for vector_length in [1, 2, 5, 15, 50]:
            for dim_length in [1, 2, 5, 27]:
                print("n_vectors=%d vector_length=%d dim_length=%d" % (n_vectors, vector_length, dim_length))
                vector_list = [ sim.NewVector( getvec( vector_length, dim_length, common=COMMON ) ) for n in range( n_vectors ) ]
                centroid = sim.NewCentroid( vector_list )
                print(centroid)
                del centroid
                del vector_list





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
