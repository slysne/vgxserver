###############################################################################
# 
# VGX Server
# Distributed engine for plugin-based graph and vector search
# 
# Module:  pyvgx.test
# File:    Navigation.py
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



def make_random_graph(N):
    g = Graph("navigation")
    # Make a randomly connected graph
    for n in range(N):
        A = g.NewVertex(str(n))
        A.SetVector( g.sim.rvec(128) )
        for b in random.sample(range(100000), 32):
            if b != n:
                B = g.NewVertex(str(b))
                g.Connect(A, ("to", M_INT|M_FWDONLY, n), B)
                B.Close()
        A.Close()
    return g




###############################################################################
# TEST_navigation_vector
#
###############################################################################
def TEST_navigation_vector():
    """
    Test basic navigation with vector
    test_level=3201
    """

    g = make_random_graph(100000)

    # Pick a few entrypoints at random and search by vector similarity
    for n in random.sample(range(100000), 1000):
        entry = str(n)
        # Simple navigation
        result_1 = g.Neighborhood(
            id = entry,
            hits = 100,
            fields = F_VAL|F_ID|F_DEPTH,
            result = R_LIST,
            navigation = {
                'vector': g.sim.rvec(128)
            }
        )
        # Check result size
        Expect( len(result_1) == 100,       f"vector search should find 100 nodes, got {len(result_1)}" )
        # Check for duplicates
        S = set([id for score, id, depth in result_1])
        Expect( len(S) == len(result_1),    f"hits should be unique" )
        # Check sorted
        L = [score for score, id, depth in result_1]
        S = list(L)
        S.sort(reverse=1)
        Expect( L == S,                     f"scores should be sorted, descending" )
        # Check top score value in range
        Expect( L[0] > 1.0 and L[0] < 2.0,  f"top hit score should be in range (1.0, 2.0), got {L[0]}" )
        # Verify hits gathered at various depths
        D = set([depth for score, id, depth in result_1])
        Expect( len(D) > 6,                 f"expect hits from many different depths, got only {len(D)} different depths" )
        
    g.Truncate()



###############################################################################
# Run
#
###############################################################################
def Run( name ):
    """
    """
    RunTests( [__name__] )
