###############################################################################
# 
# VGX Server
# Distributed engine for plugin-based graph and vector search
# 
# Module:  pyvgx.test
# File:    Recursion.py
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
from . import _query_test_support as QuerySupport
from pyvgx import *
import pyvgx
import random



def make_random_graph(N):
    g = Graph("recursion")
    # Make a randomly connected graph
    for n in range(N):
        A = g.NewVertex(str(n))
        for b in random.sample(range(100000), 32):
            if b != n:
                B = g.NewVertex(str(b))
                g.Connect(A, ("to", M_INT|M_FWDONLY, n), B)
                B.Close()
        A.Close()
    return g



###############################################################################
# TEST_recursion_basic
#
###############################################################################
def TEST_recursion_basic():
    """
    Test basic recursion
    test_level=3201
    """

    g = make_random_graph(100000)

    # Pick a few entrypoints at random
    for n in random.sample(range(100000), 1000):
        entry = str(n)
        # Simple recursion
        result_1 = g.Neighborhood(
            id = entry,
            hits = 100,
            fields = F_VAL|F_ID|F_DEPTH,
            result = R_LIST,
            recursion = {}
        )
        # Check hit count
        Expect( len(result_1) == 100,     f"should at least collect 100 nodes, got {len(result_1)}" )
        # Check for duplicates
        S = set([id for score, id, depth in result_1])
        Expect( len(S) == len(result_1),  f"hits should be unique" )

        # Add recursion filter
        result_2 = g.Neighborhood(
            id = entry,
            hits = 100,
            fields = F_VAL|F_ID|F_DEPTH,
            result = R_LIST,
            recursion = {
                'filter': "startswith(vertex.id, '9')"
            }
        )
        # Check filter
        for score, id, depth in result_2:
            Expect( id.startswith('9'),     f"recursion filter should accept only nodes starting with '9', got {id}" )
        # Check for duplicates
        S = set([id for score, id, depth in result_2])
        Expect( len(S) == len(result_2),    f"hits should be unique" )

    g.Truncate()


###############################################################################
# Run
#
###############################################################################
def Run( name ):
    """
    """
    RunTests( [__name__] )
