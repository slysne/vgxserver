###############################################################################
# 
# VGX Server
# Distributed engine for plugin-based graph and vector search
# 
# Module:  pyvgx
# File:    Wildcards.py
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

graph = None




def TEST_wildcard_escape():
    """
    Vertex names with asterisk
    test_level=3101
    """

    g = graph
    g.Truncate()

    Expect( g.Connect( "a*", "to", "b" ) == 1,              "(a*)-[to]->(b)" )
    Expect( g.Connect( "a",  "to", "b*" ) == 1,             "(a)-[to]->(b*)" )
    Expect( g.Connect( "a*", "to", "b*" ) == 1,             "(a*)-[to]->(b*)" )

    Expect( g.HasVertex( "a*" ),                            "a* in g" )
    Expect( g.HasVertex( "a" ),                             "a in g" )
    Expect( g.HasVertex( "b*" ),                            "b* in g" )
    Expect( g.HasVertex( "b" ),                             "b in g" )

    Expect( g.order == 4,                                   "4 vertices" )
    Expect( g.size == 3,                                    "3 arcs" )

    nodeset = set(['a','b','a*','b*'])
    V = set(g.Vertices())
    Expect( V == nodeset,                                   "%s, got %s" % (nodeset, V) )

    initset = set(['a','a*'])
    termset = set(['b','b*'])
    A = set(g.Arcs( fields=F_ANCHOR ))
    B = set(g.Arcs( fields=F_ID ))
    Expect( A == initset,                                   "%s, got %s" % (initset, A) )
    Expect( B == termset,                                   "%s, got %s" % (termset, B) )

    try:
        g.Neighborhood( "a*" )
        Expect( False,                                      "Wildcard in anchor should raises exception" )
    except pyvgx.QueryError as QE:
        Expect( str(QE) == "Wildcard not allowed in anchor: a*" )
    except Exception as EX:
        Expect( False,                                      "Unexpected exception %s" % str(EX) )


    # (a*)-[to]->(b)
    # (a*)-[to]->(b*)
    N = set(g.Neighborhood( r"a\*" ))
    Expect( N == termset,                                   "%s, got %s" % (termset, N) )

    # (a*)-[to]->(b)<-[to]-(a*)
    #            --------------
    # (a*)-[to]->(b)
    # --------------
    # (a*)-[to]->(b*)<-[to]-(a)
    #            --------------
    # (a*)-[to]->(b*)<-[to]-(a*)
    #            ---------------
    # (a*)<-[to]-(b*)
    # ---------------
    N = set(g.Neighborhood( r"a\*", neighbor={'collect':True}, fields=F_ANCHOR|F_ID, result=R_LIST ))
    multiset = set([
        ('b', 'a*'),
        ('a*', 'b'),
        ('b*', 'a'),
        ('b*', 'a*'),
        ('a*', 'b*')
    ])
    Expect( N == multiset,                                  "%s, got %s" % (multiset, N) )
    
    del g




def Run( name ):
    """
    Run the tests in this module
    """
    global graph
    graph = pyvgx.Graph( name )
    RunTests( [__name__] )
    graph.Close()
    del graph
