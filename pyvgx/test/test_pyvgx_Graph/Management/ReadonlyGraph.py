###############################################################################
# 
# VGX Server
# Distributed engine for plugin-based graph and vector search
# 
# Module:  pyvgx.test
# File:    ReadonlyGraph.py
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

graph = None



###############################################################################
# TEST_IsGraphReadonly
#
###############################################################################
def TEST_IsGraphReadonly():
    """
    pyvgx.Graph.IsGraphReadonly()
    Call method
    test_level=3101
    """
    graph.IsGraphReadonly()




###############################################################################
# TEST_ClearGraphReadonly
#
###############################################################################
def TEST_ClearGraphReadonly():
    """
    pyvgx.Graph.ClearGraphReadonly()
    Call method
    test_level=3101
    """
    graph.ClearGraphReadonly()




###############################################################################
# TEST_SetGraphReadonly
#
###############################################################################
def TEST_SetGraphReadonly():
    """
    pyvgx.Graph.SetGraphReadonly()
    test_level=3101
    """
    graph.Truncate()

    graph.CreateVertex( "A" )
    graph.CreateVertex( "B" )
    graph.CreateVertex( "C" )

    Expect( graph.Connect( "A", ("to",M_CNT,1), "B" ) == 1,         "Arc created" )
    Expect( graph.Count( "A", "to", "B" ) == 2,                     "Arc incremented" )

    Expect( graph.order == 3,   "Three vertices" )
    Expect( graph.size == 1,    "One arc" )

    graph.SetGraphReadonly( 60000 )

    try:
        graph.Connect( "B", "to", "C" )
        Expect( False,  "Should not be able to create arc in readonly mode" )
    except pyvgx.AccessError:
        Expect( graph.size == 1, "Still one arc in graph" )
    except:
        raise

    try:
        graph.Disconnect( "B" )
        Expect( False,  "Should not be able to delete arc in readonly mode" )
    except pyvgx.AccessError:
        Expect( graph.size == 1, "Still one arc in graph" )
    except:
        raise

    try:
        graph.Count( "A", "to", "B" )
        Expect( False,  "Should not be able to modify arc in readonly mode" )
    except pyvgx.AccessError:
        Expect( graph.size == 1, "Still one arc in graph" )
    except:
        raise

    try:
        graph.CreateVertex( "D" )
        Expect( False,  "Should not be able to create vertex in readonly mode" )
    except pyvgx.AccessError:
        Expect( graph.order == 3, "Still three vertices in graph" )
    except:
        raise

    try:
        graph.DeleteVertex( "C" )
        Expect( False,  "Should not be able to delete vertex in readonly mode" )
    except pyvgx.AccessError:
        Expect( graph.order == 3, "Still three vertices in graph" )
    except:
        raise

    try:
        graph.Truncate()
        Expect( False,  "Should not be able to truncate graph in readonly mode" )
    except pyvgx.AccessError:
        Expect( graph.order == 3, "Still three vertices in graph" )
        Expect( graph.size == 1, "Still one arc in graph" )
    except:
        raise
    
    Expect( set( graph.Neighborhood( "A" ) ) == set( ["B"] ),  "Queries allowed in readonly mode" )
    Expect( set( graph.Vertices() ) == set( ["A", "B", "C"] ), "Queries allowed in readonly mode" )

    graph.ClearGraphReadonly()

    Expect( graph.CreateVertex( "D" ) == 1,             "No longer readonly" )
    Expect( graph.Connect( "C", "to", "D" ) == 1,       "No longer readonly" )
    
    graph.Truncate()

    Expect( graph.order == 0 and graph.size == 0,       "Empty graph" )








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
