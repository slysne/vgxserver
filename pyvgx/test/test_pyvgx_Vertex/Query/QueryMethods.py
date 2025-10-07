###############################################################################
# 
# VGX Server
# Distributed engine for plugin-based graph and vector search
# 
# Module:  pyvgx.test
# File:    QueryMethods.py
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

graph = None




###############################################################################
# setup
#
###############################################################################
def setup( nterm=100, ninit=10 ):
    """
    """
    graph.Truncate()

    terminals = set()
    for term in range( nterm ):
        name = "term_%d" % term
        graph.Connect( "A", ("to",M_INT,term), name )
        terminals.add( name )
    
    initials = set()
    for init in range( ninit ):
        name = "init_%d" % init
        graph.Connect( name, ("to",M_INT,init), "A" )
        initials.add( name )

    both = set()
    both.update( terminals )
    both.update( initials )

    return terminals, initials, both




###############################################################################
# TEST_Neighborhood
#
###############################################################################
def TEST_Neighborhood():
    """
    pyvgx.Vertex.Neighborhood()
    test_level=3101
    """
    terminals, initials, both = setup()

    A = graph.OpenVertex( "A" )

    # Only terminals by default
    result = set( A.Neighborhood() )
    Expect( result == terminals )

    # Initials
    result = set( A.Neighborhood( D_IN ) )
    Expect( result == initials )

    # Both
    result = set( A.Neighborhood( D_ANY ) )
    Expect( result == both )

    graph.CloseVertex( A )




###############################################################################
# TEST_Adjacent
#
###############################################################################
def TEST_Adjacent():
    """
    pyvgx.Vertex.Adjacent()
    test_level=3101
    """
    terminals, initials, both = setup()

    A = graph.OpenVertex( "A" )

    Expect( A.Adjacent() == True )
    Expect( A.Adjacent( "to" ) == True )
    Expect( A.Adjacent( "no" ) == False )
    Expect( A.Adjacent( "to", "term_5" ) == True )
    Expect( A.Adjacent( "to", "init_5" ) == False )
    Expect( A.Adjacent( ("to",D_IN), "init_5" ) == True )

    graph.CloseVertex( A )




###############################################################################
# TEST_Aggregate
#
###############################################################################
def TEST_Aggregate():
    """
    pyvgx.Vertex.Aggregate()
    test_level=3101
    """
    NTERM = 1000
    NINIT = 2000
    terminals, initials, both = setup( nterm=NTERM, ninit=NINIT )

    A = graph.OpenVertex( "A" )

    Expect( A.Aggregate()['neighbors'] == len( terminals ) )
    Expect( A.Aggregate( D_IN )['neighbors'] == len( initials ) )
    Expect( A.Aggregate( D_ANY )['neighbors'] == len( both ) )

    Expect( A.Aggregate( ("to",D_OUT,M_INT) )['predicator_value'] == sum(range(NTERM)) )
    Expect( A.Aggregate( ("to",D_IN,M_INT) )['predicator_value'] == sum(range(NINIT)) )

    graph.CloseVertex( A )




###############################################################################
# TEST_ArcValue
#
###############################################################################
def TEST_ArcValue():
    """
    pyvgx.Vertex.ArcValue()
    test_level=3101
    """
    terminals, initials, both = setup()

    A = graph.OpenVertex( "A" )

    Expect( A.ArcValue( ("to",D_OUT), "term_5" ) == 5 )
    Expect( A.ArcValue( ("to",D_IN), "init_2" ) == 2 )

    graph.CloseVertex( A )




###############################################################################
# TEST_Degree
#
###############################################################################
def TEST_Degree():
    """
    pyvgx.Vertex.Degree()
    test_level=3101
    """
    NTERM = 50
    NINIT = 20
    terminals, initials, both = setup( nterm=NTERM, ninit=NINIT )

    A = graph.OpenVertex( "A" )

    Expect( A.Degree() == NTERM + NINIT )
    Expect( A.Degree( D_OUT ) == NTERM )
    Expect( A.Degree( D_IN ) == NINIT )

    graph.CloseVertex( A )




###############################################################################
# TEST_Inarcs
#
###############################################################################
def TEST_Inarcs():
    """
    pyvgx.Vertex.Inarcs()
    test_level=3101
    """
    terminals, initials, both = setup()

    A = graph.OpenVertex( "A" )

    Expect( len( A.Inarcs() ) == len( initials ) )
    arc = A.Inarcs()[0]
    Expect( len(arc) == 5 )
    Expect( arc[0] == "D_IN" )
    Expect( arc[1] == "to" )
    Expect( arc[2] == "M_INT" )
    Expect( type(arc[3]) is int )
    Expect( arc[4].startswith( "init_" ) )

    graph.CloseVertex( A )




###############################################################################
# TEST_Outarcs
#
###############################################################################
def TEST_Outarcs():
    """
    pyvgx.Vertex.Outarcs()
    test_level=3101
    """
    terminals, initials, both = setup()

    A = graph.OpenVertex( "A" )

    Expect( len( A.Outarcs() ) == len( terminals ) )
    arc = A.Outarcs()[0]
    Expect( len(arc) == 5 )
    Expect( arc[0] == "D_OUT" )
    Expect( arc[1] == "to" )
    Expect( arc[2] == "M_INT" )
    Expect( type(arc[3]) is int )
    Expect( arc[4].startswith( "term_" ) )

    graph.CloseVertex( A )




###############################################################################
# TEST_Initials
#
###############################################################################
def TEST_Initials():
    """
    pyvgx.Vertex.Initials()
    test_level=3101
    """
    terminals, initials, both = setup()

    A = graph.OpenVertex( "A" )

    Expect( set( A.Initials() ) == initials )
    Expect( set( A.Initials( filter='next.id == "init_*"' ) ) == initials )
    Expect( set( A.Initials( filter='next.id == "init_2"' ) ) == set(["init_2"]) )
    Expect( set( A.Initials( filter='next.id in {"init_2","init_4","term_6"}' ) ) == set(["init_2","init_4"]) )

    graph.CloseVertex( A )



###############################################################################
# TEST_Terminals
#
###############################################################################
def TEST_Terminals():
    """
    pyvgx.Vertex.Terminals()
    test_level=3101
    """
    terminals, initials, both = setup()

    A = graph.OpenVertex( "A" )

    Expect( set( A.Terminals() ) == terminals )
    Expect( set( A.Terminals( filter='next.id == "term_*"' ) ) == terminals )
    Expect( set( A.Terminals( filter='next.id == "term_2"' ) ) == set(["term_2"]) )
    Expect( set( A.Terminals( filter='next.id in {"term_2","term_4","init_6"}' ) ) == set(["term_2","term_4"]) )

    graph.CloseVertex( A )




###############################################################################
# TEST_Neighbors
#
###############################################################################
def TEST_Neighbors():
    """
    pyvgx.Vertex.Neighbors()
    test_level=3101
    """
    terminals, initials, both = setup()

    A = graph.OpenVertex( "A" )

    Expect( set( A.Neighbors() ) == both )
    Expect( set( A.Neighbors( D_OUT ) ) == terminals )
    Expect( set( A.Neighbors( D_IN ) ) == initials )

    Expect( set( A.Neighbors( D_OUT, filter='next.id == "term_*"' ) ) == terminals )
    Expect( set( A.Neighbors( D_IN,  filter='next.id == "init_*"' ) ) == initials )
    Expect( set( A.Neighbors( D_ANY, filter='next.id in {"init_*","term_*"}' ) ) == both )

    graph.CloseVertex( A )














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
