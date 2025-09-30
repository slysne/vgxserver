###############################################################################
# 
# VGX Server
# Distributed engine for plugin-based graph and vector search
# 
# Module:  pyvgxtest
# File:    Aggregate.py
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
from . import _query_test_support as QuerySupport
from pyvgx import *
import pyvgx




###############################################################################
# TEST_vxquery_aggregator
#
###############################################################################
def TEST_vxquery_aggregator():
    """
    Core vxquery_aggregator
    test_level=501
    """
    try:
        pyvgx.selftest( force=True, testroot="vgxtest", library="vgx", names=["vxquery_aggregator.c" ] )
    except:
        Expect( False )




###############################################################################
# TEST_Aggregate
#
###############################################################################
def TEST_Aggregate():
    """
    pyvgx.Graph.Aggregate()
    t_nominal=103
    test_level=3201
    """
    
    levels = 2
    for fanout_factor in [1,10,100,1000]:
        for modifiers in [ [M_INT], [M_INT,M_UINT], [M_FLT], [M_INT,M_FLT], [M_INT,M_UINT,M_FLT] ]:
            g = QuerySupport.NewFanout( "fanout", "aggregate_root", fanout_factor=fanout_factor, levels=levels, modifiers=modifiers )
            root = "aggregate_root"

            for READONLY in [False, True, False, True]:
                if READONLY:
                    g.SetGraphReadonly( 60000 )

                #
                # Single level
                #

                # No args
                agg = g.Aggregate( root )
                Expect( len(agg) == 2,                                  "only two fields" )
                Expect( agg['neighbors'] == fanout_factor,              "root has %d neighbors" % fanout_factor )
                narcs = len( modifiers ) * fanout_factor
                Expect( agg['arcs'] == len(modifiers) * fanout_factor,  "root has %d outarcs" % narcs )

                # Raise exception when no modifier specified
                try:
                    g.Aggregate( root, fields=F_VAL )
                    Expect( False, "should raise exception" )
                except pyvgx.QueryError:
                    Expect( True )
                except Exception as ex:
                    Expect( False, "unexpected exception: %s" % ex )

                # indegree
                agg = g.Aggregate( root, arc=("to_level_1", D_OUT, M_INT), fields=F_IDEG )
                Expect( len(agg) == 3,                                  "three fields" )
                if agg['indegree'] < agg['arcs']:
                    print(agg['indegree'], agg['arcs'])
                Expect( agg['indegree'] >= agg['arcs'],                 "indegree >= arcs" )
                
                # outdegree
                agg = g.Aggregate( root, arc=("to_level_1", D_OUT, M_INT), fields=F_ODEG )
                Expect( len(agg) == 3,                                  "three fields" )
                Expect( agg['outdegree'] >= 0,                          "outdegree >= 0" )

                # degrees
                agg = g.Aggregate( root, arc=("to_level_1", D_OUT, M_INT), fields=F_DEGREES )
                Expect( len(agg) == 5,                                  "three fields" )
                Expect( agg['degree'] == agg['indegree'] + agg['outdegree'],  "degree == indegree + outdegre" )

                # predicator values and mixed
                for F_x in [F_NONE, F_VAL, F_ALL]:
                    if M_INT in modifiers:
                        agg = g.Aggregate( root, arc=("to_level_1", D_OUT, M_INT) )
                        Expect( agg['neighbors'] == fanout_factor,              "root has %d neighbors" % fanout_factor )
                        Expect( agg['arcs'] == fanout_factor,                   "root has %d outarcs with modifier M_INT" % fanout_factor )
                        valsum = sum( range( fanout_factor ) )
                        Expect( agg['predicator_value'] == valsum,              "root's aggregated arc values = %d" % valsum )
                    else:
                        agg = g.Aggregate( root, arc=("to_level_1", D_OUT, M_INT) )
                        Expect( agg['neighbors'] == agg['arcs'] == 0,           "root has not arc with modifier M_INT" )


                # Multi level
                for MOD in modifiers:
                    # 2nd filter but no 2nd collect
                    agg = g.Aggregate( root, arc=("to_level_1", D_OUT, MOD), neighbor={ 'arc':( "to_level_2", D_OUT, MOD ) } )
                    Expect( agg['neighbors'] == fanout_factor,              "root has %d neighbors" % fanout_factor )
                    Expect( agg['arcs'] == fanout_factor,                   "root has %d outarcs with modifier %d" % (fanout_factor, MOD) )
                    valsum = sum( range( fanout_factor ) )
                    Expect( agg['predicator_value'] == valsum,              "root's aggregated arc values = %d" % valsum )
                    # 2nd filter with collect
                    agg = g.Aggregate( root, arc=("to_level_1", D_OUT, MOD), neighbor={ 'arc':( "to_level_2", D_OUT, MOD ), 'collect':C_COLLECT } )
                    vertices = fanout_factor + 2 * (fanout_factor ** 2)
                    Expect( agg['neighbors'] == vertices,                   "root has %d extended neighbors" % vertices )
                    Expect( agg['arcs'] == vertices,                        "root has %d extended neighborhood arcs with modifier %d" % (vertices, MOD) )
                    valsum =  sum( range( fanout_factor ) ) + fanout_factor * sum( range( 2 * fanout_factor ) )
                    Expect( agg['predicator_value'] == valsum,              "root's extended neighborhood aggregated arc values = %d" % valsum )
                if M_INT in modifiers and M_FLT in modifiers:
                    # 2nd filter (with value condition), with collect filter and no immediate collect
                    cutoff = (2.0*fanout_factor) / 2
                    agg = g.Aggregate( root, arc=("to_level_1", D_OUT, M_INT), collect=C_NONE, neighbor={ 'arc':( "to_level_2", D_OUT, M_FLT, V_LT, cutoff), 'collect':("to_level_2", D_OUT, M_INT) } )
                    vertices = fanout_factor * (2 * fanout_factor / 2 )# No immediate neighborhood, and only 1/2 of 2nd neighborhood matches 2nd arc filter
                    Expect( agg['neighbors'] == vertices,                   "root has %d filtered extended neighbors" % vertices )
                    Expect( agg['arcs'] == vertices,                        "root has %d filtered extended neighborhood arcs with modifier %d" % (vertices, MOD) )
                    valsum = fanout_factor * sum( range( fanout_factor ) )
                    Expect( agg['predicator_value'] == valsum,              "root's filtered extended neighborhood aggregated arc values = %d" % valsum )

                if READONLY:
                    g.ClearGraphReadonly()

    g.Truncate()




###############################################################################
# Run
#
###############################################################################
def Run( name ):
    """
    """
    RunTests( [__name__] )
