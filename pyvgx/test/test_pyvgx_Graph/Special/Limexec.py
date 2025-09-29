###############################################################################
# 
# VGX Server
# Distributed engine for plugin-based graph and vector search
# 
# Module:  pyvgx
# File:    Limexec.py
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
# TEST_Limexec_basic
#
###############################################################################
def TEST_Limexec_basic():
    """
    Basic timeout with limited execution
    t_nominal=10
    test_level=3101
    """
    graph_name = "limexec_graph"
    g = Graph( graph_name )

    m = g.Memory( 16 )

    N1 = 100
    N2 = 100
    N3 = 100

    for i in range( N1 ):
        first = "first_%d" % i
        F = g.NewVertex( first )
        F['i'] = i
        g.Connect( "ROOT", ("first",M_INT,i), F )
        for j in range( N2 ):
            second = "second_%d_%d" % (i,j)
            S = g.NewVertex( second )
            S['j'] = j
            g.Connect( F, ("second",M_INT,i*j), S )
            for k in range( N3 ):
                third = "third_%d_%d_%d" % (i,j,k)
                T = g.NewVertex( third )
                T['k'] = k
                T['prod'] = i*j*k
                g.Connect( S, ("third",M_INT,i*j*k), T )

    del F
    del S
    del T

    query = {
        'id'       : "ROOT",
        'memory'   : m,
        'hits'     : 0,
        'arc'      : ("first", D_OUT, M_INT),
        'collect'  : C_SCAN,
        'neighbor' : {
            'filter'   : "do( store( 0, vertex['i'] ) )",
            'traverse' : {
                'collect'   : C_SCAN,
                'arc'       : ("second", D_OUT, M_INT),
                'neighbor'  : {
                    'filter'     : "do( store( 1, vertex['j'] ) )",
                    'traverse'   : {
                        'collect'   : C_SCAN,
                        'arc'      : ("third", D_OUT, M_INT),
                        'filter'   : "do( store( 2, next['k'] ) )",
                        'neighbor' : {
                            'filter'  : "incif( mprod( 3, 0, 2 ) == vertex['prod'], R1 )"
                        }
                    }
                }
            }
        }
    }

    t0 = timestamp()
    result = g.Neighborhood( **query )
    t1 = timestamp()
    t_baseline = t1 - t0

    #print(t_baseline)
    #print(m.R1)

    N = N1 * N2 * N3
    Expect( m.R1 == N, "expected %d, got %d" % (N, m.R1) )


    lim = 3
    max_lim = int((t_baseline * 1000) / 2)
    while lim < max_lim:
        query[ 'timeout' ] = lim
        query[ 'limexec' ] = True
        try:
            m.Reset()
            g.Neighborhood( **query )
            Expect( False, "should have timed out" )
        except SearchError as timeout_ex:
            print(timeout_ex, m.R1)
        except Exception as ex:
            Expect( False, "unexpected exception %s" % ex )
        lim = int( lim * 1.4142 )

    # Should complete
    lim = int((t_baseline * 1000) * 2)
    query[ 'timeout' ] = lim
    query[ 'limexec' ] = True
    m.Reset()
    g.Neighborhood( **query )
    Expect( m.R1 == N, "expected %d, got %d" % (N, m.R1) )


    g.Erase()




###############################################################################
# Run
#
###############################################################################
def Run( name ):
    """
    """
    RunTests( [__name__] )
