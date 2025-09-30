###############################################################################
# 
# VGX Server
# Distributed engine for plugin-based graph and vector search
# 
# Module:  pyvgx.test
# File:    Query.py
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
from . import _query_test_support as QuerySupport



###############################################################################
# float_equals
#
###############################################################################
def float_equals( a, b ):
    """
    """
    return abs( a - b ) < 1e-6



###############################################################################
# DontCare
#
###############################################################################
class DontCare( str ):
    """
    """
    pass



###############################################################################
# __verify_attr
#
###############################################################################
def __verify_attr( Q, name, should_have, value=DontCare ):
    """
    """
    if should_have:
        try:
            val = getattr( Q, name )
            if value is not DontCare:
                Expect( val == value, "%s attribute '%s' should have value %s (%s), got %s (%s)" % (Q, name, value, type(value), val, type(val)) )
            return val
        except Exception as ex:
            Expect( False, "%s should have attribute '%s'" % (Q, name) )
    else:
        try:
            val = getattr( Q, name )
            Expect( False, "%s should not have attribute '%s', it has value '%d'" % (Q, name, val) )
        except AttributeError:
            pass
        except Exception as ex:
            Expect( False, "unexpected exception %s when accessing non-existent attribute '%s' for %s" % (ex, name, Q) )




###############################################################################
# TEST_vxquery_query
#
###############################################################################
def TEST_vxquery_query():
    """
    Core vxquery_query
    test_level=501
    """
    try:
        pyvgx.selftest( force=True, testroot="vgxtest", library="vgx", names=[ "vxquery_query.c" ] )
    except:
        Expect( False )




###############################################################################
# TEST_vxquery_probe
#
###############################################################################
def TEST_vxquery_probe():
    """
    Core vxquery_probe
    test_level=501
    """
    try:
        pyvgx.selftest( force=True, testroot="vgxtest", library="vgx", names=[ "vxquery_probe.c" ] )
    except:
        Expect( False )




###############################################################################
# TEST_vxquery_response
#
###############################################################################
def TEST_vxquery_response():
    """
    Core vxquery_response
    test_level=501
    """
    try:
        pyvgx.selftest( force=True, testroot="vgxtest", library="vgx", names=[ "vxquery_response.c" ] )
    except:
        Expect( False )




###############################################################################
# TEST_NeighborhoodQuery_basic
#
###############################################################################
def TEST_NeighborhoodQuery_basic():
    """
    pyvgx.Query
    NewNeighborhoodQuery
    Basic tests
    test_level=3101
    """
    g = Graph( "pyvgx.Query" )

    # No parameters
    Q = g.NewNeighborhoodQuery()

    __verify_attr( Q, "error",      True,   None )
    __verify_attr( Q, "reason",     True,   0 )
    __verify_attr( Q, "type",       True,   "Neighborhood" )
    __verify_attr( Q, "opid",       True,   0 )
    __verify_attr( Q, "texec",      True,   0.0 )
    __verify_attr( Q, "id",         False )
    #__verify_attr( Q, "hits",       True,   -1 )
    #__verify_attr( Q, "offset",     True,   0 )
    __verify_attr( Q, "memory",     False )
    #__verify_attr( Q, "timeout",    True,   0 )
    #__verify_attr( Q, "limexec",    True,   0 )

    try:
        Q.Execute()
        Expect( False, "%s should not execute without id" % (Q) )
    except KeyError:
        __verify_attr( Q, "error",  True )
        Expect( type( Q.error ) is KeyError )
        Expect( len(str(Q.error)) > 0 )
        Expect( Q.reason > 0 )
    except Exception as ex:
        Expect( False, "unexpected exception %s" % ex )


    # Add anchor id that does not exist in graph
    Q.id = "A"
    __verify_attr( Q, "id",         True,   "A" )
    try:
        Q.Execute()
        Expect( False, "%s with non-existent anchor should raise KeyError" % (Q) )
    except KeyError:
        __verify_attr( Q, "error",  True )
        Expect( "does not exist" in str(Q.error) )
        Expect( Q.reason > 0 )
    except Exception as ex:
        Expect( False, "unexpected exception %s" % ex )


    # Add arcs
    g.Connect( "A", "to", "B" )
    g.Connect( "A", "to", "C" )
    g.Connect( "A", "to", "D" )

    # Query should return three arcs
    result = Q.Execute()
    Expect( len(result) == 3 )
    Expect( "B" in result and "C" in result and "D" in result )
    __verify_attr( Q, "error", True, None )
    __verify_attr( Q, "reason", True, 0 )
    Expect( Q.opid > 0 )
    ALL = result

    # Specify offset and hits
    for offset in [0,1,2,3]:
        #Q.offset = offset
        for hits in [0,1,2,3]:
            #Q.hits = hits
            #__verify_attr( Q, "offset", True, offset )
            #__verify_attr( Q, "hits", True, hits )
            result = Q.Execute( offset=offset, hits=hits )
            Expect( result == ALL[ offset : offset+hits ] )


    # Test all attribute setters and getters
    Q = g.NewNeighborhoodQuery()
    
    # id
    __verify_attr( Q, "id",         False )
    Q.id = "A"
    Expect( Q.id == "A" )
    Q.id = g['A']
    Expect( Q.id == g['A'].internalid )
    try:
        Q.id = 123.4
        Expect( False, "should not be able to assign invalid vertex id" )
    except ValueError:
        Expect( Q.id == g['A'].internalid )
    except Exception as ex:
        Expect( False, "unexpected exception %s" % ex )
    
    # texec
    Expect( Q.texec == 0.0 )
    try:
        Q.texec = 1.23
    except AttributeError:
        pass # not writable
    except Exception as ex:
        Expect( False, "unexpected exception %s" % ex )

    # error
    Expect( Q.error is None )
    try:
        Q.error = "hello"
    except AttributeError:
        pass # not writable
    except Exception as ex:
        Expect( False, "unexpected exception %s" % ex )

    # reason
    Expect( Q.reason == 0 )
    try:
        Q.reason = 1
    except AttributeError:
        pass # not writable
    except Exception as ex:
        Expect( False, "unexpected exception %s" % ex )

    # type
    Expect( Q.type == "Neighborhood" )
    try:
        Q.type = "Adjacency"
    except AttributeError:
        pass # not writable
    except Exception as ex:
        Expect( False, "unexpected exception %s" % ex )

    # opid
    Expect( Q.opid == 0 )
    try:
        Q.opid = 1
    except AttributeError:
        pass # not writable
    except Exception as ex:
        Expect( False, "unexpected exception %s" % ex )

    # New query with memory and filter
    M = g.Memory( 128 )
    STRING = "This string should remain"
    M[0] = STRING
    M[1] = 123456789
    Q = g.NewNeighborhoodQuery( "A", memory=M, neighbor={'filter':"incif( load(0) == '%s' && load(1) == 123456789, R1)" % STRING} ) 
    Expect( len(M) == 128 )
    Expect( M[0] == STRING )
    Expect( M[1] == 123456789 )
    Expect( M[R1] == 0 )
    # Run once
    Q.Execute()
    Expect( M[0] == STRING )
    Expect( M[1] == 123456789 )
    Expect( M[R1] == 3 )
    # Run again
    Q.Execute()
    Expect( M[0] == STRING )
    Expect( M[1] == 123456789 )
    Expect( M[R1] == 6 )

    # Test execution time
    Q = g.NewNeighborhoodQuery( "A", neighbor={'filter':"cpukill(100000000)"}, result=R_TIMING )
    result = Q.Execute()
    Expect( 'time' in result )
    T = result['time']
    sr = T['search'] + T['result']
    Expect( sr > 0 and Q.texec > 0 )
    Expect( float_equals( sr, Q.texec ) )

    # Make a fully connected graph to test execution timeout
    for init in ["A", "B", "C", "D"]:
        for term in ["A", "B", "C", "D"]:
            g.Connect( init, "to", term )

    # Query will take a long time
    Q = g.NewNeighborhoodQuery( "A", collect=False, neighbor={ 'traverse':{ 'arc':D_OUT, 'collect':True, 'filter':"cpukill(50000000)" } } )
    #Expect( Q.timeout == 0 and Q.limexec == False )
    Expect( len(Q.Execute( hits=16 )) == 16 )
    # Set timeout only, to something much smaller than required time. Should complete since no acquisitions will block
    small_timeout = int( 1000 * Q.texec / 10 ) # One 10th (in milliseconds)
    Expect( len( Q.Execute( hits=16, timeout=small_timeout )) == 16 )
    # Set limexec to force timeout
    #Q.limexec = True
    try:
        Q.Execute( hits=16, timeout=small_timeout, limexec=True )
        Expect( False, "Query should time out when execution time is limited" )
    except SearchError:
        Expect( "timeout" in str(Q.error) )
        Expect( "%d" % small_timeout in str(Q.error) )
    except Exception as ex:
        Expect( False, "unexpected exception %s" % ex )

    # Set timeout to twice the expected max
    #Q.timeout = 20 * Q.timeout
    Q.Execute( hits=16, timeout=20*small_timeout, limexec=True )

    del Q

    g.Erase()








###############################################################################
# Run
#
###############################################################################
def Run( name ):
    """
    """
    RunTests( [__name__] )
