###############################################################################
# 
# VGX Server
# Distributed engine for plugin-based graph and vector search
# 
# Module:  pyvgx.test
# File:    SetProperty.py
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

from ast import Try
from pyvgxtest.pyvgxtest import RunTests, Expect, TestFailed
from pyvgx import *
import pyvgx
import random
import itertools
import math

graph = None


UNICODE = list(itertools.chain(range(0x20,0xd800),range(0xe000,0x110000)))

random.seed( 1234 )

UNI_1a = "".join( [chr(x) for x in random.sample(UNICODE,1)] )
UNI_1b = "".join( [chr(x) for x in random.sample(UNICODE,1)] )
UNI_10a = "".join( [chr(x) for x in random.sample(UNICODE,10)] )
UNI_10b = "".join( [chr(x) for x in random.sample(UNICODE,10)] )
UNI_100a = "".join( [chr(x) for x in random.sample(UNICODE,100)] )
UNI_100b = "".join( [chr(x) for x in random.sample(UNICODE,100)] )
UNI_21000a = "".join( [chr(x) for x in random.sample(UNICODE,21000)] )
UNI_21000b = "".join( [chr(x) for x in random.sample(UNICODE,21000)] )

SIZES = itertools.chain( range(1,520), range(520,MAX_STRING,953), range(MAX_STRING-17,MAX_STRING+17), (500000, 5000000, 50000000) )
RANDOM_STRING_KEYVAL = [ (f"random_key_{n}_{rstr(16)}", rstr(n)) for n in SIZES ]


###############################################################################
# float_eq
#
###############################################################################
def float_eq( a, b ):
    """
    Approximate float equivalence
    """
    return abs( a - b ) < 1e-10



###############################################################################
# some_function
#
###############################################################################
def some_function( a, b=1 ):
    """
    """
    return a + b



###############################################################################
# some_class
#
###############################################################################
class some_class( object ):
    """
    """
    def __init__( self, val ):
        self._v = val

    def __eq__( self, other ):
        return self._v == other._v


NODES = [f"Vertex {i+1} with Property" for i in range(5)]






###############################################################################
# TEST_vxvertex_property
#
###############################################################################
def TEST_vxvertex_property():
    """
    Core vxvertex_property
    test_level=501
    t_nominal=1
    """
    try:
        pyvgx.selftest( force=True, testroot="vgxtest", library="vgx", names=["vxvertex_property.c"] )
    except:
        Expect( False )



SETUP = False



###############################################################################
# TEST_SetProperty_setup
#
###############################################################################
def TEST_SetProperty_setup():
    """
    pyvgx.Vertex.SetProperty()
    Setup
    test_level=3101
    t_nominal=1
    """
    global SETUP
    if not SETUP:
        graph.Truncate()
        for NODE in NODES:
            V = graph.NewVertex( NODE, "prop_node" )
            Expect( V.HasProperties() is False,                           "Vertex should have no properties" )
            Expect( V.NumProperties() == 0,                               "Vertex should have no properties" )
            graph.CloseVertex( V )
        SETUP = True



POPULATED = False




###############################################################################
# vpropname
#
###############################################################################
def vpropname( name, virtual=False ):
    """
    """
    if virtual:
        return f"{name}__V"
    else:
        return name




###############################################################################
# assignvprop
#
###############################################################################
def assignvprop( name, virtual=False ):
    """
    """
    if virtual:
        vname = vpropname( name, virtual )
        return f"*{vname}"
    else:
        return name




###############################################################################
# TEST_SetProperty_populate
#
###############################################################################
def TEST_SetProperty_populate():
    """
    pyvgx.Vertex.SetProperty()
    Set various properties
    test_level=3101
    t_nominal=1
    """
    global POPULATED
    TEST_SetProperty_setup()

    Vlist = graph.OpenVertices( NODES, mode="a" )

    vn = 0
    for V in Vlist:
        vn += 1
        for vprop in [False, True]:

            # Set exists property
            V.SetProperty( vpropname('prop', vprop), virtual=vprop ) # value = None

            # Set boolean properties
            V.SetProperty( vpropname('true', vprop), True, virtual=vprop )
            V.SetProperty( vpropname('false', vprop), False, virtual=vprop )

            # Set integer properties
            V.SetProperty( vpropname('int_value1', vprop), 100+vn, virtual=vprop )
            V[assignvprop('int_value2', vprop)] = 200+vn

            # Set float properties
            V.SetProperty( vpropname('float_value1', vprop), 3.14+vn, virtual=vprop )
            V[assignvprop('float_value2', vprop)] = 2.71+vn

            # Set string properties
            V.SetProperty( vpropname('string_value1', vprop), f"hello {vn}", virtual=vprop )
            V[assignvprop('string_value2', vprop)] = f"hi {vn}"
            V[assignvprop('large_string1', vprop)] = "x" * 65470 + f"_middle_{vn}_" + "y" * 30
            try:
                V[assignvprop('large_string2', vprop)] = "x" * 100000 + f"_middle_{vn}_" + "y" * 100000
            except:
                Expect(0)

            # Set string properties of many sizes
            for k, v in RANDOM_STRING_KEYVAL:
                val = f"{v}-{vn}"
                if vprop or len(val) <= MAX_STRING:
                    try:
                        V.SetProperty( vpropname(k, vprop), val, virtual=vprop )
                    except:
                        Expect(0)

            # This string unique per vertex
            UNI_vn = f"{UNI_100a}-{vn}"

            # Unicode strings
            V.SetProperty( vpropname('uni_value1', vprop), UNI_1a, virtual=vprop )
            V[assignvprop('uni_value10', vprop)] = UNI_10a
            V[assignvprop('uni_value100', vprop)] = UNI_100a
            V[assignvprop('uni_value21000', vprop)] = UNI_21000a
            V[assignvprop('uni_vn', vprop)] = UNI_vn

            # UTF-8 strings
            V.SetProperty( vpropname('utf8_value1', vprop), UNI_1b.encode(), virtual=vprop )
            V[assignvprop('utf8_value10', vprop)] = UNI_10b.encode()
            V[assignvprop('utf8_value100', vprop)] = UNI_100b.encode()
            V[assignvprop('utf8_value21000', vprop)] = UNI_21000b.encode()
            V[assignvprop('utf8_vn', vprop)] = UNI_vn.encode()

            # Set other object properties (the repr() of the object will be stored
            V.SetProperty( vpropname('obj_value1', vprop), [1, "2", 3.0, {"x":"y", "vn":vn} ], virtual=vprop )

            # Set function properties
            V.SetProperty( vpropname('f_1', vprop), some_function, virtual=vprop )
            V[assignvprop('f_2', vprop)] = some_function
            # 
            #V[assignvprop('f_vn_plus_sq', vprop)] = lambda x: vn + x**2
            #V[assignvprop('f_vn_plus_sqrt', vprop)] = lambda x: vn + x**0.5
            # ^^^^^^^^^^^^^^^^^^^^ AMBD-7753 ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
            V[assignvprop('f_sin', vprop)] = math.sin

            # Set bytearray
            V.SetProperty( vpropname('ba0', vprop), bytearray( [] ), virtual=vprop )
            V.SetProperty( vpropname('ba1', vprop), bytearray( [1] ), virtual=vprop )
            V.SetProperty( vpropname('ba2', vprop), bytearray( [1,2] ), virtual=vprop )
            V.SetProperty( vpropname('ba3vn', vprop), bytearray( [1,2,vn] ), virtual=vprop )
            V.SetProperty( vpropname('ba63', vprop), bytearray( list(range(63)) ), virtual=vprop )
            V.SetProperty( vpropname('ba64', vprop), bytearray( list(range(64)) ), virtual=vprop )
            V.SetProperty( vpropname('ba65', vprop), bytearray( list(range(65)) ), virtual=vprop )
            V.SetProperty( vpropname('ba100', vprop), bytearray( list(range(100)) ), virtual=vprop )
            V.SetProperty( vpropname('ba60000', vprop), bytearray( [6]*60000 ), virtual=vprop )

            # Set bytes
            V.SetProperty( vpropname('by0', vprop), bytes( [] ), virtual=vprop )
            V.SetProperty( vpropname('by1', vprop), bytes( [1] ), virtual=vprop )
            V.SetProperty( vpropname('by2', vprop), bytes( [1,2] ), virtual=vprop )
            V.SetProperty( vpropname('by3vn', vprop), bytes( [1,2,vn] ), virtual=vprop )
            V.SetProperty( vpropname('by63', vprop), bytes( list(range(63)) ), virtual=vprop )
            V.SetProperty( vpropname('by64', vprop), bytes( list(range(64)) ), virtual=vprop )
            V.SetProperty( vpropname('by65', vprop), bytes( list(range(65)) ), virtual=vprop )
            V.SetProperty( vpropname('by100', vprop), bytes( list(range(100)) ), virtual=vprop )
            V.SetProperty( vpropname('by256', vprop), bytes( list(range(256)) ), virtual=vprop )
            V.SetProperty( vpropname('by60000', vprop), bytes( [6]*60000 ), virtual=vprop )
            V.SetProperty( vpropname('by100000', vprop), bytes( [0]*100000 ), virtual=vprop )

            # Set native arrays
            for n in range(100):
                V[ assignvprop('int_array_%d' % n, vprop) ] = list(range(n)) + [1000*vn]
                V[ assignvprop('float_array_%d' % n, vprop) ] = list(map( float, list(range(n)) + [float(1000*vn)] ))

            # Test numeric array limits
            max_array = 131000 // 8 if not vprop else -1
            something_big = 5555555 + vn
            V[ assignvprop('max_array', vprop) ] = list(range( max_array if max_array > 0 else something_big ))
            if max_array > 0:
                try:
                    L = list( range( max_array + 1 ) )
                    V[ assignvprop('too_big_array', vprop) ] = L
                    Expect( False, f"array size {len(L)} is too big and should not have been assigned" )
                except ValueError:
                    pass
                except:
                    Expect( False, "array is too big and should not have been assigned" )

            # Set native maps
            for n in range(1, 2000, 10):
                V[ assignvprop('map_%d' % n, vprop) ] = dict.fromkeys( list(range(n)) + [5000*vn], n )

            # Test native maps limits
            max_map = 5406 if not vprop else 21626 # Consult the code as to why these limits...
            V[ assignvprop('max_map',vprop) ] = dict.fromkeys( list(range( max_map )), 0.0 )
            try:
                V[ assignvprop('too_big_map', vprop) ] = dict.fromkeys( list(range( max_map + 1 )), 0.0 )
                Expect( False, "map is too big and should not have been assigned" )
            except ValueError:
                pass
            except:
                Expect( False, "map is too big and should not have been assigned" )
        
    # Close vertices
    graph.CloseVertices( Vlist )
    POPULATED = True




###############################################################################
# TEST_SetProperty_verify
#
###############################################################################
def TEST_SetProperty_verify():
    """
    pyvgx.Vertex.SetProperty()
    Verify properties
    test_level=3101
    t_nominal=4
    """
    TEST_SetProperty_setup()
    if not POPULATED:
        TEST_SetProperty_populate()


    Vlist = graph.OpenVertices( NODES, mode="r" )

    vn = 0
    for V in Vlist:
        vn += 1
        print( f"Checking vn={vn} {V.id}" )
        Expect( V['true'] == 1,                                     "true should be 1" )
        Expect( V.GetProperty( 'true' ) == 1,                       "true should be 1" )
        Expect( V.HasProperty( 'true' ),                            "true is a property" )
        Expect( V['false'] == 0,                                    "false should be 0" )
        Expect( V.GetProperty( 'false' ) == 0,                      "false should be 0" )

        # pick the nth hit to match vertex we're testing
        i = vn-1
        rdict_DICT_all = graph.Vertices( sortby=S_ID, fields=F_PROP, result=R_DICT )[i]['properties']
        rdict_DICT_select_all = graph.Vertices( sortby=S_ID, select="*", result=R_DICT )[i]['properties']
        rdict_LIST_all = dict( graph.Vertices( sortby=S_ID, fields=F_PROP, result=R_LIST )[i][0] )
        rdict_LIST_select_all = dict( graph.Vertices( sortby=S_ID, select="*", result=R_LIST )[i][0] )
        rdict_STR_all = graph.Vertices( sortby=S_ID, fields=F_PROP, result=R_STR )[i]
        rdict_STR_select_all = graph.Vertices( sortby=S_ID, select="*", result=R_STR )[i]

        #
        for P, info in [ (V, f"{V.id}"),
                   (rdict_DICT_all, f"R_DICT F_PROP result {i}"),
                   (rdict_DICT_select_all, f"R_DICT select * result {i}"),
                   (rdict_LIST_all, f"R_LIST F_PROP result {i}"),
                   (rdict_LIST_select_all, f"R_LIST select * result {i}") ]:
            for vprop in [False, True]:
                print( f"Testing all properties in {info} vprop={vprop}" )

                # Verify exists property
                key = vpropname('prop', vprop)
                Expect( P[key] in [None, 1],                                f"'{key}' should be None or 1" )

                # Verify boolean properties
                key = vpropname('true', vprop)
                Expect( P[key] == 1,                                        f"'{key}' should be 1" )
                key = vpropname('false', vprop)
                Expect( P[key] == 0,                                        f"'{key}' should be 0" )

                # Verify integer properties
                key = vpropname('int_value1', vprop)
                xval = 100+vn
                Expect( P[key] == xval,                                     f"'{key}' should be {xval}" )
                xval = 200+vn
                key = vpropname('int_value2', vprop)
                Expect( P[key] == xval,                                     f"'{key}' should be {xval}" )

                # Verify float properties
                key = vpropname('float_value1', vprop)
                xval = 3.14+vn
                Expect( float_eq( P[key], xval ),                           f"'{key}' should be {xval}" )
                key = vpropname('float_value2', vprop)
                xval = 2.71+vn
                Expect( float_eq( P[key], xval ),                           f"'{key}' should be {xval}" )

                # Verify string properties
                key = vpropname('string_value1', vprop)
                xval = f"hello {vn}"
                Expect( P[key] == xval,                                     f"'{key}' should be '{xval}'" )
                key = vpropname('string_value2', vprop)
                xval = f"hi {vn}"
                Expect( P[key] == xval,                                     f"'{key}' should be '{xval}'" )
                key = vpropname('large_string1', vprop)
                s1, s2, s3, s4 = P[key].split("_")
                Expect( s1 == "x" * 65470 and s2 == "middle" and s3 == str(vn) and s4 == "y" * 30,      f"'{key}' should match original" )
                key = vpropname('large_string2', vprop)
                s1, s2, s3, s4 = P[key].split("_")
                Expect( s1 == "x" * 100000 and s2 == "middle" and s3 == str(vn) and s4 == "y" * 100000, f"'{key}' should match original" )

                for k, v in RANDOM_STRING_KEYVAL:
                    val = f"{v}-{vn}"
                    if vprop or len(val) <= MAX_STRING:
                        key = vpropname(k, vprop)
                        Expect( P[key] == val,                              f"'{key}' should be '{val}'" )

                # This string unique per vertex
                UNI_vn = f"{UNI_100a}-{vn}"

                # Verify unicode string properties
                key = vpropname('uni_value1', vprop)
                Expect( P[key] == UNI_1a,                                   f"'{key}' should be '{UNI_1a}'" )
                key = vpropname('uni_value10', vprop)
                Expect( P[key] == UNI_10a,                                  f"'{key}' should be '{UNI_10a}'" )
                key = vpropname('uni_value100', vprop)
                Expect( P[key] == UNI_100a,                                 f"'{key}' should be '{UNI_100a}'" )
                key = vpropname('uni_value21000', vprop)
                Expect( P[key] == UNI_21000a,                               f"'{key}' should be '{UNI_21000a}'" )
                key = vpropname('uni_vn', vprop)
                Expect( P[key] == UNI_vn,                                   f"'{key}' should be '{UNI_vn}'" )

                # Verify utf-8 string properties
                key = vpropname('utf8_value1', vprop)
                Expect( P[key] == UNI_1b.encode(),                          f"'{key}' should be '{UNI_1b.encode()}'" )
                key = vpropname('utf8_value10', vprop)
                Expect( P[key] == UNI_10b.encode(),                         f"'{key}' should be '{UNI_10b.encode()}'" )
                key = vpropname('utf8_value100', vprop)
                Expect( P[key] == UNI_100b.encode(),                        f"'{key}' should be '{UNI_100b.encode()}'" )
                key = vpropname('utf8_value21000', vprop)
                Expect( P[key] == UNI_21000b.encode(),                      f"'{key}' should be '{UNI_21000b.encode()}'" )
                key = vpropname('utf8_vn', vprop)
                Expect( P[key] == UNI_vn.encode(),                          f"'{key}' should be '{UNI_vn.encode()}'" )

                # Verify object properties
                key = vpropname('obj_value1', vprop)
                Expect( P[key] == [1, "2", 3.0, {"x":"y", "vn":vn} ],       f"'{key}' should be Python list of objects" )

                # Verify function
                key = vpropname('f_1', vprop)
                Expect( P[key]( 3, 4 ) == some_function( 3, 4 ),            f"'{key}' should be some_function" )
                key = vpropname('f_2', vprop)
                Expect( P[key]( 3 ) == some_function( 3 ),                  f"'{key}' should be some_function")
                #key = vpropname('f_vn_plus_sq', vprop)
                #Expect( P[key]( 5 ) == vn + 5**2,                           f"'{key}' should compute vn + square" )
                #key = vpropname('f_vn_plus_sqrt', vprop)
                #Expect( P[key]( 5 ) == vn + 5**0.5,                         f"'{key}' should compute vn + square root" )
                # ^^^^^^^^^^^^^^^^^^^^ AMBD-7753 ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
                key = vpropname('f_sin', vprop)
                Expect( P[key]( 0.1 ) == math.sin( 0.1 ),                   f"'{key}' should compute sin()" )

                # Verify bytearray
                key = vpropname('ba0', vprop)
                Expect( P[key] == bytearray( [] ),                    "0 elements" )
                key = vpropname('ba1', vprop)
                Expect( P[key] == bytearray( [1] ),                   "1 element" )
                key = vpropname('ba2', vprop)
                Expect( P[key] == bytearray( [1,2] ),                 "2 elements" )
                key = vpropname('ba3vn', vprop)
                Expect( P[key] == bytearray( [1,2,vn] ),              "3 elements" )
                key = vpropname('ba63', vprop)
                Expect( P[key] == bytearray( list(range(63)) ),      "63 elements" )
                key = vpropname('ba64', vprop)
                Expect( P[key] == bytearray( list(range(64)) ),      "64 elements" )
                key = vpropname('ba65', vprop)
                Expect( P[key] == bytearray( list(range(65)) ),      "65 elements" )
                key = vpropname('ba100', vprop)
                Expect( P[key] == bytearray( list(range(100)) ),    "100 elements" )
                key = vpropname('ba60000', vprop)
                Expect( P[key] == bytearray( [6]*60000 ),         "60000 elements" )

                # Verify bytes
                key = vpropname('by0', vprop)
                Expect( P[key] == bytes( [] ),                               "0 elements" )
                key = vpropname('by1', vprop)
                Expect( P[key] == bytes( [1] ),                              "1 element" )
                key = vpropname('by2', vprop)
                Expect( P[key] == bytes( [1,2] ),                            "2 elements" )
                key = vpropname('by3vn', vprop)
                Expect( P[key] == bytes( [1,2,vn] ),                         "3 elements" )
                key = vpropname('by63', vprop)
                Expect( P[key] == bytes( list(range(63)) ),                 "63 elements" )
                key = vpropname('by64', vprop)
                Expect( P[key] == bytes( list(range(64)) ),                 "64 elements" )
                key = vpropname('by65', vprop)
                Expect( P[key] == bytes( list(range(65)) ),                 "65 elements" )
                key = vpropname('by100', vprop)
                Expect( P[key] == bytes( list(range(100)) ),               "100 elements" )
                key = vpropname('by256', vprop)
                Expect( P[key] == bytes( list(range(256)) ),               "100 elements" )
                key = vpropname('by60000', vprop)
                Expect( P[key] == bytes( [6]*60000 ),                    "60000 elements" )
                key = vpropname('by100000', vprop)
                Expect( P[key] == bytes( [0]*100000 ),                  "100000 elements" )

                
                # Verify native arrays
                for n in range( 100 ):
                    int_key = vpropname(f"int_array_{n}", vprop)
                    float_key = vpropname(f"float_array_{n}", vprop)

                    Expect( P[int_key] == list(range(n)) + [1000*vn],                               f"'{int_key}' should contain {n} integers" )
                    Expect( P[float_key] == list(map( float, list(range(n)) + [float(1000*vn)] )),  f"'{float_key}' should contain {n} floats" )

                    isarray_int_array = f"isarray( vertex['{int_key}'] )"
                    isarray_float_array = f"isarray( vertex['{float_key}'] )"

                    Expect( graph.Evaluate( isarray_int_array, tail=V ),      f"native '{int_key}' should be array type with {n} items" )
                    Expect( graph.Evaluate( isarray_float_array, tail=V ),    f"native '{float_key}' should be array type with {n} items" )

                    for x in range(n):
                        in_array_int_array = f"{x} in vertex['{int_key}']"
                        Expect( graph.Evaluate( in_array_int_array, head=V ) == 1,   f"native integer array '{int_key}' of size {n} should contain {x}" )
                    in_array_int_array = f"{n} in vertex['{int_key}']"
                    Expect( graph.Evaluate( in_array_int_array, head=V ) == 0,       f"native integer array '{int_key}' of size {n} should not contain {n}" )
                    in_array_int_array = f"{1000*vn} in vertex['{int_key}']"
                    Expect( graph.Evaluate( in_array_int_array, head=V ) == 1,   f"native integer array '{int_key}' of size {n} should contain {1000*vn}" )

                    for x in range(n):
                        in_array_float_array = f"{float(x)} in vertex['{float_key}']"
                        Expect( graph.Evaluate( in_array_float_array, head=V ) == 1,   f"native float array '{float_key}' of size {n} should contain {float(x)}" )
                    in_array_float_array = f"{float(n)} in vertex['{float_key}']"
                    Expect( graph.Evaluate( in_array_float_array, head=V ) == 0,       f"native float array '{float_key}' of size {n} should not contain {float(n)}" )
                    in_array_float_array = f"{float(1000*vn)} in vertex['{float_key}']"
                    Expect( graph.Evaluate( in_array_float_array, head=V ) == 1,   f"native float array '{float_key}' of size {n} should contain {float(1000*vn)}" )

                too_big_array_key = vpropname('too_big_array', vprop)
                Expect( too_big_array_key not in P,              f"'{too_big_array_key}' should not exist" )

                # Verify native maps
                for n in range( 1, 2000, 10 ):
                    map_key = vpropname(f"map_{n}", vprop)
                    for x in range(n):
                        in_map = f"{x} in vertex['{map_key}']"
                        Expect( graph.Evaluate( in_map, head=V ) == 1,      f"native map '{map_key}' of size {n} should contain key {x}" )
                    in_map = f"4000 in vertex['{map_key}']"
                    Expect( graph.Evaluate( in_map, head=V ) == 0,          f"native map '{map_key}' of size {n} should not contain key 4000" )
                    in_map = f"{5000*vn} in vertex['{map_key}']"
                    Expect( graph.Evaluate( in_map, head=V ) == 1,      f"native map '{map_key}' of size {n} should contain key {5000*vn}" )
             
                too_big_map_key = vpropname('too_big_map', vprop)
                Expect( too_big_map_key not in P,                f"'{too_big_map_key}' should not exist" )

        for vprop in [False, True]:
            # Select a few
            
            S = ["int_value1", "string_value1", "large_string1", "uni_vn", "utf8_vn"]
            select = ", ".join( [vpropname(name, vprop) for name in S] )

            i = vn-1
            rdict_DICT_select = graph.Vertices( sortby=S_ID, select=select, result=R_DICT )[i]['properties']
            rdict_LIST_select = dict( graph.Vertices( sortby=S_ID, select=select, result=R_LIST )[i][0] )
            rdict_STR_select = graph.Vertices( sortby=S_ID, select=select, result=R_STR )[i]

            for P, info in [(rdict_DICT_select, f"R_DICT result {i}"), (rdict_LIST_select, f"R_LIST result {i}")]:

                print( f"Testing selected properties in {info} vprop={vprop}" )

                # Verify integer properties
                key = vpropname('int_value1', vprop)
                xval = 100+vn
                aval = P[key]
                Expect( aval == xval,                                     f"'{key}' should be {xval}, got {aval}" )

                # Verify string properties
                key = vpropname('string_value1', vprop)
                xval = f"hello {vn}"
                aval = P[key]
                Expect( aval == xval,                                     f"'{key}' should be '{xval}', got '{aval}'" )

                key = vpropname('large_string1', vprop)
                aval = P[key]
                s1, s2, s3, s4 = aval.split("_")
                Expect( s1 == "x" * 65470 and s2 == "middle" and s3 == str(vn) and s4 == "y" * 30,      f"'{key}' should match original, got s2='{s2}' s3='{s3}'" )


                # This string unique per vertex
                UNI_vn = f"{UNI_100a}-{vn}"

                key = vpropname('uni_vn', vprop)
                aval = P[key]
                Expect( aval == UNI_vn,                                   f"'{key}' should be '{UNI_vn}', got '{aval}'" )

                key = vpropname('utf8_vn', vprop)
                aval = P[key]
                Expect( aval == UNI_vn.encode(),                          f"'{key}' should be '{UNI_vn.encode()}', got '{aval}'" )



    # Close vertices
    graph.CloseVertices( Vlist )





###############################################################################
# TEST_SetProperty_overwrite
#
###############################################################################
def TEST_SetProperty_overwrite():
    """
    pyvgx.Vertex.SetProperty()
    Overwrite various properties
    test_level=3101
    t_nominal=1
    """
    TEST_SetProperty_setup()

    Vlist = graph.OpenVertices( NODES, mode="a" )

    for V in Vlist:
        for n in range(10):
            for name in [ 'x', 'y', 'z' ]:
                for tp in [int, float, str, some_class]:
                    write = tp(n)
                    V[ name ] = write
                    read = V[ name ]
                    Expect( read == write,           f"property should be overwritten, expected {write}, got {read}" )

    
    # Close vertices
    graph.CloseVertices( Vlist )




###############################################################################
# TEST_SetProperty_many
#
###############################################################################
def TEST_SetProperty_many():
    """
    pyvgx.Vertex.SetProperty()
    Set a large number of properties
    test_level=3102
    t_nominal=1
    """
    TEST_SetProperty_setup()


    # Create vertices
    for tp in [int, float, str, some_class]:
        graph.CreateVertex( f"many_{tp.__name__}_props" )


    # Set many
    for tp in [int, float, str, some_class]:
        tpname = tp.__name__
        V = graph.OpenVertex( f"many_{tpname}_props", mode="a" )

        # Populate
        for n in range( 10000 ):
            name = f"{tpname}_{n}"
            V[ name ] = tp( n )

        # Close vertex
        graph.CloseVertex( V )


    # Verify many
    for tp in [int, float, str, some_class]:
        tpname = tp.__name__
        V = graph.OpenVertex( f"many_{tpname}_props", mode="r" )

        # Verify
        for n in range( 10000 ):
            name = f"{tpname}_{n}"
            expected = tp( n )
            read = V[ name ]
            Expect( read == expected,       f"property value should be {expected}, got {read}" )

        # Close vertex
        graph.CloseVertex( V )


    # Remove vertices
    for tp in [int, float, str, some_class]:
        graph.DeleteVertex( f"many_{tp.__name__}_props" )




###############################################################################
# TEST_SetProperty_sizes
#
###############################################################################
def TEST_SetProperty_sizes():
    """
    pyvgx.Vertex.SetProperty()
    Many string sizes
    t_nominal=15
    test_level=3102
    """
    TEST_SetProperty_setup()

    V = graph.OpenVertex( NODES[0], mode="a" )

    for n in range( 100000 ):
        val = "x" * n
        V['p'] = val
        read = V['p']
        Expect( read == val, f"same value, got {read} == {val}" )
    V['p'] = ''

    # Close vertex
    graph.CloseVertex( V )




###############################################################################
# TEST_SetProperty_virtual
#
###############################################################################
def TEST_SetProperty_virtual():
    """
    pyvgx.Vertex.SetProperty()
    Virtual (disk) properties
    t_nominal=15
    test_level=3102
    """
    pass







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
