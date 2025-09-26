###############################################################################
# 
# VGX Server
# Distributed engine for plugin-based graph and vector search
# 
# Module:  pyvgx
# File:    Fields.py
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
from pytest.threads import Worker
import threading
from pyvgx import *
import pyvgx
from math import *
import re
import json
import random
import time

random.seed( 1000 )

graph = None


FIELDS = [              #   N   PRED?   ARC?
                        #  ---  -----   ----
    F_ANCHOR,           #   0            x
    F_ANCHOR_OBID,      #   1
    F_ARCDIR,           #   2     x      x
    F_REL,              #   3     x      x
    F_MOD,              #   4     x      x
    F_VAL,              #   5     x      x
    F_ID,               #   6            x
    F_OBID,             #   7
    F_TYPE,             #   8
    F_DEG,              #   9
    F_IDEG,             #  10
    F_ODEG,             #  11
    F_VEC,              #  12
    F_PROP,             #  13
    F_RANK,             #  14
    F_SIM,              #  15
    F_HAM,              #  16
    F_TMC,              #  17
    F_TMM,              #  18
    F_TMX,              #  19
    F_DESCR,            #  20
    F_ADDR,             #  21
    F_HANDLE,           #  22
    F_RAW               #  23
]


PRED_FIELDS = F_ARCDIR | F_REL | F_MOD | F_VAL
ARC_FIELDS = F_ANCHOR | PRED_FIELDS | F_ID
NON_ARC_FIELDS = F_ANCHOR_OBID | F_OBID | F_TYPE | F_DEG | F_IDEG | F_ODEG | F_VEC | F_PROP | F_RANK | F_SIM | F_HAM | F_TMC | F_TMM | F_TMX | F_DESCR | F_ADDR | F_HANDLE | F_RAW
ALL_FIELDS = sum( FIELDS )


LIST_ORDER = {
    F_ANCHOR        : 0,
    F_ANCHOR_OBID   : 1,
    F_ARCDIR        : 2,
    F_REL           : 3,
    F_MOD           : 4,
    F_VAL           : 5,
    F_ID            : 6,
    F_OBID          : 7,
    F_TYPE          : 8,
    F_DEG           : 9,
    F_IDEG          : 10,
    F_ODEG          : 11,
    F_VEC           : 12,
    F_PROP          : 13,
    F_RANK          : 14,
    F_SIM           : 15,
    F_HAM           : 16,
    F_TMC           : 17,
    F_TMM           : 18,
    F_TMX           : 19,
    F_DESCR         : 20,
    F_ADDR          : 21,
    F_HANDLE        : 22,
    F_RAW           : 23
}


ROOT_ID = "Anchor"
ROOT_OBID = strhash128( ROOT_ID )
NODEBASE = "node_"
LONGBASE = "This is a long node name which will require a CString to hold the identifier_"
LONGMOD = 13
SMALL_SZ = 260
L2_FANOUT = 5

LARGE_SZ = 100000


def check_simple_entry( entry, n, F_x, R_x ):
    """
    simple entry
    """
    if n % LONGMOD:
        node = "%s%04d" % (NODEBASE, n)
    else:
        node = "%s%04d" % (LONGBASE, n)
    # Which field?
    if F_x == F_ANCHOR:
        Expect( entry == ROOT_ID )
    elif F_x == F_ANCHOR_OBID:
        Expect( entry == ROOT_OBID )
    elif F_x == F_ID:
        Expect( type(entry) is str )
        i = int( entry.split( "_" )[1] )
        Expect( i == n )
        Expect( entry == node )
    elif F_x == F_OBID:
        Expect( type(entry) is str )
        Expect( entry == strhash128(node) )
    elif F_x == F_TYPE:
        Expect( entry == "node" )
    elif F_x == F_DEG:
        Expect( entry == 1 + L2_FANOUT )
    elif F_x == F_IDEG:
        Expect( entry == 1 )
    elif F_x == F_ODEG:
        Expect( entry == L2_FANOUT )
    elif F_x == F_ARCDIR:
        Expect( entry == "D_OUT" )
    elif F_x == F_REL:
        Expect( entry == "to" )
    elif F_x == F_MOD:
        Expect( entry == "M_INT" )
    elif F_x == F_VAL:
        Expect( entry == n )
    elif F_x == F_VEC:
        Expect( type(entry) is list )
        dim1 = node[:8]
        dim2 = "second"
        dim3 = "d%d" % n
        Expect( entry[0][0] == dim1 )
        Expect( entry[1][0] == dim2 )
        Expect( entry[2][0] == dim3 )
    elif F_x == F_PROP:
        if R_x & R_LIST:
            Expect( type(entry) is list )
            for item in entry:
                if type( item ) is str:
                    Expect( item == "my name is %s" % node )
                elif type( item ) is int:
                    Expect( item == n )
                elif type( item ) is float:
                    Expect( item == float(n) )
        else:
            Expect( type(entry) is dict )
            Expect( entry['name'] == "my name is %s" % node )
            Expect( entry['n_int'] == n )
            Expect( type(entry['n_float']) is float )
            Expect( entry['n_float'] == float(n) )
    elif F_x == F_SIM:
        Expect( entry == -1.0 )
    elif F_x == F_HAM:
        Expect( entry == 64 )
    elif F_x == F_TMC:
        Expect( entry == graph[node].TMC )
    elif F_x == F_TMM:
        Expect( entry == graph[node].TMM )
    elif F_x == F_TMX:
        Expect( entry == graph[node].TMX )
    elif F_x == F_DESCR:
        Expect( type(entry) is int )
    elif F_x == F_ADDR:
        objstr = graph.DebugGetObjectByAddress( entry )
        obid = re.search( r"Internalid=\"(\S+)\"", objstr ).group(1)
        Expect( obid == strhash128(node) )
    elif F_x == F_HANDLE:
        Expect( len(entry.split(":")) == 4 )
    elif F_x == F_RAW:
        Expect( len(entry.split()) == 24 )



def check_single_string_entry( entry, n, F_x ):
    """
    single string entry
    """
    Expect( type(entry) is str )
    if n % LONGMOD:
        node = "%s%04d" % (NODEBASE, n)
    else:
        node = "%s%04d" % (LONGBASE, n)
    # Which field?
    if F_x == F_ANCHOR:
        Expect( entry == ROOT_ID )
    elif F_x == F_ANCHOR_OBID:
        Expect( entry == ROOT_OBID )
    elif F_x == F_ID:
        i = int( entry.split( "_" )[1] )
        Expect( i == n )
        Expect( entry == node )
    elif F_x == F_OBID:
        Expect( entry == strhash128(node) )
    elif F_x == F_TYPE:
        Expect( entry == "node" )
    elif F_x == F_DEG:
        Expect( entry == str(1 + L2_FANOUT) )
    elif F_x == F_IDEG:
        Expect( entry == "1" )
    elif F_x == F_ODEG:
        Expect( entry == str(L2_FANOUT) )
    elif F_x == F_ARCDIR:
        Expect( entry == "->" )
    elif F_x == F_REL:
        Expect( entry == "to" )
    elif F_x == F_MOD:
        Expect( entry == "M_INT" )
    elif F_x == F_VAL:
        Expect( entry == str(n) )
    elif F_x == F_VEC:
        vec = json.loads( entry )
        dim1 = node[:8]
        dim2 = "second"
        dim3 = "d%d" % n
        Expect( vec[0][0] == dim1 )
        Expect( vec[1][0] == dim2 )
        Expect( vec[2][0] == dim3 )
    elif F_x == F_PROP:
        prop = json.loads( entry )
        Expect( prop['name'] == "my name is %s" % node )
        Expect( prop['n_int'] == n )
        Expect( type(prop['n_float']) is float )
        Expect( prop['n_float'] == float(n) )
    elif F_x == F_RANK:
        Expect( float(entry) == float(n), "%s, got %s" % (n, entry) )
    elif F_x == F_SIM:
        Expect( float(entry) == -1 )
    elif F_x == F_HAM:
        Expect( float(entry) == 64 )
    elif F_x == F_TMC:
        Expect( entry == str(graph[node].TMC) )
    elif F_x == F_TMM:
        Expect( entry == str(graph[node].TMM) )
    elif F_x == F_TMX:
        Expect( entry == str(graph[node].TMX) )
    elif F_x == F_DESCR:
        Expect( len(entry.split()) == 5 )
    elif F_x == F_ADDR:
        addr = int( entry )
        objstr = graph.DebugGetObjectByAddress( addr )
        obid = re.search( r"Internalid=\"(\S+)\"", objstr ).group(1)
        Expect( obid == strhash128(node) )
    elif F_x == F_HANDLE:
        Expect( len(entry.split(":")) == 4 )
    elif F_x == F_RAW:
        qwords = re.findall( r"<(\w+)>", entry )
        Expect( len(qwords) == 24 )



def check_complex_string_entry( entry, n, fieldmask ):
    """
    complex string entry
    """
    Expect( type(entry) is str )
    if n % LONGMOD:
        node = "%s%04d" % (NODEBASE, n)
    else:
        node = "%s%04d" % (LONGBASE, n)
    if fieldmask & NON_ARC_FIELDS:
        data = json.loads( entry )
    else:
        a = ROOT_ID if fieldmask & F_ANCHOR else ""
        pred = [""]
        if fieldmask & F_REL:
            pred.append( "to" )
        if fieldmask & F_MOD:
            pred.append( "<M_INT>" )
        if fieldmask & F_VAL:
            pred.append( str(n) )
        pred.append("")
        d = ">" if fieldmask & PRED_FIELDS else ""
        b = node if fieldmask & F_ID else ""
        arc = "( %s )-[%s]-%s( %s )" % (a, " ".join(pred), d, b)
        Expect( entry == arc, "%s, got %s" % (arc, entry) )



def check_dict_items( entry, n, F_x ):
    """
    dict items
    """
    if n % LONGMOD:
        node = "%s%04d" % (NODEBASE, n)
    else:
        node = "%s%04d" % (LONGBASE, n)
    # Which field?
    if F_x & F_ANCHOR:
        Expect( entry["anchor"] == ROOT_ID )
    elif F_x & F_ANCHOR_OBID:
        Expect( entry["anchor-internalid"] == ROOT_OBID )
    elif F_x & F_ID:
        Expect( entry["id"] == node )
        i = int( entry["id"].split( "_" )[1] )
        Expect( i == n )
    elif F_x & F_OBID:
        Expect( entry["internalid"] == strhash128(node) )
    elif F_x & F_TYPE:
        Expect( entry["type"] == "node" )
    elif F_x & F_DEG:
        Expect( entry["degree"] == 1 + L2_FANOUT )
    elif F_x & F_IDEG:
        Expect( entry["indegree"] == 1 )
    elif F_x & F_ODEG:
        Expect( entry["outdegree"] == L2_FANOUT )
    elif F_x & F_ARCDIR:
        Expect( type(entry["arc"]) is dict )
        Expect( entry["arc"]["direction"] == "D_OUT" )
    elif F_x & F_REL:
        Expect( type(entry["arc"]) is dict )
        Expect( entry["arc"]["relationship"] == "to" )
    elif F_x & F_MOD:
        Expect( type(entry["arc"]) is dict )
        Expect( entry["arc"]["modifier"] == "M_INT" )
        Expect( entry["distance"] == 1 )
    elif F_x & F_VAL:
        Expect( type(entry["arc"]) is dict )
        Expect( entry["arc"]["value"] == n )
        Expect( entry["distance"] == 1 )
    elif F_x & F_VEC:
        vec = entry["vector"]
        Expect( type(vec) is list )
        dim1 = node[:8]
        dim2 = "second"
        dim3 = "d%d" % n
        Expect( vec[0][0] == dim1 )
        Expect( vec[1][0] == dim2 )
        Expect( vec[2][0] == dim3 )
    elif F_x & F_PROP:
        prop = entry["properties"]
        Expect( type(prop) is dict )
        Expect( prop['name'] == "my name is %s" % node )
        Expect( prop['n_int'] == n )
        Expect( type(prop['n_float']) is float )
        Expect( prop['n_float'] == float(n) )
    elif F_x & F_SIM:
        Expect( entry["similarity"] == -1.0 )
    elif F_x & F_HAM:
        Expect( entry["hamming-distance"] == 64 )
    elif F_x & F_TMC:
        Expect( entry["created"] == graph[node].TMC )
    elif F_x & F_TMM:
        Expect( entry["modified"] == graph[node].TMM )
    elif F_x & F_TMX:
        Expect( entry["expires"] == graph[node].TMX )
    elif F_x & F_DESCR:
        Expect( type(entry["descriptor"]) is int )
    elif F_x & F_ADDR:
        addr = entry["address"]
        objstr = graph.DebugGetObjectByAddress( addr )
        obid = re.search( r"Internalid=\"(\S+)\"", objstr ).group(1)
        Expect( obid == strhash128(node) )
    elif F_x & F_HANDLE:
        handle = entry["handle"]
        Expect( len(handle.split(":")) == 4 )
    elif F_x & F_RAW:
        raw = entry["raw-vertex"]
        Expect( len(raw.split()) == 24 )



def check_result( R_x, result, sz ):
    """
    check result type of small graph result
    """
    if R_x & R_METAS:
        Expect( type( result ) is dict )
        if R_x & R_COUNTS:
            Expect( "counts" in result )
            counts = result['counts']
            if "vertices" in counts:
                vertices = counts['vertices']
                Expect( type(vertices) is int )
            else:
                Expect( "neighbors" in counts and "arcs" in counts )
                neighbors = counts['neighbors']
                arcs = counts['arcs']
                Expect( type(neighbors) is int )
                Expect( type(arcs) is int )
                Expect( arcs >= neighbors, "arcs=%d should be >= neighbors=%d" % (arcs, neighbors) )

        if R_x & R_TIMING:
            Expect( "time" in result )
            timing = result['time']
            Expect( "total" in timing )
            Expect( "search" in timing )
            Expect( "result" in timing )
            t_total = timing['total']
            t_search = timing['search']
            t_result = timing['result']
            if t_total + 1e-6 < t_search + t_result:
                Expect( False, "t_total=%f t_search=%f t_result=%f, total should be >= %f" % (t_total, t_search, t_result, t_search+t_result) )
        if "vertices" in result:
            result_list = result[ 'vertices' ]
        elif "neighborhood" in result:
            result_list = result[ 'neighborhood' ]
        else:
            Expect( False )
    else:
        result_list = result
    Expect( type( result_list ) is list )
    Expect( len( result_list ) == sz )
    return result_list


                
def TEST_SmallSetup():
    """
    Set up a small graph for result field testing
    test_level=3101
    """

    graph.Truncate()

    graph.CreateVertex( ROOT_ID )

    for n in range(SMALL_SZ):
        if n % LONGMOD:
            node = "%s%04d" % (NODEBASE, n)
        else:
            node = "%s%04d" % (LONGBASE, n)
        V = graph.NewVertex( node, type="node" )
        V.SetProperty( "name", "my name is %s" % node )
        V.SetProperty( "n_int", n )
        V.SetProperty( "n_float", float(n) )
        dim1 = node[:8]
        dim2 = "second"
        dim3 = "d%d" % n
        V.SetVector( [(dim1,1.5), (dim2,1.0), (dim3,0.5)] )
        graph.Connect( ROOT_ID, ("to",M_INT,n), V )
        for m in range(L2_FANOUT):
            term = "term_%d" % (n+m)
            graph.Connect( node, ("term",M_FLT,n+m), term )
        graph.CloseVertex( V )
    graph.DebugCheckAllocators()



def TEST_R_SIMPLE():
    """
    Test Neighborhood() with R_SIMPLE results
    test_level=3101
    """
    if ROOT_ID not in graph:
        TEST_SmallSetup()

    # R_SIMPLE
    for R_x in [R_SIMPLE, R_SIMPLE|R_METAS]:
        for F_x in FIELDS:
            result = graph.Neighborhood( ROOT_ID, arc=("to", D_OUT, M_INT), sortby=S_VAL|S_ASC, result=R_x, fields=F_x )
            result_list = check_result( R_x, result, SMALL_SZ )
            for n in range( SMALL_SZ ):
                entry = result_list[ n ]
                check_simple_entry( entry, n, F_x, R_x )
    graph.DebugCheckAllocators()



def TEST_R_STR():
    """
    Test Neighborhood() with R_STR results
    test_level=3101
    """
    if ROOT_ID not in graph:
        TEST_SmallSetup()

    # R_STR
    for R_x in [R_STR, R_STR|R_METAS]:
        # one
        for F_x in FIELDS:
            result = graph.Neighborhood( ROOT_ID, arc=("to", D_OUT, M_INT), sortby=S_VAL|S_ASC, result=R_x, fields=F_x )
            result_list = check_result( R_x, result, SMALL_SZ )
            for n in range( SMALL_SZ ):
                entry = result_list[ n ]
                check_single_string_entry( entry, n, F_x )
        # two
        for F_a in FIELDS:
            for F_b in FIELDS:
                if F_b <= F_a:
                    continue
                result = graph.Neighborhood( ROOT_ID, arc=("to", D_OUT, M_INT), sortby=S_VAL|S_ASC, result=R_x, fields=F_a|F_b )
                result_list = check_result( R_x, result, SMALL_SZ )
                for n in range( SMALL_SZ ):
                    entry = result_list[ n ]
                    check_complex_string_entry( entry, n, F_a|F_b )
        # all
        result = graph.Neighborhood( ROOT_ID, arc=("to", D_OUT, M_INT), sortby=S_VAL|S_ASC, result=R_x, fields=F_ALL )
        result_list = check_result( R_x, result, SMALL_SZ )
        for n in range( SMALL_SZ ):
            entry = result_list[ n ]
            check_complex_string_entry( entry, n, F_ALL )
    graph.DebugCheckAllocators()



def TEST_R_LIST():
    """
    Test Neighborhood() with R_LIST results
    test_level=3101
    """
    if ROOT_ID not in graph:
        TEST_SmallSetup()

    # R_LIST
    for R_x in [R_LIST, R_LIST|R_METAS]:
        # one
        for F_x in FIELDS:
            result = graph.Neighborhood( ROOT_ID, arc=("to", D_OUT, M_INT), sortby=S_VAL|S_ASC, result=R_x, fields=F_x )
            result_list = check_result( R_x, result, SMALL_SZ )
            for n in range( SMALL_SZ ):
                entry = result_list[ n ]
                Expect( type(entry) is tuple )
                Expect( len(entry) == 1 )
                single = entry[0]
                check_simple_entry( single, n, F_x, R_x )
        # two
        for F_a in FIELDS:
            for F_b in FIELDS:
                if F_b <= F_a:
                    continue
                result = graph.Neighborhood( ROOT_ID, arc=("to", D_OUT, M_INT), sortby=S_VAL|S_ASC, result=R_x, fields=F_a|F_b )
                result_list = check_result( R_x, result, SMALL_SZ )
                for n in range( SMALL_SZ ):
                    entry = result_list[ n ]
                    Expect( type(entry) is tuple )
                    Expect( len(entry) == 2 )
                    first, second = entry
                    if LIST_ORDER[ F_a ] < LIST_ORDER[ F_b ]:
                        F_x, F_y = F_a, F_b
                    else:
                        F_x, F_y = F_b, F_a
                    check_simple_entry( first, n, F_x, R_x )
                    check_simple_entry( second, n, F_y, R_x )
        # all
        result = graph.Neighborhood( ROOT_ID, arc=("to", D_OUT, M_INT), sortby=S_VAL|S_ASC, result=R_x, fields=F_ALL )
        result_list = check_result( R_x, result, SMALL_SZ )
        for n in range( SMALL_SZ ):
            entry = result_list[ n ]
            Expect( type(entry) is tuple )
            Expect( len(entry) == len(FIELDS) )
            for F_x in FIELDS:
                i = LIST_ORDER[ F_x ]
                check_simple_entry( entry[i], n, F_x, R_x )
    graph.DebugCheckAllocators()



def TEST_R_DICT():
    """
    Test Neighborhood() with R_DICT results
    test_level=3101
    """
    if ROOT_ID not in graph:
        TEST_SmallSetup()

    # R_DICT
    for R_x in [R_DICT, R_DICT|R_METAS]:
        # one
        for F_x in FIELDS:
            result = graph.Neighborhood( ROOT_ID, arc=("to", D_OUT, M_INT), sortby=S_VAL|S_ASC, result=R_x, fields=F_x )
            result_list = check_result( R_x, result, SMALL_SZ )
            for n in range( SMALL_SZ ):
                entry = result_list[ n ]
                Expect( type(entry) is dict )
                check_dict_items( entry, n, F_x )
        # two
        for F_a in FIELDS:
            for F_b in FIELDS:
                if F_b <= F_a:
                    continue
                result = graph.Neighborhood( ROOT_ID, arc=("to", D_OUT, M_INT), sortby=S_VAL|S_ASC, result=R_x, fields=F_a|F_b )
                result_list = check_result( R_x, result, SMALL_SZ )
                for n in range( SMALL_SZ ):
                    entry = result_list[ n ]
                    Expect( type(entry) is dict )
                    check_dict_items( entry, n, F_a|F_b )

        # all
        result = graph.Neighborhood( ROOT_ID, arc=("to", D_OUT, M_INT), sortby=S_VAL|S_ASC, result=R_x, fields=F_ALL )
        result_list = check_result( R_x, result, SMALL_SZ )
        for n in range( SMALL_SZ ):
            entry = result_list[ n ]
            Expect( type(entry) is dict )
            check_dict_items( entry, n, F_ALL )
    graph.DebugCheckAllocators()



def TEST_random_fields_and_modes( worker=None, hitcount=-1, loopsize=SMALL_SZ, loop_maxtime=-1 ):
    """
    Test Neighborhood() with random combinations of fields and modes
    t_nominal=11
    test_level=3101
    """
    try:
        if ROOT_ID not in graph:
            TEST_SmallSetup()


        roots = graph.Vertices( condition={'id':'%s*'%ROOT_ID} )
        nroots = len( roots )

        # Chaos (don't crash)
        
        n = 0


        for R_x in [R_SIMPLE, R_STR, R_LIST, R_DICT]:
            #LogInfo( "random R_x=%d" % R_x)
            R_x |= R_METAS
            for F_x in FIELDS:
                #LogInfo( "random F_x=%d" % F_x)
                t0 = timestamp()
                if loop_maxtime > 0:
                    t1 = t0 + loop_maxtime
                else:
                    t1 = -1
                for r in range( loopsize ):
                    n += 1
                    F_rand = F_x | (random.randint( 0, 2**24 ) & ALL_FIELDS)
                    h = random.randint( 0, SMALL_SZ ) if hitcount < 0 else hitcount
                    root = roots[ random.randint(0,nroots-1) ]
                    result = graph.Neighborhood( root, arc=("to", D_OUT, M_INT), neighbor={ 'arc':("term",D_OUT,M_FLT) }, sortby=S_RANDOM, result=R_x, fields=F_rand, hits=h )
                    if t1 > 0 and timestamp() > t1:
                        break
        graph.DebugCheckAllocators()
        return True
    except:
        if worker is not None:
            worker.terminate()
            return False
        else:
            raise



def TEST_multithread_random_fields_and_modes():
    """
    Test random fields and modes with multiple simultaneous threads
    t_nominal=28
    test_level=3101
    """
    if ROOT_ID not in graph:
        TEST_SmallSetup()

    NW = 5

    # Make workers
    WORKERS = []
    for w in range( NW ):
        WORKERS.append( Worker( "worker_%d" % w ) )
    time.sleep( 0.5 )

    # Perform parallel test
    for w in WORKERS:
        w.perform( TEST_random_fields_and_modes, w, -1, 75 )

    # Wait for all workers to finish
    while sum([ 1 for w in WORKERS if w.is_idle() ]) < NW:
        time.sleep( 0.1 )

    # Terminate all workers
    for w in WORKERS:
        w.terminate()
    while sum([ 1 for w in WORKERS if w.is_dead() ]) < NW:
        time.sleep( 0.1 )

    # Last test to use small setup
    graph.DebugCheckAllocators()
    graph.Truncate()



def TEST_LargeSetup():
    """
    Set up a large graph for result field testing
    test_level=3201
    """
    graph.Truncate()

    graph.CreateVertex( ROOT_ID )
    NRAND = 10
    for r in range( NRAND ):
        rand_root = "%s_%d" % (ROOT_ID,r)
        graph.CreateVertex( rand_root )

    for n in range(LARGE_SZ):
        if n % LONGMOD:
            node = "%s%04d" % (NODEBASE, n)
        else:
            node = "%s%04d" % (LONGBASE, n)
        V = graph.NewVertex( node, type="node" )
        V.SetProperty( "name", "my name is %s" % node )
        V.SetProperty( "n_int", n )
        V.SetProperty( "n_float", float(n) )
        dim1 = node[:8]
        dim2 = "second"
        dim3 = "d%d" % n
        V.SetVector( [(dim1,1.5), (dim2,1.0), (dim3,0.5)] )
        graph.Connect( ROOT_ID, ("to",M_INT,n), V )
        r = random.randint(0,NRAND-1)
        rand_root = "%s_%d" % (ROOT_ID,r)
        graph.Connect( rand_root, ("to",M_INT,n), V )
        graph.CloseVertex( V )
    graph.DebugCheckAllocators()



def TEST_large_neighborhood_result_set( worker=None, maxhits=-1 ):
    """
    Return large result set for Neighborhood()
    t_nominal=67
    test_level=3201
    """
    try:
        if ROOT_ID not in graph:
            TEST_LargeSetup()

        for metas in [0, R_COUNTS, R_TIMING, R_METAS]:
            for R_x in [R_SIMPLE|metas, R_STR|metas, R_LIST|metas, R_DICT|metas]:
                #LogInfo( "neighborhood R_x=%d" % R_x)
                for F_x in [ F_ID, F_TYPE, F_PROP, F_DEG, F_ALL ]:
                    #LogInfo( "neighborhood F_x=%d" % F_x)
                    fullsz = LARGE_SZ if maxhits < 0 else maxhits
                    for hc in [0, 1, 2, 3, 9, 99, 100, 101, fullsz, maxhits]:
                        # Neighborhood
                        result = graph.Neighborhood( ROOT_ID, result=R_x, fields=F_x, hits=hc )
                        sz = LARGE_SZ if hc < 0 else hc
                        result_list = check_result( R_x, result, sz )
        graph.DebugCheckAllocators()
        return True
    except:
        if worker is not None:
            worker.terminate()
            return False
        else:
            raise



def TEST_large_global_result_set( worker=None, maxhits=-1 ):
    """
    Return large result set for Vertices()
    t_nominal=57
    test_level=3201
    """
    try:
        if ROOT_ID not in graph:
            TEST_LargeSetup()

        for metas in [0, R_COUNTS, R_TIMING, R_METAS]:
            for R_x in [R_SIMPLE|metas, R_STR|metas, R_LIST|metas, R_DICT|metas]:
                #LogInfo( "global R_x=%d" % R_x)
                for F_x in [ F_ID, F_TYPE, F_PROP, F_DEG, F_ALL ]:
                    #LogInfo( "global F_x=%d" % F_x)
                    fullsz = graph.order if maxhits < 0 else maxhits
                    for hc in [0, 1, 2, 3, 9, 99, 100, 101, fullsz, maxhits]:
                        # Vertices
                        result = graph.Vertices( result=R_x, fields=F_x, hits=hc )
                        sz = graph.order if hc < 0 else hc
                        result_list = check_result( R_x, result, sz )
        graph.DebugCheckAllocators()
        return True
    except:
        if worker is not None:
            worker.terminate()
            return False
        else:
            raise



def TEST_multithread_large_result_sets( worker=None ):
    """
    Test large result sets with multiple simultaneous threads
    t_nominal=683
    test_level=3201
    """
    if ROOT_ID not in graph:
        TEST_LargeSetup()


    for READONLY in [False, True]:
        if READONLY:
            NW = 8
            #LogInfo( "READONLY" )
            graph.SetGraphReadonly( 60000 )
        else:
            NW = 4
            #LogInfo( "WRITABLE" )

        # Make workers
        WORKERS = []
        for w in range( NW ):
            WORKERS.append( Worker( "worker_%d" % w ) )
        time.sleep( 0.5 )


        # Perform parallel test
        wx = 0
        for jx in range( NW * 3 ):
            worker = WORKERS[ wx ]
            worker.perform( TEST_large_neighborhood_result_set, worker, 8000 )
            time.sleep( 0.1 )
            wx += 1
            if wx >= NW:
                wx = 0

            worker = WORKERS[ wx ]
            worker.perform( TEST_random_fields_and_modes, worker, -1, 50, 1 )
            time.sleep( 0.1 )
            wx += 1
            if wx >= NW:
                wx = 0

            worker = WORKERS[ wx ]
            worker.perform( TEST_large_global_result_set, worker, 10000 )
            time.sleep( 0.1 )
            wx += 1
            if wx >= NW:
                wx = 0

        # Wait for all workers to finish
        while sum([ 1 for w in WORKERS if w.is_idle() ]) < NW:
            time.sleep( 0.1 )

        #LogInfo( "All workers finished" )

        # Terminate all workers
        for w in WORKERS:
            w.terminate()
        while sum([ 1 for w in WORKERS if w.is_dead() ]) < NW:
            time.sleep( 0.1 )

        if READONLY:
            graph.ClearGraphReadonly()



    # Last test to use large setup
    graph.DebugCheckAllocators()
    graph.ClearGraphReadonly()
    graph.Truncate()





def Run( name ):
    print(system.Registry())
    global graph
    graph = pyvgx.Graph( name )
    RunTests( [__name__] )
    graph.Truncate()
    graph.Close()
    del graph
