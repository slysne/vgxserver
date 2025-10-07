###############################################################################
# 
# VGX Server
# Distributed engine for plugin-based graph and vector search
# 
# Module:  pyvgx.test
# File:    Select.py
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
from ..Query._query_test_support import *
from pyvgx import *
import pyvgx
from math import *
import random
import json
import operator

graph = None

FIELDS = [              #   N   PRED?   ARC?
                        #  ---  -----   ----
    F_NONE,             #   -     -      -
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



FIELD_PROP_MAP = {
    #F_REL       : (),   # Can't do these arc fields easily because of nested structure or fancy string
    #F_MOD       : (),
    #F_VAL       : ( "" ),
    F_ID        : ( "id",           ".id" ),
    F_OBID      : ( "internalid",   ".internalid" ),
    F_TYPE      : ( "type",         ".type" ),
    F_DEG       : ( "degree",       ".deg" ),
    F_IDEG      : ( "indegree",     ".ideg" ),
    F_ODEG      : ( "outdegree",    ".odeg" ),
    F_VEC       : ( "vector",       ".vector" ),
    F_RANK      : ( "rankscore",    ".rank" ),
    F_TMC       : ( "created",      ".tmc" ),
    F_TMM       : ( "modified",     ".tmm" ),
    F_TMX       : ( "expires",      ".tmx" ),
    F_ADDR      : ( "address",      ".address" )
}


PROP_TO_FIELD_MAP = {
    ".id"           : F_ID,
    ".internalid"   : F_OBID,
    ".type"         : F_TYPE,
    ".deg"          : F_DEG,
    ".ideg"         : F_IDEG,
    ".odeg"         : F_ODEG,
    ".vector"       : F_VEC,
    ".rank"         : F_RANK,
    ".tmc"          : F_TMC,
    ".tmm"          : F_TMM,
    ".tmx"          : F_TMX,
    ".address"      : F_ADDR
}


# Alpha, Beta, Gamma
A_B_G = "".join([chr(x) for x in range(945,948)])
A_B_G_utf8 = A_B_G.encode()



###############################################################################
# check_dict_result
#
###############################################################################
def check_dict_result( F_x, keys, data ):
    """
    Check dict result entry
    """
    P = data.get( 'properties', data )
    # Make sure all selected keys are present in the result entry
    if keys[0] == "*":
        Expect( "number" in P and "string" in P and "utf8" in P and len(P) == 3 )
    else:
        for key in keys:
            if ":" in key:
                key, default_val = [x.strip() for  x in key.split(":")]
            elif "=" in key:
                label, expr = [x.strip() for  x in key.split("=")]
                key = label
            Expect( key in P, "'%s' should be in %s" % (key,P) )
    # Match the property values when we can
    if F_x == F_ID:
        N = int( data['id'].split("_")[-1] )
        if "number" in P:
            n = P["number"]
            Expect( n == N )
        if "string" in P:
            n = int( P["string"].split()[-1] )
            Expect( A_B_G in P["string"] )
            Expect( n == N )
        if "utf8" in P:
            uni = P["utf8"]
            n = int( uni.split()[-1] )
            if type(uni) is bytes:
                Expect( A_B_G_utf8 in uni )
            else:
                Expect( A_B_G in uni )
            Expect( n == N )

    # Match selected property against normal field
    if F_x in FIELD_PROP_MAP:
        fname, pname = FIELD_PROP_MAP[ F_x ]
        Expect( fname in data,                "'%s' should be in %s" % (fname, data)  )
        if pname in P:
            Expect( data[fname] == P[pname],  "'%s' should equal '%s'" % (data[fname], P[pname]) )
    



###############################################################################
# check_json_result
#
###############################################################################
def check_json_result( F_x, keys, result ):
    """
    Convert (json) string result to dict and check
    """
    data = json.loads( result )
    check_dict_result( F_x, keys, data )




###############################################################################
# check_list_result
#
###############################################################################
def check_list_result( F_x, keys, result ):
    """
    Check list result entry
    """
    Expect( type( result ) is tuple )
    if F_x in [F_NONE, F_PROP]:
        Expect( len( result ) == 1 )
        proplist = result[0]
        fieldval = None
    else:
        Expect( len( result ) == 2 )
        if F_x < F_PROP:
            proplist = result[1]
            fieldval = result[0]
        else:
            proplist = result[0]
            fieldval = result[1]

    Expect( type( proplist ) is list )
    np = len( proplist )
    if keys[0] == "*":
        nk = 3
    else:
        nk = len( keys )
    Expect( np == nk,       "%d != %d" % (np, nk) )
    # Match selected property against normal field
    # and verify order of property values as listed in keys
    if fieldval is not None:
        if keys[0] != "*":
            for key,valtuple in zip( keys, proplist ):
                pkey, pval = valtuple
                if "=" in key: # skip labels
                    continue
                Expect( key == pkey,        "%s == %s" % (key, pkey) )
                if PROP_TO_FIELD_MAP.get( key ) == F_x:
                    Expect( pval == fieldval,    "%s == %s" % (pval, fieldval) )




###############################################################################
# check_results
#
###############################################################################
def check_results( R_x, F_x, select, results ):
    """
    Check a sample of results
    """
    keys = [key.strip().split(":")[0] for key in select.split(";")]
    # Check first 10 hits, 30 randomly selected hits, last 10 hits (too slow to check all)
    sample = results[:10]
    sample.extend( random.sample( results, 30 ) )
    sample.extend( results[-10:] )
    for item in sample:
        if R_x == R_STR:
            check_json_result( F_x, keys, item )
        elif R_x == R_LIST:
            check_list_result( F_x, keys, item )
        elif R_x == R_DICT:
            check_dict_result( F_x, keys, item )




###############################################################################
# TEST_Neighborhood_basic_select
#
###############################################################################
def TEST_Neighborhood_basic_select():
    """
    pyvgx.Neighborhood() select statement
    t_nominal=183
    test_level=3102
    """
    graph.Truncate()
    root = "select_root"
    rankvector = [("dim1_1",1.0),("dim2_1",0.5),("dim3_1",0.25)]
    for n in range( 1000 ):
        V = graph.NewVertex( "select_node_%d" % n )
        V['string'] = "this is %s string %d" % (A_B_G, n)
        V['utf8'] = b"encoded %s %d" % (A_B_G_utf8, n)
        V['number'] = n
        graph.Connect( root, "to", V )
        graph.CloseVertex( V )
    for n in range( 1000, 1200 ):
        V = graph.NewVertex( "select_node_with_vector_%d" % n )
        V['string'] = "this is %s string %d" % (A_B_G, n)
        V['utf8'] = b"encoded %s %d" % (A_B_G_utf8, n)
        V['number'] = n
        V.SetVector( [("dim1_%d"%(n%17),1.0),("dim2_%d"%(n%23),0.5),("dim3_%d"%(n%31),0.25)] )
        graph.Connect( root, "to", V )
        graph.CloseVertex( V )


    SELECT = [
        ".c1",
        ".c0",
        ".virtual",
        ".id",
        ".internalid",
        ".type",
        ".deg",
        ".ideg",
        ".odeg",
        ".tmc",
        ".tmm",
        ".tmx",
        ".vector",
        ".op",
        ".refc",
        ".bidx",
        ".oidx",
        ".address",
        ".arc.value",
        ".arc.dist",
        ".arc.type",
        ".arc.dir",
        ".arc.mod",
        "*",
        "number",
        "string",
        "utf8",
        "number; string",
        "string; number",
        "number; utf8",
        "utf8; number",
        "string; utf8",
        "utf8; string",
        "number; string; utf8",
        "string; number; utf8",
        "number; utf8; string",
        "utf8; number; string",
        "string; utf8; number",
        "utf8; string; number",
        "string; number; utf8; .c1",
        "string; number; utf8; .c1; .c0",
        "string; number; utf8; .c1; .c0; .virtual",
        "string; number; utf8; .c1; .c0; .virtual; .id",
        "string; number; utf8; .c1; .c0; .virtual; .id; .internalid",
        "string; number; utf8; .c1; .c0; .virtual; .id; .internalid; .type",
        "string; number; utf8; .c1; .c0; .virtual; .id; .internalid; .type; .deg",
        "string; number; utf8; .c1; .c0; .virtual; .id; .internalid; .type; .deg; .ideg",
        "string; number; utf8; .c1; .c0; .virtual; .id; .internalid; .type; .deg; .ideg; .odeg",
        "string; number; utf8; .c1; .c0; .virtual; .id; .internalid; .type; .deg; .ideg; .odeg; .tmc",
        "string; number; utf8; .c1; .c0; .virtual; .id; .internalid; .type; .deg; .ideg; .odeg; .tmc; .tmm",
        "string; number; utf8; .c1; .c0; .virtual; .id; .internalid; .type; .deg; .ideg; .odeg; .tmc; .tmm; .tmx",
        "string; number; utf8; .c1; .c0; .virtual; .id; .internalid; .type; .deg; .ideg; .odeg; .tmc; .tmm; .tmx; .vector",
        "string; number; utf8; .c1; .c0; .virtual; .id; .internalid; .type; .deg; .ideg; .odeg; .tmc; .tmm; .tmx; .vector; .op",
        "string; number; utf8; .c1; .c0; .virtual; .id; .internalid; .type; .deg; .ideg; .odeg; .tmc; .tmm; .tmx; .vector; .op; .refc",
        "string; number; utf8; .c1; .c0; .virtual; .id; .internalid; .type; .deg; .ideg; .odeg; .tmc; .tmm; .tmx; .vector; .op; .refc; .bidx",
        "string; number; utf8; .c1; .c0; .virtual; .id; .internalid; .type; .deg; .ideg; .odeg; .tmc; .tmm; .tmx; .vector; .op; .refc; .bidx; .oidx",
        "string; number; utf8; .c1; .c0; .virtual; .id; .internalid; .type; .deg; .ideg; .odeg; .tmc; .tmm; .tmx; .vector; .op; .refc; .bidx; .oidx; .address",
        "string; number; utf8; .c1; .c0; .virtual; .id; .internalid; .type; .deg; .ideg; .odeg; .tmc; .tmm; .tmx; .vector; .op; .refc; .bidx; .oidx; .address; .arc.value",
        "string; number; utf8; .c1; .c0; .virtual; .id; .internalid; .type; .deg; .ideg; .odeg; .tmc; .tmm; .tmx; .vector; .op; .refc; .bidx; .oidx; .address; .arc.value; .arc.dist",
        "string; number; utf8; .c1; .c0; .virtual; .id; .internalid; .type; .deg; .ideg; .odeg; .tmc; .tmm; .tmx; .vector; .op; .refc; .bidx; .oidx; .address; .arc.value; .arc.dist; .arc.type",
        "string; number; utf8; .c1; .c0; .virtual; .id; .internalid; .type; .deg; .ideg; .odeg; .tmc; .tmm; .tmx; .vector; .op; .refc; .bidx; .oidx; .address; .arc.value; .arc.dist; .arc.type; .arc.dir",
        "string; number; utf8; .c1; .c0; .virtual; .id; .internalid; .type; .deg; .ideg; .odeg; .tmc; .tmm; .tmx; .vector; .op; .refc; .bidx; .oidx; .address; .arc.value; .arc.dist; .arc.type; .arc.dir; .arc.mod",
        "number; .c1; .c0; .virtual; .id; .internalid; .type; .deg; .ideg; .odeg; .tmc; .tmm; .tmx; .vector; .op; .refc; .bidx; .oidx; .address; .arc.value; .arc.dist; .arc.type; .arc.dir; .arc.mod",
        ".c1; .c0; .virtual; .id; .internalid; .type; .deg; .ideg; .odeg; .tmc; .tmm; .tmx; .vector; .op; .refc; .bidx; .oidx; .address; .arc.value; .arc.dist; .arc.type; .arc.dir; .arc.mod",
        ".c0; .virtual; .id; .internalid; .type; .deg; .ideg; .odeg; .tmc; .tmm; .tmx; .vector; .op; .refc; .bidx; .oidx; .address; .arc.value; .arc.dist; .arc.type; .arc.dir; .arc.mod",
        ".virtual; .id; .internalid; .type; .deg; .ideg; .odeg; .tmc; .tmm; .tmx; .vector; .op; .refc; .bidx; .oidx; .address; .arc.value; .arc.dist; .arc.type; .arc.dir; .arc.mod",
        ".id; .internalid; .type; .deg; .ideg; .odeg; .tmc; .tmm; .tmx; .vector; .op; .refc; .bidx; .oidx; .address; .arc.value; .arc.dist; .arc.type; .arc.dir; .arc.mod",
        ".internalid; .type; .deg; .ideg; .odeg; .tmc; .tmm; .tmx; .vector; .op; .refc; .bidx; .oidx; .address; .arc.value; .arc.dist; .arc.type; .arc.dir; .arc.mod",
        ".type; .deg; .ideg; .odeg; .tmc; .tmm; .tmx; .vector; .op; .refc; .bidx; .oidx; .address; .arc.value; .arc.dist; .arc.type; .arc.dir; .arc.mod",
        ".deg; .ideg; .odeg; .tmc; .tmm; .tmx; .vector; .op; .refc; .bidx; .oidx; .address; .arc.value; .arc.dist; .arc.type; .arc.dir; .arc.mod",
        ".ideg; .odeg; .tmc; .tmm; .tmx; .vector; .op; .refc; .bidx; .oidx; .address; .arc.value; .arc.dist; .arc.type; .arc.dir; .arc.mod",
        ".odeg; .tmc; .tmm; .tmx; .vector; .op; .refc; .bidx; .oidx; .address; .arc.value; .arc.dist; .arc.type; .arc.dir; .arc.mod",
        ".tmc; .tmm; .tmx; .vector; .op; .refc; .bidx; .oidx; .address; .arc.value; .arc.dist; .arc.type; .arc.dir; .arc.mod",
        ".tmm; .tmx; .vector; .op; .refc; .bidx; .oidx; .address; .arc.value; .arc.dist; .arc.type; .arc.dir; .arc.mod",
        ".tmx; .vector; .op; .refc; .bidx; .oidx; .address; .arc.value; .arc.dist; .arc.type; .arc.dir; .arc.mod",
        ".vector; .op; .refc; .bidx; .oidx; .address; .arc.value; .arc.dist; .arc.type; .arc.dir; .arc.mod",
        ".op; .refc; .bidx; .oidx; .address; .arc.value; .arc.dist; .arc.type; .arc.dir; .arc.mod",
        ".refc; .bidx; .oidx; .address; .arc.value; .arc.dist; .arc.type; .arc.dir; .arc.mod",
        ".bidx; .oidx; .address; .arc.value; .arc.dist; .arc.type; .arc.dir; .arc.mod",
        ".oidx; .address; .arc.value; .arc.dist; .arc.type; .arc.dir; .arc.mod",
        ".address; .arc.value; .arc.dist; .arc.type; .arc.dir; .arc.mod",
        ".arc.value; .arc.dist; .arc.type; .arc.dir; .arc.mod",
        ".arc.dist; .arc.type; .arc.dir; .arc.mod",
        ".arc.type; .arc.dir; .arc.mod",
        ".arc.dir; .arc.mod",
        ".arc.mod",
        "number; number; string; string",
        "string; string; number; number",
        "number; string; number; string",
        "string; number; string; number",
        "utf8; number; number; string; string",
        "string; utf8; string; utf8; number; number",
        "number; string; utf8; utf8; number; string",
        "utf8; string; number; string; number; utf8; utf8",
        "string; number; string; number; .id",
        "string; number; string; .id; number; .id",
        "string; number; .id; string; .id; number; .id",
        "string; .id; number; .id; string; .id; number; .id",
        ".id; string; .id; number; .id; string; .id; number; .id",
        ".id; string; utf8; .id; number; .id; string; .id; number; .id",
        ".id; string; .id; number; .id; string; .id; number; .type",
        ".id; string; .id; number; .id; string; utf8; .id; number; .type",
        ".id; string; .id; number; .id; string; .type; number; .type",
        ".id; string; .type; number; .type; string; .type; number; .arc.type",
        ".id; string; .type; number; .type; string; utf8; .type; number; .arc.type",
        ".type; string; .type; number; .type; string; .arc.type; number; .arc.type",
        ".type; string; .type; number; .arc.type; string; .arc.type; number; .virtual",
        ".type; string; .arc.type; number; .arc.type; string; .virtual; number; .virtual",
        ".arc.type; string; .arc.type; number; .virtual; string; .virtual; number; .id",
        ".arc.type; string; .virtual; number; .virtual; string; .id; number; .vector",
        ".virtual; string; .virtual; number; .id; string; .vector; number; .internalid",
        ".virtual; string; .id; number; .vector; string; .internalid; number; .internalid",
        ".id; string; .vector; number; .internalid; string; .internalid; number; .vector",
        ".vector; string; .internalid; number; .internalid; string; .vector; number; .arc.value",
        ".internalid; string; .internalid; number; .vector; string; .arc.value; number; .arc.value",
        ".internalid; string; .vector; number; .arc.value; string; .arc.value; number; .arc.mod",
        ".vector; string; .arc.value; number; .arc.value; string; .arc.mod; number; .arc.mod",
        ".arc.value; string; .arc.value; number; .arc.mod; string; .arc.mod; number; .arc.type",
        ".arc.value; string; .arc.mod; number; .arc.mod; string; .arc.type; number; .arc.type",
        ".arc.mod; string; .arc.mod; number; .arc.type; string; .arc.type; number; .arc.dir",
        ".arc.mod; string; .arc.type; number; .arc.type; string; .arc.dir; number; .arc.dir",
        ".arc.type; string; .arc.type; number; .arc.dir; string; .arc.dir; number; .arc.dist",
        ".arc.type; string; .arc.dir; number; .arc.dir; string; .arc.dist; number; .id",
        ".arc.dir; string; .arc.dir; number; .arc.dist; string; .id; number",
        ".arc.dir; string; .arc.dist; number; .id; string; number",
        ".arc.dist; string; .id; number; string; number",
        ".id; string; number; string; number",
        ".id; string; number; utf8; string; number",
        ".id; string; number; string; utf8; number",
        "noexist",
        "noexist:100",
        "label: 1 + 2 + 3",
        "A: vertex['number'] / 5; B: vertex['string']",
        "number; A: vertex['number'] / 5; B: vertex['string']",
        "number; A: vertex['number'] / 5; B: vertex['utf8']",
        "A: vertex['number'] / 5; B: vertex['string']; number",
        "A: vertex['number'] / 5; B: vertex['utf8']; number",
        "string; A: vertex['number'] / 5; B: vertex['string']; number",
        "string; A: vertex['number'] / 5; B: vertex['utf8']; number",
        "string; A: vertex['number'] / 5; .id; B: vertex['string']; number",
        "string; A: vertex['number'] / 5; .id; B: vertex['utf8']; number",
        (False, "A: sim( .vector, vector )"),
        ".rank; A: context.rank",
        "A: graph.ts",
        "A: graph.age",
        "A: graph.order",
        "A: graph.size",
        "A: graph.op",
        "A: sys.tick",
        (False, "A: firstval( vertex['nope'], vertex['nix'], vertex['number'], 3.14 ) + 1"),
        (False, "A: firstval( vertex['nope'], vertex['nix'], 3.14, vertex['number'] ) + 1"),
        (False, "A: firstval( vertex['nope'], 3.14, vertex['nix'], vertex['number'] ) + 1"),
        (False, "A: firstval( 3.14, vertex['nope'], vertex['nix'], vertex['number'] ) + 1")
    ]

    for READONLY in [False, True]:
        if READONLY:
            graph.SetGraphReadonly( 60000 )
        for R_x in [R_SIMPLE, R_STR, R_LIST, R_DICT ]:
            for F_x in FIELDS:
                print("R_x:%08X F_x:%08X ..." % (R_x, F_x), end=' ')
                T = []
                rsz = 1
                for vector in [None, rankvector]:
                    for select in SELECT:
                        if type( select ) is tuple:
                            do_check, select = select
                        else:
                            do_check = True
                        t0 = timestamp()
                        try:
                            result = graph.Neighborhood( root, vector=vector, fields=F_x, result=R_x, select=select )
                        except:
                            print(select)
                            raise
                        t1 = timestamp()
                        rsz = len( result )
                        if select != "*":
                            tx = t1-t0
                            sz = len( select.split(";") )
                            T.append( (tx,sz) )
                        if do_check:
                            check_results( R_x, F_x, select, result )
                t_fld = 0.0
                t_exe = 0.0
                for tx,sz in T:
                    t_fld += tx/sz
                    t_exe += tx
                t_fld /= len(T)
                t_exe /= len(T)
                print("tx=%.2fms (%.2fus/hit)  field=%.2fms (%.2fus/hit)" % (1000*t_exe, 1000000*t_exe/rsz, 1000*t_fld, 1000000*t_fld/rsz))

                try:
                    result = graph.Neighborhood( root, fields=F_x, result=R_x, select=".notdefined" )
                    Expect( False, "Invalid vertex attribute should raise exception" )
                except QueryError:
                    pass
                except Exception as ex:
                    Expect( False, "Unexpected error: %s" % ex )

                try:
                    result = graph.Neighborhood( root, fields=F_x, result=R_x, select=")mmm" )
                    Expect( False, "Syntax error should raise exception" )
                except QueryError:
                    pass
                except Exception as ex:
                    Expect( False, "Unexpected error: %s" % ex )

        if READONLY:
            graph.ClearGraphReadonly()


    graph.DebugCheckAllocators()
    graph.ClearGraphReadonly()
    graph.Truncate()




###############################################################################
# TEST_Select_deref
#
###############################################################################
def TEST_Select_deref():
    """
    Select deref stability
    test_level=3102
    """
    graph.Truncate()
    root = "select_root"

    graph.CreateVertex( "A" )
    graph.CreateVertex( "B" )
    graph.CreateVertex( "C" )

    graph.Connect( "A", "to", "B" )
    graph.Connect( "B", "to", "C" )

    for R_x in [ R_SIMPLE, R_STR, R_LIST, R_DICT ]:
        for F_x in [ F_NONE, F_PROP, F_ID, F_DEG, F_ID|F_PROP, F_ID|F_DEG, F_PROP|F_DEG, F_PROP|F_ID|F_DEG]:
            # Neighborhood
            for node in ["A", "B", "C"]:
                for arcdir in [D_OUT, D_IN, D_ANY]:
                    for vref in ["", "next", "vertex", "prev"]:
                        for field in ["", ".c1", ".c0", ".virtual", ".id", ".internalid", ".type", ".deg", ".ideg", ".odeg",
                                      ".tmc", ".tmm", ".tmx", ".vector", ".op", ".refc", ".bidx", ".oidx", ".locked",
                                      ".address", ".index", ".bitindex", ".bitvector"]:
                            select = "%s%s" % (vref, field)
                            if select:
                                graph.Neighborhood( node, select="x: %s"              % select, fields=F_x, result=R_x )
                                graph.Neighborhood( node, select="x: %s; y: next.id"   % select, fields=F_x, result=R_x )
                                graph.Neighborhood( node, select="x: %s; y: vertex.id" % select, fields=F_x, result=R_x )
                                graph.Neighborhood( node, select="x: %s; y: prev.id"   % select, fields=F_x, result=R_x )

            # Global
            for aref in ["", "next", "prev"]:
                for field in ["", ".arc.value", ".arc.dist", ".arc.type", ".arc.dir", ".arc.mod"]:
                    select = "%s%s" % (aref, field)
                    if select:
                        graph.Vertices( select="x: %s"               % select, fields=F_x, result=R_x )
                        graph.Vertices( select="x: %s; y: next.id"    % select, fields=F_x, result=R_x )
                        graph.Vertices( select="x: %s; y: vertex.id"  % select, fields=F_x, result=R_x )
                        graph.Vertices( select="x: %s; y: prev.id"    % select, fields=F_x, result=R_x )
                        graph.Arcs(     select="x: %s"              % select, fields=F_x, result=R_x )
                        graph.Arcs(     select="x: %s; y: next.id"    % select, fields=F_x, result=R_x )
                        graph.Arcs(     select="x: %s; y: vertex.id"  % select, fields=F_x, result=R_x )
                        graph.Arcs(     select="x: %s; y: prev.id"    % select, fields=F_x, result=R_x )






    graph.Truncate()




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
    graph.Truncate()
    graph.Close()
    del graph
