from pytest.pytest import RunTests, Expect, TestFailed
from ..Query._query_test_support import *
from pyvgx import *
import pyvgx
from math import *
import operator

graph = None


def is_sorted_asc( L ):
  return all( a <= b for a, b in zip(L, L[1:]) )

def is_sorted_desc( L ):
  return all( a >= b for a, b in zip(L, L[1:]) )


def TEST_vxquery_rank():
    """
    Core vxquery_rank
    test_level=501
    """
    try:
        pyvgx.selftest( force=True, testroot="vgxtest", library="vgx", names=["vxquery_rank.c" ] )
    except:
        Expect( False )



def TEST_Neighborhood_rank():
    """
    Various ranking for pyvgx.Graph.Neighborhood()
    t_nominal=126
    test_level=3101
    """
    root = "rank_root"
    params = { 'id':root, 'result':R_DICT, 'sortby':S_RANK }
    levels = 2
    for fanout_factor in [5,25,125,625]:
    #for fanout_factor in [1,5,25,125,625]:
        for modifiers in [ [M_INT], [M_INT,M_UINT], [M_FLT], [M_INT,M_FLT], [M_INT,M_UINT,M_FLT] ]:
            g = NewFanout( "fanout", root, fanout_factor=fanout_factor, levels=levels, modifiers=modifiers )

            # Add various arcs to root's terminals from other roots to give terminals a variable number of incident arcs
            for side in range( int( sqrt( fanout_factor ) ) ):
                AppendFanout( "fanout", "side_%d" % side, fanout_factor=side, levels=2, modifiers=modifiers )

            for READONLY in [False, True]:
                if READONLY:
                    g.SetGraphReadonly( 60000 )

                for H_x in [1, 2, 3, 4, 5, 10, 100, -1]:
                    params['hits'] = H_x
                    size = fanout_factor * len( modifiers )
                    if H_x > size or H_x < 0:
                        hit_arcs = size
                    else:
                        hit_arcs = H_x

                    result = g.Neighborhood( rank="1", **params )
                    Expect( len(result) == hit_arcs,                "all arcs" )

                    formula = "2 * next.arc.value + next.tmc"
                    result = g.Neighborhood( rank=formula, fields=F_VAL|F_TMC|F_RANK, **params )
                    result_computed = [ 2 * x['arc']['value'] + x['created'] for x in result ]
                    Expect( is_sorted_desc( result_computed ),      "ranked result sorted descending according to formula: '%s'" % formula )
                    result_rankscore = [ x['rankscore'] for x in result ]
                    Expect( is_sorted_desc( result_rankscore ),     "ranked result sorted descending according to formula: '%s'" % formula )

                    formula = "sin( next.arc.value ) / ( next['number'] + 1 )"
                    result = g.Neighborhood( rank=formula, select=".arc.value; number; .rank", **params )
                    result_computed = [ sin( x['properties']['.arc.value'] ) / ( x['properties']['number'] + 1 ) for x in result ]
                    Expect( is_sorted_desc( result_computed ),      "ranked result sorted descending according to formula: '%s'" % formula )
                    result_rankscore = [ x['properties']['.rank'] for x in result ]
                    Expect( is_sorted_desc( result_rankscore ),     "ranked result sorted descending according to formula: '%s'" % formula )

                    formula = "prev.arc.value * next.arc.value"
                    result = g.Neighborhood( rank=formula, fields=F_RANK, collect=C_NONE, neighbor={'collect':C_COLLECT, 'arc':D_OUT }, **params )
                    result_rankscore = [ x['rankscore'] for x in result ]
                    Expect( is_sorted_desc( result_rankscore ),     "ranked result sorted descending according to formula: '%s'" % formula )

                    formula = "(prev['number'] - prev.arc.value) * vertex['number'] * (next['number'] - next.arc.value)"
                    result = g.Neighborhood( rank=formula, fields=F_RANK, collect=C_NONE, neighbor={'collect':C_COLLECT, 'arc':D_OUT }, **params )
                    result_rankscore = [ x['rankscore'] for x in result ]
                    Expect( is_sorted_desc( result_rankscore ),     "ranked result sorted descending according to formula: '%s'" % formula )

                if READONLY:
                    g.ClearGraphReadonly()

    g.DebugCheckAllocators()
    g.ClearGraphReadonly()
    g.Truncate()


 
def TEST_Vertices_rank():
    """
    Various ranking for pyvgx.Graph.Vertices()
    t_nominal=13
    test_level=3101
    """
    root = "rank_root"
    params = { 'result':R_DICT, 'sortby':S_RANK }
    levels = 1
    for fanout_factor in [1,10,100,1000,10000]:
        for modifiers in [ [M_INT], [M_INT,M_UINT], [M_FLT], [M_INT,M_FLT], [M_INT,M_UINT,M_FLT] ]:
            g = NewFanout( "fanout", root, fanout_factor=fanout_factor, levels=levels, modifiers=modifiers )

            # Add various arcs to root's terminals from other roots to give terminals a variable number of incident arcs
            for side in range( int( sqrt( fanout_factor ) ) ):
                AppendFanout( "fanout", "side_%d" % side, fanout_factor=side, levels=2, modifiers=modifiers )

            for READONLY in [False, True]:
                if READONLY:
                    g.SetGraphReadonly( 60000 )
                for H_x in [1, 2, 3, 4, 5, 10, 100, -1]:
                    params['hits'] = H_x
                    if H_x > g.order or H_x < 0:
                        hit_vertices = g.order
                    else:
                        hit_vertices = H_x

                result = g.Vertices( rank="1", **params )
                Expect( len(result) == hit_vertices,             "all vertices" )

                formula = "2 * vertex.tmc"
                result = g.Vertices( rank=formula, fields=F_TMC|F_RANK, **params )
                result_computed = [ 2 * x['created'] for x in result ]
                Expect( is_sorted_desc( result_computed ),      "ranked result sorted descending according to formula: '%s'" % formula )
                result_rankscore = [ x['rankscore'] for x in result ]
                Expect( is_sorted_desc( result_rankscore ),     "ranked result sorted descending according to formula: '%s'" % formula )

                formula = "1 / ( vertex['number'] + 1 )"
                result = g.Vertices( rank=formula, select="number; .rank", **params )
                result_computed = [ 1.0 / ( x['properties']['number'] + 1 ) for x in result ]
                Expect( is_sorted_desc( result_computed ),      "ranked result sorted descending according to formula: '%s'" % formula )
                result_rankscore = [ x['properties']['.rank'] for x in result ]
                Expect( is_sorted_desc( result_rankscore ),     "ranked result sorted descending according to formula: '%s'" % formula )

                if READONLY:
                    g.ClearGraphReadonly()

    g.DebugCheckAllocators()
    g.ClearGraphReadonly()
    g.Truncate()


 
def TEST_Arcs_rank():
    """
    Various ranking for pyvgx.Graph.Arcs()
    t_nominal=14
    test_level=3101
    """
    root = "rank_root"
    params = { 'result':R_DICT, 'sortby':S_RANK }
    levels = 1
    for fanout_factor in [1,10,100,1000,10000]:
        for modifiers in [ [M_INT], [M_INT,M_UINT], [M_FLT], [M_INT,M_FLT], [M_INT,M_UINT,M_FLT] ]:
            g = NewFanout( "fanout", root, fanout_factor=fanout_factor, levels=levels, modifiers=modifiers )

            # Add various arcs to root's terminals from other roots to give terminals a variable number of incident arcs
            for side in range( int( sqrt( fanout_factor ) ) ):
                AppendFanout( "fanout", "side_%d" % side, fanout_factor=side, levels=2, modifiers=modifiers )

            for READONLY in [False, True]:
                if READONLY:
                    g.SetGraphReadonly( 60000 )
                for H_x in [1, 2, 3, 4, 5, 10, 100, -1]:
                    params['hits'] = H_x
                    if H_x > g.size or H_x < 0:
                        hit_arcs = g.size
                    else:
                        hit_arcs = H_x

                result = g.Arcs( rank="1", **params )
                Expect( len(result) == hit_arcs,                "all arcs" )

                formula = "2 * next.tmc"
                result = g.Arcs( rank=formula, fields=F_TMC|F_RANK, **params )
                result_computed = [ 2 * x['created'] for x in result ]
                Expect( is_sorted_desc( result_computed ),      "ranked result sorted descending according to formula: '%s'" % formula )
                result_rankscore = [ x['rankscore'] for x in result ]
                Expect( is_sorted_desc( result_rankscore ),     "ranked result sorted descending according to formula: '%s'" % formula )

                formula = "1 / ( next['number'] + 1 )"
                result = g.Arcs( rank=formula, select="number; .rank", **params )
                result_computed = [ 1.0 / ( x['properties']['number'] + 1 ) for x in result ]
                Expect( is_sorted_desc( result_computed ),      "ranked result sorted descending according to formula: '%s'" % formula )
                result_rankscore = [ x['properties']['.rank'] for x in result ]
                Expect( is_sorted_desc( result_rankscore ),     "ranked result sorted descending according to formula: '%s'" % formula )

                if READONLY:
                    g.ClearGraphReadonly()

    g.DebugCheckAllocators()
    g.ClearGraphReadonly()
    g.Truncate()





def Run( name ):
    global graph
    graph = pyvgx.Graph( name )
    RunTests( [__name__] )
    graph.Truncate()
    graph.Close()
    del graph

