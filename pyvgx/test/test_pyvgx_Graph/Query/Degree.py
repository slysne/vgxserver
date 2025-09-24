from pytest.pytest import RunTests, Expect, TestFailed
from . import _query_test_support as QuerySupport
from pyvgx import *
import pyvgx




def TEST_Degree():
    """
    pyvgx.Graph.Degree()
    test_level=3201
    """
    levels = 2
    for fanout_factor in [1,10,100]:
        for modifiers in [ [M_INT], [M_INT,M_UINT], [M_FLT], [M_INT,M_FLT], [M_INT,M_UINT,M_FLT] ]:
            g = QuerySupport.NewFanout( "fanout", "degree_root", fanout_factor=fanout_factor, levels=levels, modifiers=modifiers )
            root = "degree_root"

            outdegree = fanout_factor * len( modifiers )
            Expect( g.Degree( root ) == outdegree,                          "root degree = %d" % outdegree )
            Expect( g.Degree( root, D_OUT ) == outdegree,                   "root outdegree = %d" % outdegree )
            Expect( g.Degree( root, D_IN ) == 0,                            "root indegree = 0" )

            indegree = len( modifiers )
            Expect( g.Degree( "level_1_0", D_IN ) == indegree,              "level_1_0 has %d inarc(s) (from root)" % indegree )

            outdegree = 2*fanout_factor * len( modifiers )
            Expect( g.Degree( "level_1_0", D_OUT ) == outdegree,            "level_1_0 has %d outarcs" % outdegree )

            Expect( g.Degree( "level_1_0" ) == indegree + outdegree,        "level_1_0 has %d arcs" % (indegree + outdegree) )

            if M_INT in modifiers:
                Expect( g.Degree( "level_1_0", ("to_level_2", D_OUT, M_INT, V_EQ, 0) ) == 1,                    "level_1_0 has one M_INT arc with value = 0" )
                Expect( g.Degree( "level_1_0", ("to_level_2", D_OUT, M_INT, V_GT, 0) ) == 2*fanout_factor-1,    "level_1_0 has %d M_INT arcs with value > 0" % (2*fanout_factor-1) )
    g.Truncate()



def Run( name ):
    RunTests( [__name__] )

