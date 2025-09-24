from pytest.pytest import RunTests, Expect, TestFailed
from . import _query_test_support as QuerySupport
from pyvgx import *
import pyvgx


def TEST_Outarcs():
    """
    pyvgx.Graph.Outarcs()
    test_level=3101
    """
    levels = 1
    for fanout_factor in [1,10,100]:
        for modifiers in [ [M_INT], [M_INT,M_UINT], [M_FLT], [M_INT,M_FLT], [M_INT,M_UINT,M_FLT] ]:
            g = QuerySupport.NewFanout( "fanout", "root", fanout_factor=fanout_factor, levels=levels, modifiers=modifiers )
            root = "root"
            outarcs = g.Outarcs( root )
            outdegree = fanout_factor * len( modifiers )
            Expect( len( outarcs ) == outdegree,            "%d outarcs" % outdegree )

            Expect( len( g.Outarcs( root, hits=0 ) ) == 0,  "hits = 0" )
            Expect( len( g.Outarcs( root, hits=1 ) ) == 1,  "hits = 1" )

            terminals = set( [term for arcdir, rel, mod, val, term in outarcs] )
            expect_terminals = set( ["level_1_%d" % x for x in range( fanout_factor )] )
            Expect( terminals == expect_terminals,          "all terminals" )
    g.Truncate()




def Run( name ):
    RunTests( [__name__] )

