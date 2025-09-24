from pytest.pytest import RunTests, Expect, TestFailed
from . import _query_test_support as QuerySupport
from pyvgx import *
import pyvgx




def TEST_Initials():
    """
    pyvgx.Graph.Initials()
    test_level=3101
    """
    levels = 1
    for fanout_factor in [1,10,100]:
        current_terminals = ["level_1_%d" % fx for fx in range(fanout_factor)]
        expect_terminals = set( current_terminals )
        for modifiers in [ [M_INT], [M_INT,M_UINT], [M_FLT], [M_INT,M_FLT], [M_INT,M_UINT,M_FLT] ]:
            g = QuerySupport.NewFanout( "fanout", "root", fanout_factor=fanout_factor, levels=levels, modifiers=modifiers )
            root = "root"

            for fx in range( fanout_factor ):
                 single = g.Initials( "level_1_%d" % fx )
                 Expect( len(single) == 1 and single[0] == root,        "root is only initial" )
    g.Truncate()







def Run( name ):
    RunTests( [__name__] )

