from pytest.pytest import RunTests, Expect, TestFailed
from . import _query_test_support as QuerySupport
from pyvgx import *
import pyvgx




def TEST_Search():
    """
    pyvgx.Graph.Search()
    test_level=3101
    """
    pyvgx.SetOutputStream( "TEST_Search_output.txt" )
    levels = 2
    for fanout_factor in [1,10,100]:
        for modifiers in [ [M_INT], [M_INT,M_UINT], [M_FLT], [M_INT,M_FLT], [M_INT,M_UINT,M_FLT] ]:
            g = QuerySupport.NewFanout( "fanout", "root", fanout_factor=fanout_factor, levels=levels, modifiers=modifiers )
            root = "root"
            # Make sure nothing crashes or raises exceptions
            g.Search( root, hits=3, offset=2, sortby=S_ID, arc=("*",D_OUT) )
            g.Search( "level_1_0", hits=3, offset=2, sortby=S_ID, arc=("*",D_ANY) )

    pyvgx.SetOutputStream( None )
    g.Truncate()



def Run( name ):
    RunTests( [__name__] )

