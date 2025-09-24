from pytest.pytest import RunTests, Expect, TestFailed
from . import _query_test_support as QuerySupport
from pyvgx import *
import pyvgx




def TEST_ArcValue():
    """
    pyvgx.Graph.ArcValue()
    test_level=3201
    """
    
    levels = 2
    for fanout_factor in [1,10,100]:
        for modifiers in [ [M_INT], [M_INT,M_UINT], [M_FLT], [M_INT,M_FLT], [M_INT,M_UINT,M_FLT] ]:
            g = QuerySupport.NewFanout( "fanout", "arcvalue_root", fanout_factor=fanout_factor, levels=levels, modifiers=modifiers )
            root = "arcvalue_root"
            if M_INT in modifiers:
                for fx in range( fanout_factor ):
                    Expect( g.ArcValue( root, ("to_level_1", D_OUT, M_INT), "level_1_%d" % fx ) == fx,    "M_INT arc value = %d" % fx )
            if M_FLT in modifiers:
                for fx in range( fanout_factor ):
                    Expect( g.ArcValue( root, ("to_level_1", D_OUT, M_FLT), "level_1_%d" % fx ) == float(fx),    "M_FLT arc value = %f" % float(fx) )
    g.Truncate()




 



def Run( name ):
    RunTests( [__name__] )

