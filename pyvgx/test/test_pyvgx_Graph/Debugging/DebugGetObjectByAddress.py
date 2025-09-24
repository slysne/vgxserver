from pytest.pytest import RunTests, Expect, TestFailed
from pyvgx import *
import pyvgx




def TEST_DebugGetObjectByAddress():
    """
    pyvgx.Graph.DebugGetObjectByAddress()
    test_level=3101
    """
    graph = Graph( "debug" )
    graph.Truncate()

    for n in range( 100000 ):
        V = graph.NewVertex( "vertex_%d" % n )
        graph.CloseVertex( V )

    for n in range( 100 ):
        vertices = graph.Vertices( hits=100, sortby=S_RANDOM, result=R_DICT, fields=F_ID, select=".address" )
        for v in vertices:
            id = v['id']
            prop = v['properties']
            addr = prop['.address']
            objrepr = graph.DebugGetObjectByAddress( addr )
            Expect( objrepr.startswith( "<object at 0x%x" % addr ) )


    graph.Erase()



def Run( name ):
    RunTests( [__name__] )

