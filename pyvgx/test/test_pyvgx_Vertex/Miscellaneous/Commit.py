from pytest.pytest import RunTests, Expect, TestFailed
from pyvgx import *
import pyvgx

graph = None



def TEST_Commit():
    """
    pyvgx.Vertex.Commit()
    Basic
    test_level=3101
    """
    
    # Reset
    graph.Truncate()

    graph.CreateVertex( "A" )
    graph.CreateVertex( "B" )

    # Writable A
    A = graph.OpenVertex( "A", "w" )
    # Readonly B
    B = graph.OpenVertex( "B", "r" )

    # A's opcount should not increment when no changes are made
    op1 = A.Commit()
    Expect( op1 > 0 )
    op2 = A.Commit()
    Expect( op2 == op1 )
    # A's opcount should increment after a change is made
    A['x'] = 1
    op3 = A.Commit()
    Expect( op3 > op2 )
    graph.CloseVertex( A )

    # B is readonly and can't be committed
    op = B.Commit()
    Expect( op < 0 )
    graph.CloseVertex( B )






def Run( name ):
    global graph
    graph = pyvgx.Graph( name )
    RunTests( [__name__] )
    graph.Close()
    del graph


