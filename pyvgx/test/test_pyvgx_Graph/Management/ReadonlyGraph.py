from pytest.pytest import RunTests, Expect, TestFailed
from pyvgx import *
import pyvgx

graph = None


def TEST_IsGraphReadonly():
    """
    pyvgx.Graph.IsGraphReadonly()
    Call method
    test_level=3101
    """
    graph.IsGraphReadonly()



def TEST_ClearGraphReadonly():
    """
    pyvgx.Graph.ClearGraphReadonly()
    Call method
    test_level=3101
    """
    graph.ClearGraphReadonly()



def TEST_SetGraphReadonly():
    """
    pyvgx.Graph.SetGraphReadonly()
    test_level=3101
    """
    graph.Truncate()

    graph.CreateVertex( "A" )
    graph.CreateVertex( "B" )
    graph.CreateVertex( "C" )

    Expect( graph.Connect( "A", ("to",M_CNT,1), "B" ) == 1,         "Arc created" )
    Expect( graph.Count( "A", "to", "B" ) == 2,                     "Arc incremented" )

    Expect( graph.order == 3,   "Three vertices" )
    Expect( graph.size == 1,    "One arc" )

    graph.SetGraphReadonly( 60000 )

    try:
        graph.Connect( "B", "to", "C" )
        Expect( False,  "Should not be able to create arc in readonly mode" )
    except pyvgx.AccessError:
        Expect( graph.size == 1, "Still one arc in graph" )
    except:
        raise

    try:
        graph.Disconnect( "B" )
        Expect( False,  "Should not be able to delete arc in readonly mode" )
    except pyvgx.AccessError:
        Expect( graph.size == 1, "Still one arc in graph" )
    except:
        raise

    try:
        graph.Count( "A", "to", "B" )
        Expect( False,  "Should not be able to modify arc in readonly mode" )
    except pyvgx.AccessError:
        Expect( graph.size == 1, "Still one arc in graph" )
    except:
        raise

    try:
        graph.CreateVertex( "D" )
        Expect( False,  "Should not be able to create vertex in readonly mode" )
    except pyvgx.AccessError:
        Expect( graph.order == 3, "Still three vertices in graph" )
    except:
        raise

    try:
        graph.DeleteVertex( "C" )
        Expect( False,  "Should not be able to delete vertex in readonly mode" )
    except pyvgx.AccessError:
        Expect( graph.order == 3, "Still three vertices in graph" )
    except:
        raise

    try:
        graph.Truncate()
        Expect( False,  "Should not be able to truncate graph in readonly mode" )
    except pyvgx.AccessError:
        Expect( graph.order == 3, "Still three vertices in graph" )
        Expect( graph.size == 1, "Still one arc in graph" )
    except:
        raise
    
    Expect( set( graph.Neighborhood( "A" ) ) == set( ["B"] ),  "Queries allowed in readonly mode" )
    Expect( set( graph.Vertices() ) == set( ["A", "B", "C"] ), "Queries allowed in readonly mode" )

    graph.ClearGraphReadonly()

    Expect( graph.CreateVertex( "D" ) == 1,             "No longer readonly" )
    Expect( graph.Connect( "C", "to", "D" ) == 1,       "No longer readonly" )
    
    graph.Truncate()

    Expect( graph.order == 0 and graph.size == 0,       "Empty graph" )







def Run( name ):
    global graph
    graph = pyvgx.Graph( name )
    RunTests( [__name__] )
    graph.Close()
    del graph

