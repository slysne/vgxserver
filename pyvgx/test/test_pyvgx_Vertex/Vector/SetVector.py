from pytest.pytest import RunTests, Expect, TestFailed
from pyvgx import *
import pyvgx

graph = None



def TEST_SetVector():
    """
    pyvgx.Vertex.SetVector()
    Basic
    test_level=3101
    """

    if "vertex" in graph:
        graph.DeleteVertex( "vertex" )

    V = graph.NewVertex( "vertex" )

    V.SetVector( [] )
    Expect( V.GetVector().external == [],                               "empty" )
    # TODO: fix such that V.GetVector() == [] also

    V.SetVector( [("a",1)] )
    Expect( V.GetVector().external == [("a",1.0)],                      "one element" )

    V.SetVector( [("a",1),("b",0.5)] )
    Expect( V.GetVector().external == [("a",1.0),("b",0.5)],            "two elements" )

    V.SetVector( [("a",1),("b",0.5),("c",0.25)] )
    Expect( V.GetVector().external == [("a",1.0),("b",0.5),("c",0.25)], "three elements" )

    V.SetVector( [("a",1),("b",0.5)] )
    Expect( V.GetVector().external == [("a",1.0),("b",0.5)],            "two elements" )

    V.SetVector( [("a",1)] )
    Expect( V.GetVector().external == [("a",1.0)],                      "one element" )

    V.SetVector( [] )
    Expect( V.GetVector().external == [],                               "empty" )



def TEST_SetVector_limits():
    """
    pyvgx.Vertex.SetVector()
    Limits
    test_level=3101
    """

    if "vertex" in graph:
        graph.DeleteVertex( "vertex" )

    V = graph.NewVertex( "vertex" )

    MAX = graph.sim.max_vector_size

    for vlen in range( 2, MAX+10 ):
        c = ord( "A" )
        for termsz in range( 1, 28 ):
            vec = []
            for n in range( vlen ):
                if n == 0:
                    vec.append( ("<",2) ) # start marker
                elif n == vlen-1:
                    vec.append( (">",2) ) # end marker
                else:
                    term = chr(c) * termsz
                    weight = 2 * float(vlen-n) / vlen
                    vec.append( (term,weight) )
            V.SetVector( vec )

            if vlen <= MAX:
                Expect( V.GetVector().length == vlen,       "correct length" )
            else:
                vlen = MAX
                Expect( V.GetVector().length == MAX,         "truncated" )

            if termsz <= 27:
                for n in range( 1, vlen-1 ):
                    v_vec = V.GetVector().external[n][0]
                    vec_n = vec[n][0]
                    Expect( V.GetVector().external[n][0] == vec[n][0],      "expected '%s', got '%s' / vector=%s" % (vec_n, v_vec, V.GetVector()) )
            else:
                for n in range( 1, vlen-1 ):
                    Expect( V.GetVector().external[n][0] == vec[n][0][:27], "correct term prefix" )











def Run( name ):
    global graph
    graph = pyvgx.Graph( name )
    RunTests( [__name__] )
    graph.Close()
    del graph

