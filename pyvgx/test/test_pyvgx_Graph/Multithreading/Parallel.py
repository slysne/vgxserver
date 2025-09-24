from pytest.pytest import RunTests, Expect, TestFailed, PerformCleanup
from pytest.threads import Worker
from pyvgx import *
import pyvgx
#from pyframehash import *
import random
import time



graph_name = "parallel"



def get_graph( create=False  ):
    g = Graph( graph_name )
    if create:
        g.Truncate()
        for n in range( 1000000 ):
            for i in range( 10 ):
                g.Connect( str(n), "to", str(n+i) )
                g.Connect( str(i), ("index",M_INT,n), str(n) )


    return g



def arcs( g ):
    t0 = timestamp()
    g.Arcs( hits=10, sortby=S_DEG )
    t1 = timestamp()
    LogInfo( "Arcs(): %.2fs" % (t1-t0) )



def vertices( g ):
    t0 = timestamp()
    g.Vertices( hits=10, sortby=S_DEG )
    t1 = timestamp()
    LogInfo( "Vertices(): %.2fs" % (t1-t0) )



def neighborhood( g ):
    root = str( random.randint(0,9) )
    t0 = timestamp()
    g.Neighborhood( root, hits=10, arc=("index",D_OUT,M_INT), sortby=S_VAL )
    t1 = timestamp()
    LogInfo( "Neighborhood(): %.2fs" % (t1-t0) )



def terminate( self ):
    self.terminate()



def TEST_Parallel_Writable():
    """
    Test concurrent pyvgx.Vertices(), pyvgx.Arcs() and pyvgx.Neighborhood() in WRITABLE graph
    t_nominal=177
    test_level=3201
    """

    g = get_graph( create=True )
    g.DebugCheckAllocators()

    W = [ Worker(str(x)) for x in range(8) ]

    for i in range(5):
        for w in W:
            w.perform( arcs, g )
        for w in W:
            w.perform( vertices, g )
        for w in W:
            w.perform( neighborhood, g )


    for w in W:
        w.perform( terminate, w )

    
    while len([1 for w in W if not w.is_dead()]):
        time.sleep( 1 )

    g.DebugCheckAllocators()



def TEST_Parallel_Readonly():
    """
    Test concurrent pyvgx.Vertices(), pyvgx.Arcs() and pyvgx.Neighborhood() in READONLY graph
    t_nominal=36
    test_level=3201
    """

    g = get_graph()
    g.DebugCheckAllocators()

    g.SetGraphReadonly( 60000 )
    g.DebugCheckAllocators()

    W = [ Worker(str(x)) for x in range(8) ]

    for i in range(15):
        for w in W:
            w.perform( arcs, g )
        for w in W:
            w.perform( vertices, g )
        for w in W:
            w.perform( neighborhood, g )


    for w in W:
        w.perform( terminate, w )

    
    while len([1 for w in W if not w.is_dead()]):
        time.sleep( 1 )

    g.ClearGraphReadonly()

    g.DebugCheckAllocators()
    g.Truncate()
    g.DebugCheckAllocators()

    g.Erase()



def Run( name ):
    RunTests( [__name__] )
    PerformCleanup()

