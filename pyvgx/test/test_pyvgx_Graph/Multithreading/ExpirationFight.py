from pytest.pytest import RunTests, Expect, TestFailed, PerformCleanup
from pytest.threads import Worker
from pyvgx import *
import pyvgx
#from pyframehash import *
import random
import time



RUNNING = True
DURATION = 600 # 10 minutes
#DURATION = 18*3600

graph_name = "expiration_fight"

N_PAIRS_0 = 3000000
N_NODES_0 = 7000000

N_PAIRS = N_PAIRS_0
N_NODES = N_NODES_0


def TEST_ExpirationFight():
    """
    t_nominal=660
    test_level=3701
    """
    global RUNNING
    global N_PAIRS
    global N_NODES

    def connect( graph ):
        global RUNNING
        global N_PAIRS
        global N_NODES

        n = 0

        while RUNNING:
            if N_PAIRS < 1 or N_NODES < 1:
                return
            try:
                n += 1
                A = None
                T = None

                # Current time
                ts = int( graph.ts )

                # Terminal
                term = "term_%d" % random.randint( 1, N_PAIRS )
                T = graph.NewVertex( term, type="terminal", timeout=1000 )

                # Get the initial of terminal (if one exists) and open it
                current = graph.Neighborhood( T, hits=1, arc=("current",D_IN), result=R_LIST, fields=F_VAL|F_ID, timeout=1000 )
                if current:
                    ts, init = current[0]
                    A = graph.OpenVertex( init, mode="a", timeout=1000 )
                    if random.random() > 0.99:
                        graph.Disconnect( A, arc=("current",D_OUT,M_TMM), neighbor=T, timeout=1000 )
                        graph.DeleteVertex( A.id, timeout=1000 )
                        graph.CloseVertex( A )
                        A = None
                # No initial of terminal, make new and connect it to terminal
                if A is None:
                    init = "init_%s" % pyvgx.strhash128( "%s_%d" % (T.id, ts) )
                    A = graph.NewVertex( init, type="initial", timeout=1000 )
                    graph.Connect( A, ("to",M_TMC,ts), T )
                    A['ts'] = ts
                    A['n'] = n

                # Update current arc time from initial to terminal
                graph.Connect( A, ("current",M_TMM,ts), T )

                # Set initial expiration
                A.SetExpiration( random.randint(1,600), True )

                assert A.indegree == 0

                node = "node_%d" % random.randint( 1, N_NODES )
                N = graph.NewVertex( node, type="node", timeout=1000 )
                if not graph.Adjacent( A, ("to",D_OUT,M_TMC), N ):
                    graph.Connect( A, ("to",M_TMC,ts), N )
                graph.CloseVertex( N )
                graph.CloseVertex( A )
                graph.CloseVertex( T )
            except Exception as ex:
                print(ex)
            


    def add_remove( graph ):
        global RUNNING
        n = 0
        while RUNNING:
            if N_NODES < 1:
                return
            try:
                n += 1
                node = "node_%d" % random.randint( 1, N_NODES )
                N = graph.NewVertex( node, type="node", timeout=1000 )
                N.SetExpiration( 1 + n % 500, True )
                graph.CloseVertex( N )
            except Exception as ex:
                print(ex)



    graph = Graph( graph_name )
    graph.Truncate()
    LogInfo( "Running expiration fight for %d seconds" % DURATION )
    LogInfo( "%s" % graph )
    LogInfo( "%s" % graph.EventBacklog() )
    Connecter = Worker( "Connecter" )
    Connecter.perform_sync( 1000, threadlabel, "Connecter" )
    Connecter.perform( connect, graph )
    AddRemover = Worker( "AddRemover" )
    AddRemover.perform_sync( 1000, threadlabel, "AddRemover" )
    AddRemover.perform( add_remove, graph )

    t0 = t1 = time.time()
    t_end = t0 + DURATION
    try:
        t2 = t1
        while t1 - t0 < DURATION:
            t1 = time.time()
            time.sleep( 0.1 )
            if t1 - t2 > 5:
                LogInfo( "%s (p=%d n=%d)" % (graph, N_PAIRS, N_NODES) )
                LogInfo( "%s" % graph.EventBacklog() )
                t2 += 5
                factor = float(t_end - t1) / DURATION
                N_PAIRS = int( N_PAIRS_0 * factor )
                N_NODES = int( N_NODES_0 * factor )
    except KeyboardInterrupt:
        LogInfo( "User cancelled" )

    LogInfo( "Stopping expiration fight" )
    LogInfo( "%s" % graph )
    LogInfo( "%s" % graph.EventBacklog() )
    RUNNING = False
    Connecter.terminate()
    AddRemover.terminate()
    t1 = time.time()
    while not Connecter.is_dead() or not AddRemover.is_dead():
        time.sleep( 0.1 )
        if time.time() - t1 > 10:
            break

    if Connecter.is_dead():
        LogInfo( "Connecter stopped" )
    else:
        LogWarning( "Failed to stop Connecter" )
    if AddRemover.is_dead():
        LogInfo( "AddRemover stopped" )
    else:
        LogWarning( "Failed to stop AddRemover" )
    
    graph.EventDisable()
    graph.Truncate()
    graph.EventEnable()

    LogInfo( "%s" % graph )
    LogInfo( "%s" % graph.EventBacklog() )

    graph.Erase()





def Run( name ):
    RunTests( [__name__] )
    PerformCleanup()

