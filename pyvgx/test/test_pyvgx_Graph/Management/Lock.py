from pytest.pytest import RunTests, Expect, TestFailed
from pyvgx import *
import pyvgx
import re
import time
from pytest.threads import Worker

graph = None




def TEST_Lock():
    """
    pyvgx.Graph.Lock()
    Basic tests
    test_level=3101
    """

    # Anonymous lock
    anon = graph.Lock()
    anon_id = anon.id

    Expect( anon_id in graph,                       "Lock exists" )
    Expect( anon.IsExpired(),                       "Lock vertex should already be expired" )
    Expect( re.match( "[a-z0-9]{32}", anon_id ),    "Lock auto id should be 32-digit hex string, got %s" % anon_id ) 
    Expect( anon.Writable(),                        "Lock should be acquired writable" )
    Expect( anon.type == "lock_",                   "Lock type should be 'lock_', got '%s'" % anon.type )

    anon.Close()
    time.sleep(2)
    Expect( anon_id not in graph,                   "Lock should not exist" )

    # Lock with identifier
    name = "lock_1"
    lock = graph.Lock( name )
    lock_id = lock.id

    Expect( lock_id in graph,                       "Lock exists" )
    Expect( lock.IsExpired(),                       "Lock vertex should already be expired" )
    Expect( name in lock_id,                        "Lock vertex id should contain %s" % name ) 
    Expect( lock.Writable(),                        "Lock should be acquired writable" )
    Expect( lock.type == "lock_",                   "Lock type should be 'lock_', got '%s'" % lock.type )

    lock.Close()
    time.sleep(2)
    Expect( lock_id not in graph,                   "Lock should not exist" )



LOCKS = {}


def acquire( id, timeout ):
    try:
        LOCKS[id] = graph.Lock( id, timeout )
        return LOCKS[id]
    except:
        return None


def release( id ):
    try:
        lock = LOCKS.pop(id)
        lock.Close()
        return True
    except:
        return False


def TEST_Lock_timeout():
    """
    pyvgx.Graph.Lock()
    Access and timeout tests
    test_level=3101
    """

    worker = Worker( "other_thread" )

    # Main thread acquires mutex
    mutex = acquire( "mutex", 0 )
    Expect( mutex is not None,              "Main thread should acquire mutex" )
    Expect( len(LOCKS) == 1,                "One lock exists" )

    # Other thread tries (and fails) to acquire mutex
    worker.perform_sync( 1.0, acquire, "mutex", 0 )
    Expect( worker.collect() is None,       "Other thread should immediately fail to acquire mutex" )

    worker.perform_sync( 5.0, acquire, "mutex", 2000 )
    Expect( worker.collect() is None,       "Other thread should fail to acquire mutex after timeout" )

    # Main thread gives up mutex
    Expect( release( "mutex" ) == True,     "Main thread should release mutex" )
    Expect( len(LOCKS) == 0,                "Zero locks exists" )
    
    # Other thread acquires mutex
    worker.perform_sync( 5.0, acquire, "mutex", 2000 )
    Expect( worker.collect() is not None,   "Other thread should acquire mutex" )
    Expect( len(LOCKS) == 1,                "One lock exists" )

    # Main thread tries (and fails) to acquire mutex
    mutex = acquire( "mutex", 1000 ) 
    Expect( mutex is None,                  "Main thread should fail to acquire mutex" )

    # Other thread gives up mutex
    worker.perform_sync( 1.0, release, "mutex" )
    Expect( worker.collect() is True,       "Other thread should release mutex" )
    Expect( len(LOCKS) == 0,                "Zero locks exists" )

 



def Run( name ):
    global graph
    graph = pyvgx.Graph( name )
    RunTests( [__name__] )
    graph.Close()
    del graph

