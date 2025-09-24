from pytest.pytest import RunTests, Expect, TestFailed
from pyvgx import *
import pyvgx

graph = None


def TEST_vxevent_eventapi():
    """
    Core vxevent_eventapi
    t_nominal=289
    test_level=501
    """
    try:
        pyvgx.selftest( force=True, testroot="vgxtest", library="vgx", names=["vxevent_eventapi.c"] )
    except:
        Expect( False )



def TEST_vxevent_eventmon():
    """
    Core vxevent_eventmon
    test_level=501
    """
    try:
        pyvgx.selftest( force=True, testroot="vgxtest", library="vgx", names=["vxevent_eventmon.c"] )
    except:
        Expect( False )



def TEST_vxevent_eventexec():
    """
    Core vxevent_eventexec
    test_level=501
    """
    try:
        pyvgx.selftest( force=True, testroot="vgxtest", library="vgx", names=["vxevent_eventexec.c"] )
    except:
        Expect( False )



def TEST_EventBacklog():
    """
    pyvgx.Graph.EventBacklog()
    Call method
    test_level=3101
    """
    graph.EventBacklog()



def TEST_EventDisable():
    """
    pyvgx.Graph.EventDisable()
    Call method
    test_level=3101
    """
    graph.EventDisable()

    # restore
    graph.EventEnable()



def TEST_EventEnable():
    """
    pyvgx.Graph.EventEnable()
    Call method
    test_level=3101
    """
    graph.EventEnable()



def TEST_EventFlush():
    """
    pyvgx.Graph.EventFlush()
    Call method
    test_level=3101
    """
    graph.EventFlush()



def Run( name ):
    global graph
    graph = pyvgx.Graph( name )
    RunTests( [__name__] )
    graph.Close()
    del graph

