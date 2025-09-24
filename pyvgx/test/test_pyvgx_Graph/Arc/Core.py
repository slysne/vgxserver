from pytest.pytest import RunTests, Expect, TestFailed
import time
from pyvgx import *
import pyvgx

graph = None


def TEST_vxarcvector_comparator():
    """
    Core vxarcvector_comparator
    test_level=501
    """
    try:
        pyvgx.selftest( force=True, testroot="vgxtest", library="vgx", names=["vxarcvector_comparator.c"] )
    except:
        Expect( False )



def TEST_vxarcvector_filter():
    """
    Core vxarcvector_filter
    test_level=501
    """
    try:
        pyvgx.selftest( force=True, testroot="vgxtest", library="vgx", names=["vxarcvector_filter.c"] )
    except:
        Expect( False )



def TEST_vxarcvector_fhash():
    """
    Core vxarcvector_fhash
    test_level=501
    """
    try:
        pyvgx.selftest( force=True, testroot="vgxtest", library="vgx", names=["vxarcvector_fhash.c"] )
    except:
        Expect( False )



def TEST_vxarcvector_cellproc():
    """
    Core vxarcvector_cellproc
    test_level=501
    """
    try:
        pyvgx.selftest( force=True, testroot="vgxtest", library="vgx", names=["vxarcvector_cellproc.c"] )
    except:
        Expect( False )



def TEST_vxarcvector_traverse():
    """
    Core vxarcvector_traverse
    test_level=501
    """
    try:
        pyvgx.selftest( force=True, testroot="vgxtest", library="vgx", names=["vxarcvector_traverse.c"] )
    except:
        Expect( False )



def TEST_vxarcvector_exists():
    """
    Core vxarcvector_exists
    test_level=501
    """
    try:
        pyvgx.selftest( force=True, testroot="vgxtest", library="vgx", names=["vxarcvector_exists.c"] )
    except:
        Expect( False )



def TEST_vxarcvector_delete():
    """
    Core vxarcvector_delete
    test_level=501
    """
    try:
        pyvgx.selftest( force=True, testroot="vgxtest", library="vgx", names=["vxarcvector_delete.c"] )
    except:
        Expect( False )



def TEST_vxarcvector_expire():
    """
    Core vxarcvector_expire
    test_level=501
    """
    try:
        pyvgx.selftest( force=True, testroot="vgxtest", library="vgx", names=["vxarcvector_expire.c"] )
    except:
        Expect( False )



def TEST_vxarcvector_dispatch():
    """
    Core vxarcvector_dispatch
    test_level=501
    """
    try:
        pyvgx.selftest( force=True, testroot="vgxtest", library="vgx", names=["vxarcvector_dispatch.c"] )
    except:
        Expect( False )



def TEST_vxarcvector_api():
    """
    Core vxarcvector_api
    t_nominal=1303
    test_level=502
    """
    try:
        pyvgx.selftest( force=True, testroot="vgxtest", library="vgx", names=["vxarcvector_api.c"] )
    except:
        Expect( False )



def TEST_vxgraph_arc():
    """
    Core vxgraph_arc
    test_level=501
    """
    try:
        pyvgx.selftest( force=True, testroot="vgxtest", library="vgx", names=["vxgraph_arc.c"] )
    except:
        Expect( False )




def Run( name ):
    global graph
    graph = pyvgx.Graph( name )
    RunTests( [__name__] )
    graph.Truncate()
    graph.Close()
    del graph

