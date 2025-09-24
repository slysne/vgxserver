from pytest.pytest import RunTests, Expect, TestFailed
from pyvgx import *
import pyvgx

graph = None


def TEST_vxenum():
    """
    Core vxenum
    test_level=501
    """
    try:
        pyvgx.selftest( force=True, testroot="vgxtest", library="vgx", names=["vxenum.c"] )
    except:
        Expect( False )



def TEST_vxenum_vtx():
    """
    Core vxenum_vtx
    test_level=501
    """
    try:
        pyvgx.selftest( force=True, testroot="vgxtest", library="vgx", names=["vxenum_vtx.c"] )
    except:
        Expect( False )



def TEST_vxenum_rel():
    """
    Core vxenum_rel
    test_level=501
    """
    try:
        pyvgx.selftest( force=True, testroot="vgxtest", library="vgx", names=["vxenum_rel.c"] )
    except:
        Expect( False )



def TEST_vxenum_dim():
    """
    Core vxenum_dim
    t_nominal=44
    test_level=501
    """
    try:
        pyvgx.selftest( force=True, testroot="vgxtest", library="vgx", names=["vxenum_dim.c"] )
    except:
        Expect( False )



def TEST_vxenum_propkey():
    """
    Core vxenum_propkey
    test_level=501
    """
    try:
        pyvgx.selftest( force=True, testroot="vgxtest", library="vgx", names=["vxenum_propkey.c"] )
    except:
        Expect( False )



def TEST_vxenum_propval():
    """
    Core vxenum_propval
    test_level=501
    """
    try:
        pyvgx.selftest( force=True, testroot="vgxtest", library="vgx", names=["vxenum_propval.c"] )
    except:
        Expect( False )



def Run( name ):
    global graph
    graph = pyvgx.Graph( name )
    RunTests( [__name__] )
    graph.Close()
    del graph

