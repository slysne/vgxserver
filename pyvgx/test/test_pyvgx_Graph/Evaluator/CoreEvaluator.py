from pytest.pytest import RunTests, Expect, TestFailed
from pyvgx import *
import pyvgx



def TEST_vxeval():
    """
    Core vxeval
    t_nominal=19
    test_level=501
    """
    try:
        if pyvgx.system.IsInitialized():
            pyvgx.system.Unload()
        pyvgx.selftest( force=True, testroot="vgxtest", library="vgx", names=["vxeval.c"] )
    except:
        Expect( False )




def Run( name ):
    name = system.Root()
    RunTests( [__name__] )
    if not pyvgx.system.IsInitialized():
        pyvgx.system.Initialize( name, euclidean=False )
