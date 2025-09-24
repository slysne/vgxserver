from pytest.pytest import RunTests, Expect, TestFailed
from pyvgx import *
import pyvgx





def TEST_comlib_COMLIB():
    """
    Core comlib COMLIB
    t_nominal=7
    test_level=101
    """
    try:
        pyvgx.selftest( force=True, testroot="comlib", library="comlib", names=["COMLIB"] )
    except:
        Expect( False )
 


def TEST_comlib_cxcstring():
    """
    Core comlib cxcstring
    t_nominal=16
    test_level=101
    """
    try:
        pyvgx.selftest( force=True, testroot="comlib", library="comlib", names=["cxcstring.c"] )
    except:
        Expect( False )



def TEST_comlib_CStringQueue_t():
    """
    Core comlib CStringQueue_t
    test_level=101
    """
    try:
        pyvgx.selftest( force=True, testroot="comlib", library="comlib", names=["CStringQueue_t"] )
    except:
        Expect( False )



def TEST_comlib_CByteQueue_t():
    """
    Core comlib CByteQueue_t
    test_level=101
    """
    try:
        pyvgx.selftest( force=True, testroot="comlib", library="comlib", names=["CByteQueue_t"] )
    except:
        Expect( False )



def TEST_comlib_CWordQueue_t():
    """
    Core comlib CWordQueue_t
    test_level=101
    """
    try:
        pyvgx.selftest( force=True, testroot="comlib", library="comlib", names=["CWordQueue_t"] )
    except:
        Expect( False )



def TEST_comlib_CDwordQueue_t():
    """
    Core comlib CDwordQueue_t
    test_level=101
    """
    try:
        pyvgx.selftest( force=True, testroot="comlib", library="comlib", names=["CDwordQueue_t"] )
    except:
        Expect( False )



def TEST_comlib_CQwordQueue_t():
    """
    Core comlib CQwordQueue_t
    test_level=101
    """
    try:
        pyvgx.selftest( force=True, testroot="comlib", library="comlib", names=["CQwordQueue_t"] )
    except:
        Expect( False )



def TEST_comlib_Cm128iQueue_t():
    """
    Core comlib Cm128iQueue_t
    test_level=101
    """
    try:
        pyvgx.selftest( force=True, testroot="comlib", library="comlib", names=["Cm128iQueue_t"] )
    except:
        Expect( False )



def TEST_comlib_Cm256iQueue_t():
    """
    Core comlib Cm256iQueue_t
    test_level=101
    """
    try:
        pyvgx.selftest( force=True, testroot="comlib", library="comlib", names=["Cm256iQueue_t"] )
    except:
        Expect( False )



def TEST_comlib_Cm512iQueue_t():
    """
    Core comlib Cm512iQueue_t
    test_level=101
    """
    try:
        pyvgx.selftest( force=True, testroot="comlib", library="comlib", names=["Cm512iQueue_t"] )
    except:
        Expect( False )



def TEST_comlib_CtptrQueue_t():
    """
    Core comlib CtptrQueue_t
    test_level=101
    """
    try:
        pyvgx.selftest( force=True, testroot="comlib", library="comlib", names=["CtptrQueue_t"] )
    except:
        Expect( False )



def TEST_comlib_Cx2tptrQueue_t():
    """
    Core comlib Cx2tptrQueue_t
    test_level=101
    """
    try:
        pyvgx.selftest( force=True, testroot="comlib", library="comlib", names=["Cx2tptrQueue_t"] )
    except:
        Expect( False )



def TEST_comlib_CaptrQueue_t():
    """
    Core comlib CaptrQueue_t
    test_level=101
    """
    try:
        pyvgx.selftest( force=True, testroot="comlib", library="comlib", names=["CaptrQueue_t"] )
    except:
        Expect( False )



def TEST_comlib_CByteHeap_t():
    """
    Core comlib CByteHeap_t
    test_level=101
    """
    try:
        pyvgx.selftest( force=True, testroot="comlib", library="comlib", names=["CByteHeap_t"] )
    except:
        Expect( False )



def TEST_comlib_CWordHeap_t():
    """
    Core comlib CWordHeap_t
    test_level=101
    """
    try:
        pyvgx.selftest( force=True, testroot="comlib", library="comlib", names=["CWordHeap_t"] )
    except:
        Expect( False )



def TEST_comlib_CDwordHeap_t():
    """
    Core comlib CDwordHeap_t
    test_level=101
    """
    try:
        pyvgx.selftest( force=True, testroot="comlib", library="comlib", names=["CDwordHeap_t"] )
    except:
        Expect( False )



def TEST_comlib_CQwordHeap_t():
    """
    Core comlib CQwordHeap_t
    test_level=101
    """
    try:
        pyvgx.selftest( force=True, testroot="comlib", library="comlib", names=["CQwordHeap_t"] )
    except:
        Expect( False )



def TEST_comlib_CtptrHeap_t():
    """
    Core comlib CtptrHeap_t
    test_level=101
    """
    try:
        pyvgx.selftest( force=True, testroot="comlib", library="comlib", names=["CtptrHeap_t"] )
    except:
        Expect( False )



def TEST_comlib_CaptrHeap_t():
    """
    Core comlib CaptrHeap_t
    test_level=101
    """
    try:
        pyvgx.selftest( force=True, testroot="comlib", library="comlib", names=["CaptrHeap_t"] )
    except:
        Expect( False )




def Run( name ):
    RunTests( [__name__] )





