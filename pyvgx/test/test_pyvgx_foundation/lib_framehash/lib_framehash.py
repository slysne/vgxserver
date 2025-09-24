from pytest.pytest import RunTests, Expect, TestFailed
from pyvgx import *
import pyvgx




def TEST_framehash_memory():
    """
    Core framehash memory
    test_level=301
    """
    try:
        pyvgx.selftest( force=True, testroot="framehash", library="framehash", names=["memory.c"] )
    except:
        Expect( False )



def TEST_framehash_frameallocator():
    """
    Core framehash frameallocator
    test_level=301
    """
    try:
        pyvgx.selftest( force=True, testroot="framehash", library="framehash", names=["frameallocator.c"] )
    except:
        Expect( False )



def TEST_framehash_basementallocator():
    """
    Core framehash basementallocator
    test_level=301
    """
    try:
        pyvgx.selftest( force=True, testroot="framehash", library="framehash", names=["basementallocator.c"] )
    except:
        Expect( False )



def TEST_framehash_processor():
    """
    Core framehash processor
    test_level=301
    """
    try:
        pyvgx.selftest( force=True, testroot="framehash", library="framehash", names=["processor.c"] )
    except:
        Expect( False )



def TEST_framehash_framemath():
    """
    Core framehash framemath
    test_level=301
    """
    try:
        pyvgx.selftest( force=True, testroot="framehash", library="framehash", names=["framemath.c"] )
    except:
        Expect( False )



def TEST_framehash_delete():
    """
    Core framehash delete
    test_level=301
    """
    try:
        pyvgx.selftest( force=True, testroot="framehash", library="framehash", names=["delete.c"] )
    except:
        Expect( False )



def TEST_framehash_serialization():
    """
    Core framehash serialization
    test_level=301
    """
    try:
        pyvgx.selftest( force=True, testroot="framehash", library="framehash", names=["serialization.c"] )
    except:
        Expect( False )



def TEST_framehash_hashing():
    """
    Core framehash hashing
    t_nominal=7
    test_level=301
    """
    try:
        pyvgx.selftest( force=True, testroot="framehash", library="framehash", names=["hashing.c"] )
    except:
        Expect( False )



def TEST_framehash_leaf():
    """
    Core framehash leaf
    test_level=301
    """
    try:
        pyvgx.selftest( force=True, testroot="framehash", library="framehash", names=["leaf.c"] )
    except:
        Expect( False )



def TEST_framehash_basement():
    """
    Core framehash basement
    test_level=301
    """
    try:
        pyvgx.selftest( force=True, testroot="framehash", library="framehash", names=["basement.c"] )
    except:
        Expect( False )



def TEST_framehash_cache():
    """
    Core framehash cache
    test_level=301
    """
    try:
        pyvgx.selftest( force=True, testroot="framehash", library="framehash", names=["cache.c"] )
    except:
        Expect( False )



def TEST_framehash_radix():
    """
    Core framehash radix
    test_level=301
    """
    try:
        pyvgx.selftest( force=True, testroot="framehash", library="framehash", names=["radix.c"] )
    except:
        Expect( False )



def TEST_framehash_fmacro():
    """
    Core framehash fmacro
    test_level=301
    """
    try:
        pyvgx.selftest( force=True, testroot="framehash", library="framehash", names=["fmacro.c"] )
    except:
        Expect( False )



def TEST_framehash_framehash():
    """
    Core framehash framehash
    test_level=302
    """
    try:
        pyvgx.selftest( force=True, testroot="framehash", library="framehash", names=["framehash.c"] )
    except:
        Expect( False )



def TEST_framehash_api_class():
    """
    Core framehash api_class
    t_nominal=1094
    test_level=303
    """
    try:
        pyvgx.selftest( force=True, testroot="framehash", library="framehash", names=["api_class.c"] )
    except:
        Expect( False )



def TEST_framehash_api_generic():
    """
    Core framehash api_generic
    test_level=305
    """
    try:
        pyvgx.selftest( force=True, testroot="framehash", library="framehash", names=["api_generic.c"] )
    except:
        Expect( False )



def TEST_framehash_api_object():
    """
    Core framehash api_object
    test_level=305
    """
    try:
        pyvgx.selftest( force=True, testroot="framehash", library="framehash", names=["api_object.c"] )
    except:
        Expect( False )



def TEST_framehash_api_int56():
    """
    Core framehash api_int56
    test_level=305
    """
    try:
        pyvgx.selftest( force=True, testroot="framehash", library="framehash", names=["api_int56.c"] )
    except:
        Expect( False )



def TEST_framehash_api_real56():
    """
    Core framehash api_real56
    test_level=305
    """
    try:
        pyvgx.selftest( force=True, testroot="framehash", library="framehash", names=["api_real56.c"] )
    except:
        Expect( False )



def TEST_framehash_api_pointer():
    """
    Core framehash api_pointer
    test_level=305
    """
    try:
        pyvgx.selftest( force=True, testroot="framehash", library="framehash", names=["api_pointer.c"] )
    except:
        Expect( False )



def TEST_framehash_api_info():
    """
    Core framehash api_info
    test_level=305
    """
    try:
        pyvgx.selftest( force=True, testroot="framehash", library="framehash", names=["api_info.c"] )
    except:
        Expect( False )



def TEST_framehash_api_iterator():
    """
    Core framehash api_iterator
    test_level=305
    """
    try:
        pyvgx.selftest( force=True, testroot="framehash", library="framehash", names=["api_iterator.c"] )
    except:
        Expect( False )



def TEST_framehash_api_manage():
    """
    Core framehash api_manage
    test_level=305
    """
    try:
        pyvgx.selftest( force=True, testroot="framehash", library="framehash", names=["api_manage.c"] )
    except:
        Expect( False )



def TEST_framehash_api_simple():
    """
    Core framehash api_simple
    t_nominal=1360
    test_level=305
    """
    try:
        pyvgx.selftest( force=True, testroot="framehash", library="framehash", names=["api_simple.c"] )
    except:
        Expect( False )




def Run( name ):
    RunTests( [__name__] )
