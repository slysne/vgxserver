###############################################################################
# 
# VGX Server
# Distributed engine for plugin-based graph and vector search
# 
# Module:  comlib
# File:    lib_comlib.py
# Author:  Stian Lysne <...>
# 
# Copyright © 2025 Rakuten, Inc.
# 
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# 
#     http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
# 
###############################################################################

from pytest.pytest import RunTests, Expect, TestFailed
from pyvgx import *
import pyvgx






###############################################################################
# TEST_comlib_COMLIB
#
###############################################################################
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
 



###############################################################################
# TEST_comlib_cxcstring
#
###############################################################################
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




###############################################################################
# TEST_comlib_CStringQueue_t
#
###############################################################################
def TEST_comlib_CStringQueue_t():
    """
    Core comlib CStringQueue_t
    test_level=101
    """
    try:
        pyvgx.selftest( force=True, testroot="comlib", library="comlib", names=["CStringQueue_t"] )
    except:
        Expect( False )




###############################################################################
# TEST_comlib_CByteQueue_t
#
###############################################################################
def TEST_comlib_CByteQueue_t():
    """
    Core comlib CByteQueue_t
    test_level=101
    """
    try:
        pyvgx.selftest( force=True, testroot="comlib", library="comlib", names=["CByteQueue_t"] )
    except:
        Expect( False )




###############################################################################
# TEST_comlib_CWordQueue_t
#
###############################################################################
def TEST_comlib_CWordQueue_t():
    """
    Core comlib CWordQueue_t
    test_level=101
    """
    try:
        pyvgx.selftest( force=True, testroot="comlib", library="comlib", names=["CWordQueue_t"] )
    except:
        Expect( False )




###############################################################################
# TEST_comlib_CDwordQueue_t
#
###############################################################################
def TEST_comlib_CDwordQueue_t():
    """
    Core comlib CDwordQueue_t
    test_level=101
    """
    try:
        pyvgx.selftest( force=True, testroot="comlib", library="comlib", names=["CDwordQueue_t"] )
    except:
        Expect( False )




###############################################################################
# TEST_comlib_CQwordQueue_t
#
###############################################################################
def TEST_comlib_CQwordQueue_t():
    """
    Core comlib CQwordQueue_t
    test_level=101
    """
    try:
        pyvgx.selftest( force=True, testroot="comlib", library="comlib", names=["CQwordQueue_t"] )
    except:
        Expect( False )




###############################################################################
# TEST_comlib_Cm128iQueue_t
#
###############################################################################
def TEST_comlib_Cm128iQueue_t():
    """
    Core comlib Cm128iQueue_t
    test_level=101
    """
    try:
        pyvgx.selftest( force=True, testroot="comlib", library="comlib", names=["Cm128iQueue_t"] )
    except:
        Expect( False )




###############################################################################
# TEST_comlib_Cm256iQueue_t
#
###############################################################################
def TEST_comlib_Cm256iQueue_t():
    """
    Core comlib Cm256iQueue_t
    test_level=101
    """
    try:
        pyvgx.selftest( force=True, testroot="comlib", library="comlib", names=["Cm256iQueue_t"] )
    except:
        Expect( False )




###############################################################################
# TEST_comlib_Cm512iQueue_t
#
###############################################################################
def TEST_comlib_Cm512iQueue_t():
    """
    Core comlib Cm512iQueue_t
    test_level=101
    """
    try:
        pyvgx.selftest( force=True, testroot="comlib", library="comlib", names=["Cm512iQueue_t"] )
    except:
        Expect( False )




###############################################################################
# TEST_comlib_CtptrQueue_t
#
###############################################################################
def TEST_comlib_CtptrQueue_t():
    """
    Core comlib CtptrQueue_t
    test_level=101
    """
    try:
        pyvgx.selftest( force=True, testroot="comlib", library="comlib", names=["CtptrQueue_t"] )
    except:
        Expect( False )




###############################################################################
# TEST_comlib_Cx2tptrQueue_t
#
###############################################################################
def TEST_comlib_Cx2tptrQueue_t():
    """
    Core comlib Cx2tptrQueue_t
    test_level=101
    """
    try:
        pyvgx.selftest( force=True, testroot="comlib", library="comlib", names=["Cx2tptrQueue_t"] )
    except:
        Expect( False )




###############################################################################
# TEST_comlib_CaptrQueue_t
#
###############################################################################
def TEST_comlib_CaptrQueue_t():
    """
    Core comlib CaptrQueue_t
    test_level=101
    """
    try:
        pyvgx.selftest( force=True, testroot="comlib", library="comlib", names=["CaptrQueue_t"] )
    except:
        Expect( False )




###############################################################################
# TEST_comlib_CByteHeap_t
#
###############################################################################
def TEST_comlib_CByteHeap_t():
    """
    Core comlib CByteHeap_t
    test_level=101
    """
    try:
        pyvgx.selftest( force=True, testroot="comlib", library="comlib", names=["CByteHeap_t"] )
    except:
        Expect( False )




###############################################################################
# TEST_comlib_CWordHeap_t
#
###############################################################################
def TEST_comlib_CWordHeap_t():
    """
    Core comlib CWordHeap_t
    test_level=101
    """
    try:
        pyvgx.selftest( force=True, testroot="comlib", library="comlib", names=["CWordHeap_t"] )
    except:
        Expect( False )




###############################################################################
# TEST_comlib_CDwordHeap_t
#
###############################################################################
def TEST_comlib_CDwordHeap_t():
    """
    Core comlib CDwordHeap_t
    test_level=101
    """
    try:
        pyvgx.selftest( force=True, testroot="comlib", library="comlib", names=["CDwordHeap_t"] )
    except:
        Expect( False )




###############################################################################
# TEST_comlib_CQwordHeap_t
#
###############################################################################
def TEST_comlib_CQwordHeap_t():
    """
    Core comlib CQwordHeap_t
    test_level=101
    """
    try:
        pyvgx.selftest( force=True, testroot="comlib", library="comlib", names=["CQwordHeap_t"] )
    except:
        Expect( False )




###############################################################################
# TEST_comlib_CtptrHeap_t
#
###############################################################################
def TEST_comlib_CtptrHeap_t():
    """
    Core comlib CtptrHeap_t
    test_level=101
    """
    try:
        pyvgx.selftest( force=True, testroot="comlib", library="comlib", names=["CtptrHeap_t"] )
    except:
        Expect( False )




###############################################################################
# TEST_comlib_CaptrHeap_t
#
###############################################################################
def TEST_comlib_CaptrHeap_t():
    """
    Core comlib CaptrHeap_t
    test_level=101
    """
    try:
        pyvgx.selftest( force=True, testroot="comlib", library="comlib", names=["CaptrHeap_t"] )
    except:
        Expect( False )





###############################################################################
# Run
#
###############################################################################
def Run( name ):
    """
    """
    RunTests( [__name__] )
