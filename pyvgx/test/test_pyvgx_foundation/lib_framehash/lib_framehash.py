###############################################################################
# 
# VGX Server
# Distributed engine for plugin-based graph and vector search
# 
# Module:  framehash
# File:    lib_framehash.py
# Author:  Stian Lysne slysne.dev@gmail.com
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

from pyvgxtest.pyvgxtest import RunTests, Expect, TestFailed
from pyvgx import *
import pyvgx





###############################################################################
# TEST_framehash_memory
#
###############################################################################
def TEST_framehash_memory():
    """
    Core framehash memory
    test_level=301
    """
    try:
        pyvgx.selftest( force=True, testroot="framehash", library="framehash", names=["memory.c"] )
    except:
        Expect( False )




###############################################################################
# TEST_framehash_frameallocator
#
###############################################################################
def TEST_framehash_frameallocator():
    """
    Core framehash frameallocator
    test_level=301
    """
    try:
        pyvgx.selftest( force=True, testroot="framehash", library="framehash", names=["frameallocator.c"] )
    except:
        Expect( False )




###############################################################################
# TEST_framehash_basementallocator
#
###############################################################################
def TEST_framehash_basementallocator():
    """
    Core framehash basementallocator
    test_level=301
    """
    try:
        pyvgx.selftest( force=True, testroot="framehash", library="framehash", names=["basementallocator.c"] )
    except:
        Expect( False )




###############################################################################
# TEST_framehash_processor
#
###############################################################################
def TEST_framehash_processor():
    """
    Core framehash processor
    test_level=301
    """
    try:
        pyvgx.selftest( force=True, testroot="framehash", library="framehash", names=["processor.c"] )
    except:
        Expect( False )




###############################################################################
# TEST_framehash_framemath
#
###############################################################################
def TEST_framehash_framemath():
    """
    Core framehash framemath
    test_level=301
    """
    try:
        pyvgx.selftest( force=True, testroot="framehash", library="framehash", names=["framemath.c"] )
    except:
        Expect( False )




###############################################################################
# TEST_framehash_delete
#
###############################################################################
def TEST_framehash_delete():
    """
    Core framehash delete
    test_level=301
    """
    try:
        pyvgx.selftest( force=True, testroot="framehash", library="framehash", names=["delete.c"] )
    except:
        Expect( False )




###############################################################################
# TEST_framehash_serialization
#
###############################################################################
def TEST_framehash_serialization():
    """
    Core framehash serialization
    test_level=301
    """
    try:
        pyvgx.selftest( force=True, testroot="framehash", library="framehash", names=["serialization.c"] )
    except:
        Expect( False )




###############################################################################
# TEST_framehash_hashing
#
###############################################################################
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




###############################################################################
# TEST_framehash_leaf
#
###############################################################################
def TEST_framehash_leaf():
    """
    Core framehash leaf
    test_level=301
    """
    try:
        pyvgx.selftest( force=True, testroot="framehash", library="framehash", names=["leaf.c"] )
    except:
        Expect( False )




###############################################################################
# TEST_framehash_basement
#
###############################################################################
def TEST_framehash_basement():
    """
    Core framehash basement
    test_level=301
    """
    try:
        pyvgx.selftest( force=True, testroot="framehash", library="framehash", names=["basement.c"] )
    except:
        Expect( False )




###############################################################################
# TEST_framehash_cache
#
###############################################################################
def TEST_framehash_cache():
    """
    Core framehash cache
    test_level=301
    """
    try:
        pyvgx.selftest( force=True, testroot="framehash", library="framehash", names=["cache.c"] )
    except:
        Expect( False )




###############################################################################
# TEST_framehash_radix
#
###############################################################################
def TEST_framehash_radix():
    """
    Core framehash radix
    test_level=301
    """
    try:
        pyvgx.selftest( force=True, testroot="framehash", library="framehash", names=["radix.c"] )
    except:
        Expect( False )




###############################################################################
# TEST_framehash_fmacro
#
###############################################################################
def TEST_framehash_fmacro():
    """
    Core framehash fmacro
    test_level=301
    """
    try:
        pyvgx.selftest( force=True, testroot="framehash", library="framehash", names=["fmacro.c"] )
    except:
        Expect( False )




###############################################################################
# TEST_framehash_framehash
#
###############################################################################
def TEST_framehash_framehash():
    """
    Core framehash framehash
    test_level=302
    """
    try:
        pyvgx.selftest( force=True, testroot="framehash", library="framehash", names=["framehash.c"] )
    except:
        Expect( False )




###############################################################################
# TEST_framehash_api_class
#
###############################################################################
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




###############################################################################
# TEST_framehash_api_generic
#
###############################################################################
def TEST_framehash_api_generic():
    """
    Core framehash api_generic
    test_level=305
    """
    try:
        pyvgx.selftest( force=True, testroot="framehash", library="framehash", names=["api_generic.c"] )
    except:
        Expect( False )




###############################################################################
# TEST_framehash_api_object
#
###############################################################################
def TEST_framehash_api_object():
    """
    Core framehash api_object
    test_level=305
    """
    try:
        pyvgx.selftest( force=True, testroot="framehash", library="framehash", names=["api_object.c"] )
    except:
        Expect( False )




###############################################################################
# TEST_framehash_api_int56
#
###############################################################################
def TEST_framehash_api_int56():
    """
    Core framehash api_int56
    test_level=305
    """
    try:
        pyvgx.selftest( force=True, testroot="framehash", library="framehash", names=["api_int56.c"] )
    except:
        Expect( False )




###############################################################################
# TEST_framehash_api_real56
#
###############################################################################
def TEST_framehash_api_real56():
    """
    Core framehash api_real56
    test_level=305
    """
    try:
        pyvgx.selftest( force=True, testroot="framehash", library="framehash", names=["api_real56.c"] )
    except:
        Expect( False )




###############################################################################
# TEST_framehash_api_pointer
#
###############################################################################
def TEST_framehash_api_pointer():
    """
    Core framehash api_pointer
    test_level=305
    """
    try:
        pyvgx.selftest( force=True, testroot="framehash", library="framehash", names=["api_pointer.c"] )
    except:
        Expect( False )




###############################################################################
# TEST_framehash_api_info
#
###############################################################################
def TEST_framehash_api_info():
    """
    Core framehash api_info
    test_level=305
    """
    try:
        pyvgx.selftest( force=True, testroot="framehash", library="framehash", names=["api_info.c"] )
    except:
        Expect( False )




###############################################################################
# TEST_framehash_api_iterator
#
###############################################################################
def TEST_framehash_api_iterator():
    """
    Core framehash api_iterator
    test_level=305
    """
    try:
        pyvgx.selftest( force=True, testroot="framehash", library="framehash", names=["api_iterator.c"] )
    except:
        Expect( False )




###############################################################################
# TEST_framehash_api_manage
#
###############################################################################
def TEST_framehash_api_manage():
    """
    Core framehash api_manage
    test_level=305
    """
    try:
        pyvgx.selftest( force=True, testroot="framehash", library="framehash", names=["api_manage.c"] )
    except:
        Expect( False )




###############################################################################
# TEST_framehash_api_simple
#
###############################################################################
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





###############################################################################
# Run
#
###############################################################################
def Run( name ):
    """
    """
    RunTests( [__name__] )
