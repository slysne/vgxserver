###############################################################################
# 
# VGX Server
# Distributed engine for plugin-based graph and vector search
# 
# Module:  pyvgx.test
# File:    Core.py
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
import time
from pyvgx import *
import pyvgx

graph = None



###############################################################################
# TEST_vxarcvector_comparator
#
###############################################################################
def TEST_vxarcvector_comparator():
    """
    Core vxarcvector_comparator
    test_level=501
    """
    try:
        pyvgx.selftest( force=True, testroot="vgxtest", library="vgx", names=["vxarcvector_comparator.c"] )
    except:
        Expect( False )




###############################################################################
# TEST_vxarcvector_filter
#
###############################################################################
def TEST_vxarcvector_filter():
    """
    Core vxarcvector_filter
    test_level=501
    """
    try:
        pyvgx.selftest( force=True, testroot="vgxtest", library="vgx", names=["vxarcvector_filter.c"] )
    except:
        Expect( False )




###############################################################################
# TEST_vxarcvector_fhash
#
###############################################################################
def TEST_vxarcvector_fhash():
    """
    Core vxarcvector_fhash
    test_level=501
    """
    try:
        pyvgx.selftest( force=True, testroot="vgxtest", library="vgx", names=["vxarcvector_fhash.c"] )
    except:
        Expect( False )




###############################################################################
# TEST_vxarcvector_cellproc
#
###############################################################################
def TEST_vxarcvector_cellproc():
    """
    Core vxarcvector_cellproc
    test_level=501
    """
    try:
        pyvgx.selftest( force=True, testroot="vgxtest", library="vgx", names=["vxarcvector_cellproc.c"] )
    except:
        Expect( False )




###############################################################################
# TEST_vxarcvector_traverse
#
###############################################################################
def TEST_vxarcvector_traverse():
    """
    Core vxarcvector_traverse
    test_level=501
    """
    try:
        pyvgx.selftest( force=True, testroot="vgxtest", library="vgx", names=["vxarcvector_traverse.c"] )
    except:
        Expect( False )




###############################################################################
# TEST_vxarcvector_exists
#
###############################################################################
def TEST_vxarcvector_exists():
    """
    Core vxarcvector_exists
    test_level=501
    """
    try:
        pyvgx.selftest( force=True, testroot="vgxtest", library="vgx", names=["vxarcvector_exists.c"] )
    except:
        Expect( False )




###############################################################################
# TEST_vxarcvector_delete
#
###############################################################################
def TEST_vxarcvector_delete():
    """
    Core vxarcvector_delete
    test_level=501
    """
    try:
        pyvgx.selftest( force=True, testroot="vgxtest", library="vgx", names=["vxarcvector_delete.c"] )
    except:
        Expect( False )




###############################################################################
# TEST_vxarcvector_expire
#
###############################################################################
def TEST_vxarcvector_expire():
    """
    Core vxarcvector_expire
    test_level=501
    """
    try:
        pyvgx.selftest( force=True, testroot="vgxtest", library="vgx", names=["vxarcvector_expire.c"] )
    except:
        Expect( False )




###############################################################################
# TEST_vxarcvector_dispatch
#
###############################################################################
def TEST_vxarcvector_dispatch():
    """
    Core vxarcvector_dispatch
    test_level=501
    """
    try:
        pyvgx.selftest( force=True, testroot="vgxtest", library="vgx", names=["vxarcvector_dispatch.c"] )
    except:
        Expect( False )




###############################################################################
# TEST_vxarcvector_api
#
###############################################################################
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




###############################################################################
# TEST_vxgraph_arc
#
###############################################################################
def TEST_vxgraph_arc():
    """
    Core vxgraph_arc
    test_level=501
    """
    try:
        pyvgx.selftest( force=True, testroot="vgxtest", library="vgx", names=["vxgraph_arc.c"] )
    except:
        Expect( False )





###############################################################################
# Run
#
###############################################################################
def Run( name ):
    """
    """
    global graph
    graph = pyvgx.Graph( name )
    RunTests( [__name__] )
    graph.Truncate()
    graph.Close()
    del graph
