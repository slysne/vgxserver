###############################################################################
# 
# VGX Server
# Distributed engine for plugin-based graph and vector search
# 
# Module:  pyvgx.test
# File:    Enumerator.py
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

graph = None



###############################################################################
# TEST_vxenum
#
###############################################################################
def TEST_vxenum():
    """
    Core vxenum
    test_level=501
    """
    try:
        pyvgx.selftest( force=True, testroot="vgxtest", library="vgx", names=["vxenum.c"] )
    except:
        Expect( False )




###############################################################################
# TEST_vxenum_vtx
#
###############################################################################
def TEST_vxenum_vtx():
    """
    Core vxenum_vtx
    test_level=501
    """
    try:
        pyvgx.selftest( force=True, testroot="vgxtest", library="vgx", names=["vxenum_vtx.c"] )
    except:
        Expect( False )




###############################################################################
# TEST_vxenum_rel
#
###############################################################################
def TEST_vxenum_rel():
    """
    Core vxenum_rel
    test_level=501
    """
    try:
        pyvgx.selftest( force=True, testroot="vgxtest", library="vgx", names=["vxenum_rel.c"] )
    except:
        Expect( False )




###############################################################################
# TEST_vxenum_dim
#
###############################################################################
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




###############################################################################
# TEST_vxenum_propkey
#
###############################################################################
def TEST_vxenum_propkey():
    """
    Core vxenum_propkey
    test_level=501
    """
    try:
        pyvgx.selftest( force=True, testroot="vgxtest", library="vgx", names=["vxenum_propkey.c"] )
    except:
        Expect( False )




###############################################################################
# TEST_vxenum_propval
#
###############################################################################
def TEST_vxenum_propval():
    """
    Core vxenum_propval
    test_level=501
    """
    try:
        pyvgx.selftest( force=True, testroot="vgxtest", library="vgx", names=["vxenum_propval.c"] )
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
    graph.Close()
    del graph
