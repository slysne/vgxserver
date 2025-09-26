###############################################################################
# 
# VGX Server
# Distributed engine for plugin-based graph and vector search
# 
# Module:  pyvgx
# File:    EventProcessor.py
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
