###############################################################################
# 
# VGX Server
# Distributed engine for plugin-based graph and vector search
# 
# Module:  pyvgx
# File:    JavaScript.py
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
from .. import _http_support as Support
from pyvgx import *
import pyvgx
import urllib.request
import re

graph = None




def TEST_Artifacts_javascript_header():
    """
    header.js
    test_level=4101
    """
    # Send request
    bytes, headers = Support.send_request( "header.js" )
    # Check headers
    Support.assert_headers( headers, bytes, "application/javascript" )
    # Sanity check content
    artifact = bytes.decode()
    for text in [ "class CommonHeader" ]:
        Expect( text in artifact,           "Should contain '%s'" % text )



def TEST_Artifacts_javascript_uptime():
    """
    uptime.js
    test_level=4101
    """
    # Send request
    bytes, headers = Support.send_request( "uptime.js" )
    # Check headers
    Support.assert_headers( headers, bytes, "application/javascript" )
    # Sanity check content
    artifact = bytes.decode()
    for text in [ "class UptimeRefresher" ]:
        Expect( text in artifact,           "Should contain '%s'" % text )



def TEST_Artifacts_javascript_boxstate():
    """
    boxstate.js
    test_level=4101
    """
    # Send request
    bytes, headers = Support.send_request( "boxstate.js" )
    # Check headers
    Support.assert_headers( headers, bytes, "application/javascript" )
    # Sanity check content
    artifact = bytes.decode()
    for text in [ "class BoxState" ]:
        Expect( text in artifact,           "Should contain '%s'" % text )



def TEST_Artifacts_javascript_jquery():
    """
    jquery.js
    test_level=4101
    """
    # Send request
    bytes, headers = Support.send_request( "jquery.js" )
    # Check headers
    Support.assert_headers( headers, bytes, "application/javascript" )
    # Sanity check content
    artifact = bytes.decode()
    for text in [ "jQuery", "function", "getElementById" ]:
        Expect( text in artifact,           "Should contain '%s'" % text )







def Run( name ):
    global graph
    graph = pyvgx.Graph( name )
    RunTests( [__name__] )
    graph.Close()
    del graph
