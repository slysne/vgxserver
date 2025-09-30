###############################################################################
# 
# VGX Server
# Distributed engine for plugin-based graph and vector search
# 
# Module:  pyvgxtest
# File:    HTML.py
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

from pyvgxtest.pyvgxtest import RunTests, Expect, TestFailed
from .. import _http_support as Support
from pyvgx import *
import pyvgx
import urllib.request
import re

graph = None



###############################################################################
# TEST_Artifacts_HTML_header
#
###############################################################################
def TEST_Artifacts_HTML_header():
    """
    header.html
    test_level=4101
    t_nominal=1
    """
    # Send request
    bytes, headers = Support.send_request( "header.html" )
    # Check headers
    Support.assert_headers( headers, bytes, "text/html" )
    # Sanity check content
    artifact = bytes.decode()
    for tag in ["div", "table", "tr", "td" ]:
        Expect( "<%s" % tag in artifact,            "<%s> tag(s) should exist" % tag )
    for text in ["commonHeaderTable", "Processor", "logo_b-x.png" ]:
        Expect( text in artifact,                   "%s should exist" % text )




###############################################################################
# TEST_Artifacts_HTML_footer
#
###############################################################################
def TEST_Artifacts_HTML_footer():
    """
    footer.html
    test_level=4101
    t_nominal=1
    """
    # Send request
    bytes, headers = Support.send_request( "footer.html" )
    # Check headers
    Support.assert_headers( headers, bytes, "text/html" )
    # Sanity check
    artifact = bytes.decode()
    for tag in ["div", "span" ]:
        Expect( "<%s" % tag in artifact,            "<%s> tag(s) should exist" % tag )
    for text in ["vgx"]:
        Expect( text in artifact,                   "%s should exist" % text )




###############################################################################
# TEST_Artifacts_HTML_admin
#
###############################################################################
def TEST_Artifacts_HTML_admin():
    """
    admin
    test_level=4101
    t_nominal=1
    """
    # Send request
    bytes, headers = Support.send_request( "admin" )
    # Check headers
    Support.assert_headers( headers, bytes, "text/html" )
    # Sanity check
    artifact = bytes.decode()
    for tag in [ "html", "head", "body", "style", "script", "div", "tr", "td", "input" ]:
        Expect( "<%s" % tag in artifact,            "<%s> tag(s) should exist" % tag )
    for text in [ "VGX Admin", "Service", "Provider", "Subscribers" ]:
        Expect( text in artifact,                   "%s should exist" % text )




###############################################################################
# TEST_Artifacts_HTML_system
#
###############################################################################
def TEST_Artifacts_HTML_system():
    """
    system
    test_level=4101
    t_nominal=1
    """
    # Send request
    bytes, headers = Support.send_request( "system" )
    # Check headers
    Support.assert_headers( headers, bytes, "text/html" )
    # Sanity check
    artifact = bytes.decode()
    for tag in [ "html", "head", "body", "style", "script", "div", "tr", "td", "input" ]:
        Expect( "<%s" % tag in artifact,            "<%s> tag(s) should exist" % tag )




###############################################################################
# TEST_Artifacts_HTML_console
#
###############################################################################
def TEST_Artifacts_HTML_console():
    """
    console
    test_level=4101
    t_nominal=1
    """
    # Send request
    bytes, headers = Support.send_request( "console" )
    # Check headers
    Support.assert_headers( headers, bytes, "text/html" )
    # Sanity check
    artifact = bytes.decode()
    for tag in [ "html", "head", "body", "style", "script", "div", "tr", "td", "textarea" ]:
        Expect( "<%s" % tag in artifact,            "<%s> tag(s) should exist" % tag )
    for text in [ "VGX Console", "Execute" ]:
        Expect( text in artifact,                   "%s should exist" % text )




###############################################################################
# TEST_Artifacts_HTML_index
#
###############################################################################
def TEST_Artifacts_HTML_index():
    """
    index
    test_level=4101
    t_nominal=1
    """
    # Send request
    bytes, headers = Support.send_request( "" )
    # Check headers
    Support.assert_headers( headers, bytes, "text/html" )
    # Sanity check
    artifact = bytes.decode()
    for tag in [ "html", "head", "body", "style", "script", "div", "tr", "td", "button" ]:
        Expect( "<%s" % tag in artifact,            "<%s> tag(s) should exist" % tag )
    for text in [ "VGX", "Admin", "Search", "JSON" ]:
        Expect( text in artifact,                   "%s should exist" % text )




###############################################################################
# TEST_Artifacts_HTML_plugin
#
###############################################################################
def TEST_Artifacts_HTML_plugin():
    """
    plugin
    test_level=4101
    t_nominal=1
    """
    # Send request
    bytes, headers = Support.send_request( "plugin" )
    # Check headers
    Support.assert_headers( headers, bytes, "text/html" )
    # Sanity check
    artifact = bytes.decode()
    for tag in [ "html", "head", "body", "style", "script", "div", "tr", "td", "button" ]:
        Expect( "<%s" % tag in artifact,            "<%s> tag(s) should exist" % tag )
    for text in [ "VGX Plugin", "User", "Builtin", "Execute" ]:
        Expect( text in artifact,                   "%s should exist" % text )




###############################################################################
# TEST_Artifacts_HTML_search
#
###############################################################################
def TEST_Artifacts_HTML_search():
    """
    search
    test_level=4101
    t_nominal=1
    """
    # Send request
    bytes, headers = Support.send_request( "search" )
    # Check headers
    Support.assert_headers( headers, bytes, "text/html" )
    # Sanity check
    artifact = bytes.decode()
    for tag in [ "html", "head", "body", "style", "script", "div", "tr", "td", "input" ]:
        Expect( "<%s" % tag in artifact,            "<%s> tag(s) should exist" % tag )
    for text in [ "VGX Search", "Execute", "Vertex", "Arc", "Query" ]:
        Expect( text in artifact,                   "%s should exist" % text )




###############################################################################
# TEST_Artifacts_HTML_status
#
###############################################################################
def TEST_Artifacts_HTML_status():
    """
    status
    test_level=4101
    t_nominal=1
    """
    # Send request
    bytes, headers = Support.send_request( "status" )
    # Check headers
    Support.assert_headers( headers, bytes, "text/html" )
    # Sanity check
    artifact = bytes.decode()
    for tag in [ "html", "head", "body", "style", "script", "div", "tr", "td" ]:
        Expect( "<%s" % tag in artifact,            "<%s> tag(s) should exist" % tag )
    for text in [ "VGX Status", "Service", "Transaction", "Graph", "Memory" ]:
        Expect( text in artifact,                   "%s should exist" % text )






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
