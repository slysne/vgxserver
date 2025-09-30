###############################################################################
# 
# VGX Server
# Distributed engine for plugin-based graph and vector search
# 
# Module:  pyvgxtest
# File:    JsonResponse.py
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
import json

graph = None




###############################################################################
# TEST_json_format
#
###############################################################################
def TEST_json_format():
    """
    test_level=4101
    t_nominal=1
    """
    # Send request
    bytes, headers = Support.send_request( "vgx/builtin/echo", json=True )
    # Check headers
    Support.assert_headers( headers, bytes, "application/json" )


    D = json.loads( bytes )
    status = D.get( 'status' )
    response = D.get( 'response' )
    exec_ms = D.get( 'exec_ms' )

    Expect( type( status ) is str,          "status is string, got %s" % type(status) )
    Expect( type( response ) is dict,       "response is dict, got %s" % type(response) )
    Expect( type( exec_ms ) is float,       "exec_ms is float, got %s" % type(exec_ms) )

    Expect( status == "OK",                 "status OK, got %s" % status )







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
