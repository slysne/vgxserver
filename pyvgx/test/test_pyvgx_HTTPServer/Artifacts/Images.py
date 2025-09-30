###############################################################################
# 
# VGX Server
# Distributed engine for plugin-based graph and vector search
# 
# Module:  pyvgx.test
# File:    Images.py
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
# TEST_Artifacts_images_logo
#
###############################################################################
def TEST_Artifacts_images_logo():
    """
    logo_b-x.png
    test_level=4101
    t_nominal=1
    """
    # Send request
    bytes, headers = Support.send_request( "logo_b-x.png" )
    # Check headers
    Support.assert_headers( headers, bytes, "image/png" )




###############################################################################
# TEST_Artifacts_images_loader
#
###############################################################################
def TEST_Artifacts_images_loader():
    """
    loader.gif
    test_level=4101
    t_nominal=1
    """
    # Send request
    bytes, headers = Support.send_request( "loader.gif" )
    # Check headers
    Support.assert_headers( headers, bytes, "image/gif" )




###############################################################################
# TEST_Artifacts_images_favicon
#
###############################################################################
def TEST_Artifacts_images_favicon():
    """
    favicon.ico
    test_level=4101
    t_nominal=1
    """
    # Send request
    bytes, headers = Support.send_request( "favicon.ico" )
    # Check headers
    Support.assert_headers( headers, bytes, "image/ico" )






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
