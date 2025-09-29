###############################################################################
# 
# VGX Server
# Distributed engine for plugin-based graph and vector search
# 
# Module:  pyvgx
# File:    FeedPartials.py
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
from . import engines
import pprint




###############################################################################
# TEST_FeedPartials_basic
#
###############################################################################
def TEST_FeedPartials_basic():
    """
    Feed data to different partials
    test_level=4101
    t_nominal=70
    """

    HOST = "127.0.0.1"
    TD_PORTS = [9710, 9720]
    E_PORTS = [ 9610, 9620, 9630, 9640, 9650 ]

    # Set up backends
    ENGINES = []
    try:
        TD_cf = engines.GetMatrixConfig( width=len(E_PORTS), height=1, host=HOST, ports=E_PORTS )
        pprint.pprint( TD_cf )

        DISPATCHERS = engines.StartDispatcherEngines( HOST, TD_PORTS, TD_cf )
        ENGINES.extend( DISPATCHERS )

        SERVERS = engines.StartServerEngines( HOST, E_PORTS, prefill=False )
        ENGINES.extend( SERVERS )

        # Feed data
        engines.FeedData( HOST, TD_PORTS, nthreads=16 )
    finally:
        engines.StopEngines( ENGINES )




###############################################################################
# Run
#
###############################################################################
def Run( name ):
    """
    """
    RunTests( [__name__] )
