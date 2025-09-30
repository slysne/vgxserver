###############################################################################
# 
# VGX Server
# Distributed engine for plugin-based graph and vector search
# 
# Module:  pyvgx.test
# File:    SimpleProxy.py
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




###############################################################################
# TEST_ProxySingleEngine
#
###############################################################################
def TEST_ProxySingleEngine():
    """
    Single Engine Proxy Test
    test_level=4101
    t_nominal=48
    """

    D_PORT = 9747
    E_HOST = "127.0.0.1"
    E_PORT = 9647

    proxy_cf = {
        'partitions':[
            [ {'host':E_HOST, 'port':E_PORT} ],
        ],
        'replicas':[
            {'channels':8, 'priority':1, 'primary':1}
        ]
    }

    # Start local dispatcher
    system.StartHTTP( D_PORT, dispatcher=proxy_cf )
    local_host, local_port = Support.get_server_host_port()

    # Start backend server engine in a new process
    ENGINES = engines.StartServerEngines( E_HOST, [E_PORT] )

    try:

        try:
            # Send requests to local dispatcher acting as proxy for backend server engine
            engines.RunQueries( local_host, local_port )

        finally:
            # Local shutdown
            system.StopHTTP()

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
