###############################################################################
# 
# VGX Server
# Distributed engine for plugin-based graph and vector search
# 
# Module:  pyvgx.test
# File:    TopDispatcher.py
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
from .. import _http_support as Support
from ..Metrics.Endpoints import TEST_endpoint__vgx_nodestat as endpoint_vgx_nodestat
from ..Metrics.Endpoints import TEST_endpoint__vgx_matrix as endpoint_vgx_matrix
from ..Metrics.Endpoints import TEST_endpoint__vgx_dispatch as endpoint_vgx_dispatch
from pyvgx import *
import pyvgx
from . import engines
import pprint
import os




###############################################################################
# __validate_endpoints
#
###############################################################################
def __validate_endpoints():
    """
    """
    endpoint_vgx_nodestat()
    endpoint_vgx_matrix()
    endpoint_vgx_dispatch()




###############################################################################
# TEST_TopDispatcher_basic
#
###############################################################################
def TEST_TopDispatcher_basic():
    """
    Top Dispather Test simple 2x1
    test_level=4101
    t_nominal=46
    """

    D_PORT = 9747
    E_HOST = "127.0.0.1"
    E_PORTS = [9610, 9620]

    disp_cf = {}
    disp_cf['partitions'] = [ [{'host':E_HOST, 'port':port}] for port in E_PORTS ]
    disp_cf['replicas'] =   [  {'channels':8, 'priority':1, 'primary':1}   ]

    # Start local dispatcher
    system.StartHTTP( D_PORT, dispatcher=disp_cf )
    local_host, local_port = Support.get_server_host_port()

    ENGINES = engines.StartServerEngines( E_HOST, E_PORTS )

    try:
        try:
            # Send requests to local dispatcher acting as top dispatcher for backend server engines
            engines.RunQueries( local_host, local_port )

            # Validate endpoints
            __validate_endpoints()
        finally:
            # Local shutdown
            system.StopHTTP()
    finally:
        engines.StopEngines( ENGINES )
    



###############################################################################
# TEST_TopDispatcher_matrix
#
###############################################################################
def TEST_TopDispatcher_matrix():
    """
    Top Dispather Test several matrix shapes
    test_level=4101
    t_nominal=199
    """

    D_PORT = 9747
    E_HOST = "127.0.0.1"
    E_PORTS = [ 9610, 9620, 9630, 9640, 9650, 9660, 9670, 9680, 9690, 9700, 9710, 9720 ]

    # Set up backends
    ENGINES = engines.StartServerEngines( E_HOST, E_PORTS )

    try:
        for width, height in [ (12,1), (6,2), (4,3), (3,4), (2,6), (1,12) ]:
            try:
                # Start local dispatcher in WxH mode
                disp_cf = engines.GetMatrixConfig( width=width, height=height, host=E_HOST, ports=E_PORTS )
                pprint.pprint( disp_cf )

                # Local start
                system.StartHTTP( D_PORT, dispatcher=disp_cf )

                # Validate endpoints
                __validate_endpoints()

                # Send requests to local dispatcher acting as top dispatcher for backend server engines
                local_host, local_port = Support.get_server_host_port()
                engines.RunQueries( local_host, local_port, nthreads=5, quiet=True )

            finally:
                # Local shutdown
                system.StopHTTP()

    finally:
        engines.StopEngines( ENGINES )




###############################################################################
# TEST_TopDispatcher_multilevel_scenario1
#
###############################################################################
def TEST_TopDispatcher_multilevel_scenario1():
    """
    Top Dispather Test multiple dispatcher levels setup 1
    test_level=4101
    t_nominal=72
    """

    HOST = "127.0.0.1"

    TD_PORT = 9747
    D_PORTS = [ 9810, 9820 ]
    S_PORTS = [ 9610, 9620, 9630, 9640, 9650, 9660, 9670, 9680 ]

    #           TD
    #      D1        D2
    #   S1   S2   S5   S6
    #   S3   S4   S7   S8
    #

    # Dispatcher configs
    TD_cf = engines.GetMatrixConfig( width=2, height=1, host=HOST, ports=D_PORTS )
    D1_cf = engines.GetMatrixConfig( width=2, height=2, host=HOST, ports=S_PORTS[:4] )
    D2_cf = engines.GetMatrixConfig( width=2, height=2, host=HOST, ports=S_PORTS[4:] )

    pprint.pprint( TD_cf )
    pprint.pprint( D1_cf )
    pprint.pprint( D2_cf )

    ENGINES = []

    try:
        try:
            # Set up backends
            D1 = engines.StartDispatcherEngines( HOST, D_PORTS[:1], D1_cf )
            ENGINES.extend( D1 )

            D2 = engines.StartDispatcherEngines( HOST, D_PORTS[1:], D2_cf )
            ENGINES.extend( D2 )
            
            SERVERS = engines.StartServerEngines( HOST, S_PORTS )
            ENGINES.extend( SERVERS )

            D1_engine, D1_host, D1_port = D1[0]
            D1_width, D1_height = engines.GetDispatcherMatrixDimensions( HOST, D1_port )

            D2_engine, D2_host, D2_port = D2[0]
            D2_width, D2_height = engines.GetDispatcherMatrixDimensions( HOST, D2_port )

            total_count = engines.N_TERM * (D1_width + D2_width)

            # Local start
            system.StartHTTP( TD_PORT, dispatcher=TD_cf )

            # Validate endpoints
            __validate_endpoints()

            # Send requests to local dispatcher acting as top dispatcher for backend server engines
            local_host, local_port = Support.get_server_host_port()
            engines.RunQueries( local_host, local_port, total_count=total_count, nthreads=5, quiet=True )

        finally:
            # Local shutdown
            system.StopHTTP()

    finally:
        engines.StopEngines( ENGINES )




###############################################################################
# TEST_TopDispatcher_multilevel_scenario2
#
###############################################################################
def TEST_TopDispatcher_multilevel_scenario2():
    """
    Top Dispather Test multiple dispatcher levels setup 2
    test_level=4101
    t_nominal=85
    """

    HOST = "127.0.0.1"

    PX_PORT = 9747
    TD_PORT = 9990
    D_PORTS = [ 9910, 9920, 9930, 9940 ]
    S_PORTS = [ 9610, 9620, 9630, 9640, 9650, 9660 ]

    #         Proxy
    #          TD
    #     D1        D3
    #     D2        D4
    #  S1 S2 S3  S4 S5 S6

    # Dispatcher configs
    PX_cf = engines.GetMatrixConfig( width=1, height=1, host=HOST, ports=[TD_PORT] )
    TD_cf = engines.GetMatrixConfig( width=2, height=2, host=HOST, ports=D_PORTS )
    D1_cf = engines.GetMatrixConfig( width=3, height=1, host=HOST, ports=S_PORTS[:3] )
    D2_cf = engines.GetMatrixConfig( width=3, height=1, host=HOST, ports=S_PORTS[:3] )
    D3_cf = engines.GetMatrixConfig( width=3, height=1, host=HOST, ports=S_PORTS[3:] )
    D4_cf = engines.GetMatrixConfig( width=3, height=1, host=HOST, ports=S_PORTS[3:] )

    pprint.pprint( PX_cf )
    pprint.pprint( TD_cf )
    pprint.pprint( D1_cf )
    pprint.pprint( D2_cf )
    pprint.pprint( D3_cf )
    pprint.pprint( D4_cf )

    ENGINES = []

    try:
        try:
            # Set up backends
            TD = engines.StartDispatcherEngines( HOST, [TD_PORT], TD_cf )
            ENGINES.extend( TD )
            
            D1 = engines.StartDispatcherEngines( HOST, [D_PORTS[0]], D1_cf )
            ENGINES.extend( D1 )
            
            D2 = engines.StartDispatcherEngines( HOST, [D_PORTS[1]], D2_cf )
            ENGINES.extend( D2 )
            
            D3 = engines.StartDispatcherEngines( HOST, [D_PORTS[2]], D3_cf )
            ENGINES.extend( D3 )

            D4 = engines.StartDispatcherEngines( HOST, [D_PORTS[3]], D4_cf )
            ENGINES.extend( D4 )

            SERVERS = engines.StartServerEngines( HOST, S_PORTS )
            ENGINES.extend( SERVERS )

            D1_engine, D1_host, D1_port = D1[0]
            D1_width, D1_height = engines.GetDispatcherMatrixDimensions( HOST, D1_port )

            D3_engine, D3_host, D3_port = D3[0]
            D3_width, D3_height = engines.GetDispatcherMatrixDimensions( HOST, D3_port )

            total_count = engines.N_TERM * (D1_width + D3_width)

            # Local start
            system.StartHTTP( PX_PORT, dispatcher=PX_cf )

            # Validate endpoints
            __validate_endpoints()

            # Send requests to local dispatcher acting as top dispatcher for backend server engines
            local_host, local_port = Support.get_server_host_port()
            engines.RunQueries( local_host, local_port, total_count=total_count, nthreads=5, quiet=True )

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
