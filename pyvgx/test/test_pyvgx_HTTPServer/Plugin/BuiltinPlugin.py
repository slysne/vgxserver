###############################################################################
# 
# VGX Server
# Distributed engine for plugin-based graph and vector search
# 
# Module:  pyvgx
# File:    BuiltinPlugin.py
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
import json

graph = None




###############################################################################
# TEST_get_builtins
#
###############################################################################
def TEST_get_builtins():
    """
    /vgx/builtins
    test_level=4101
    t_nominal=1
    """
    # Send request
    bytes, headers = Support.send_request( "vgx/builtins", json=True )
    # Check headers
    Support.assert_headers( headers, bytes, "application/json" )

    D = json.loads( bytes )
    builtin_list = D.get( 'response' )

    # response is a list of plugins
    Expect( type( builtin_list ) is list,               "builtin_list is list, got %s" % type( builtin_list ) )

    for plugin in builtin_list:
        # Plugin is dict
        Expect( type(plugin) is dict,                   "plugin is dict, got %s" % type( plugin ) )
        path = plugin.get( 'path' )
        description = plugin.get( 'description' )
        parameters = plugin.get( 'parameters' )
        bound_graph = plugin.get( 'bound_graph' )
        # path
        Expect( path.startswith( '/vgx/builtin/' ) )
        # description
        Expect( type(description) is list,              "description is list of lines, got %s" % type( description ) )
        # parameters
        Expect( type(parameters) is dict,               "parameters is dict, got %s" % type( parameters ) )
        # bound_graph
        Expect( bound_graph == "None",                  "bound_graph None for all builtins, got %s" % str(bound_graph) )




###############################################################################
# TEST_builtin__vgx_builtin_arcs
#
###############################################################################
def TEST_builtin__vgx_builtin_arcs():
    """
    /vgx/builtin/arcs
    test_level=4101
    t_nominal=1
    """
    # Build a small graph
    graph.Truncate()
    graph.CreateVertex( "A" )
    graph.CreateVertex( "B" )
    graph.CreateVertex( "C" )
    graph.CreateVertex( "D" )
    graph.Connect( "A", "to", ["B", "C", "D"] )

    # Send request
    bytes, headers = Support.send_request( "vgx/builtin/arcs?graph=%s" % (graph.name), json=True )
    Support.assert_headers( headers, bytes, "application/json" )
    http_arcs = json.loads( bytes )['response']

    # Perform internal query
    N = 10
    sdk_arcs = graph.Arcs( hits=N )
    for n in range( len(sdk_arcs) ):
        a = http_arcs[n]
        b = sdk_arcs[n]
        Expect( a == b,                 "should be equal, got %s != %s" % (a,b) )




###############################################################################
# TEST_builtin__vgx_builtin_status
#
###############################################################################
def TEST_builtin__vgx_builtin_status():
    """
    /vgx/builtin/status
    test_level=4101
    t_nominal=1
    """

    REF = json.loads(
    """
    {
        "version": "vgx v?.?.? [date: Feb 23 2023 19:39:11, built by: local, build #: N/A]",
        "graphs": {
            "test_pyvgx_HTTPServer/_system": {
                "order": 4,
                "size": 2,
                "properties": 12,
                "vectors": 0,
                "pooled_bytes": 381821890,
                "pooled_to_vgx_process_ratio": 11.41
            },
            "test_pyvgx_HTTPServer/test_pyvgx_HTTPServer.Plugin": {
                "order": 0,
                "size": 0,
                "properties": 0,
                "vectors": 0,
                "pooled_bytes": 150750765,
                "pooled_to_vgx_process_ratio": 4.505
            }
        },
        "time": {
            "vgx_uptime": 13211,
            "vgx_current": 1677288857,
            "vgx_age": 37701
        },
        "host": {
            "name": "SomeHost",
            "ip": "172.29.160.1"
        },
        "httpserver": {
            "server": "http://172.29.160.1:9900",
            "bytes_in": 5335,
            "bytes_out": 25481,
            "total_requests": 7,
            "connected_clients": 2,
            "request_rate": {
                "current": 0.0,
                "all": 0.001
            },
            "latency_ms": {
                "current": {
                    "mean": 1.49,
                    "80th": 1.17,
                    "95th": 1.21,
                    "99th": 1.22
                },
                "all": {
                    "mean": 6.68,
                    "80th": 12.76,
                    "95th": 13.44,
                    "99th": 13.63
                }
            }
        },
        "performance": {
            "internal_queries": 0,
            "average_internal_latency_us": 0
        },
        "transaction": {
            "out": {
                "id": "00000000000000000000000000000000",
                "serial": 0,
                "total": 0,
                "attached": {
                    "bytes": 0,
                    "operations": 0,
                    "opcodes": 0,
                    "transactions": 0,
                    "subscribers": [ null ]
                }
            },
            "in": {
                "id": "00000000000000000000000000000000",
                "serial": 0,
                "total": 0,
                "attached": {
                    "bytes": 0,
                    "operations": 0,
                    "opcodes": 0,
                    "transactions": 0,
                    "provider": []
                }
            }
        },
        "durable": {
            "id": "00000000000000000000000000000000",
            "serial": 0
        },
        "memory": {
            "system_property_bytes": 247209145,
            "system_pooled_bytes": 381821890,
            "vgx_pooled_bytes": 914394545,
            "vgx_process_bytes": 33464320,
            "vgx_nonpooled_bytes": 880930225,
            "vgx_pooled_to_process_ratio": 27.324,
            "host_physical_bytes": 137108156416,
            "vgx_process_to_host_physical_ratio": 0.0
        }
    }
    """
    )

    # Send request
    bytes, headers = Support.send_request( "vgx/builtin/status", json=True )
    Support.assert_headers( headers, bytes, "application/json" )

    # Validate JSON and get response
    R = Support.response_from_json( bytes )

    # Validate structure against reference
    Support.validate_structure( REF, R )




###############################################################################
# TEST_builtin__vgx_builtin_ping
#
###############################################################################
def TEST_builtin__vgx_builtin_ping():
    """
    /vgx/builtin/ping
    test_level=4101
    t_nominal=1
    """

    REF = json.loads(
    """
    true
    """
    )

    # Send request
    bytes, headers = Support.send_request( "vgx/builtin/ping", json=True )
    Support.assert_headers( headers, bytes, "application/json" )

    # Validate JSON and get response
    R = Support.response_from_json( bytes )

    # Validate structure against reference
    Support.validate_structure( REF, R )




###############################################################################
# TEST_builtin__vgx_builtin_metrics
#
###############################################################################
def TEST_builtin__vgx_builtin_metrics():
    """
    /vgx/builtin/metrics
    test_level=4101
    t_nominal=1
    """

    REF = json.loads(
    """
    {
        "bytes_in": 10744,
        "bytes_out": 51886,
        "serving": 1,
        "total_requests": 15,
        "connected_clients": 2,
        "request_rate": {
            "current": 0.0,
            "all": 0.001
        },
        "latency_ms": {
            "current": {
                "mean": 0.21,
                "80.0": 0.18,
                "90.0": 0.18,
                "95.0": 0.18,
                "99.0": 0.18
            },
            "all": {
                "mean": 3.97,
                "80.0": 11.72,
                "90.0": 12.7,
                "95.0": 13.18,
                "99.0": 13.57
            }
        }
    }
    """
    )

    # Send request
    bytes, headers = Support.send_request( "vgx/builtin/metrics", json=True )
    Support.assert_headers( headers, bytes, "application/json" )

    # Validate JSON and get response
    R = Support.response_from_json( bytes )

    # Validate structure against reference
    Support.validate_structure( REF, R )









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
